//! Tests for the tournament match replay routes: the round trip and the replace,
//! the service-token gate (and the routes refusing every caller with no token
//! configured), the refusals of an unsafe id and an oversized body, and that the
//! run-tree routes neither list, delete nor write over a replay.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use tower::ServiceExt;

use super::tests::{
    SERVICE_TOKEN, app_with_limit, app_with_service_token, delete_request, list_request,
    seed_upload, spawn_stub, tarball,
};
use crate::auth::LOCAL_IDENTITY_HEADER;

/// A `PUT` of `body` as `tournament`'s `game` replay, presenting `token`.
fn put_request(tournament: &str, game: &str, token: Option<&str>, body: &str) -> Request<Body> {
    let mut builder = Request::builder()
        .method("PUT")
        .uri(format!("/tournaments/{tournament}/matches/{game}/replay"))
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder.body(Body::from(body.to_string())).unwrap()
}

/// A `GET` of `tournament`'s `game` replay at `prefix` (the root or the edge's),
/// presenting `token`.
fn get_request(prefix: &str, tournament: &str, game: &str, token: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().method("GET").uri(format!(
        "{prefix}/tournaments/{tournament}/matches/{game}/replay"
    ));
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    builder.body(Body::empty()).unwrap()
}

/// The status `request` is answered with.
async fn status(app: &Router, request: Request<Body>) -> StatusCode {
    app.clone().oneshot(request).await.unwrap().status()
}

/// The replay `app` serves for `tournament`'s `game`, asserting it is served as
/// JSON.
async fn served_replay(app: &Router, tournament: &str, game: &str) -> String {
    let response = app
        .clone()
        .oneshot(get_request("", tournament, game, Some(SERVICE_TOKEN)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/json"
    );
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn a_stored_replay_is_served_back_and_replaced_whole() {
    let stub = spawn_stub().await;
    let (app, _store, objects) = app_with_service_token(&stub, Some(SERVICE_TOKEN)).await;

    let response = app
        .clone()
        .oneshot(put_request(
            "t1",
            "m1",
            Some(SERVICE_TOKEN),
            r#"{"ticks":[1]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body, serde_json::json!({ "bytes": 13 }));
    assert_eq!(
        objects.keys(),
        vec!["tournaments/t1/matches/m1/replay.json".to_string()]
    );
    assert_eq!(served_replay(&app, "t1", "m1").await, r#"{"ticks":[1]}"#);

    assert_eq!(
        status(&app, put_request("t1", "m1", Some(SERVICE_TOKEN), "{}")).await,
        StatusCode::CREATED
    );
    assert_eq!(served_replay(&app, "t1", "m1").await, "{}");
}

#[tokio::test]
async fn the_routes_are_served_under_the_edge_prefix_too() {
    let stub = spawn_stub().await;
    let (app, _store, _objects) = app_with_service_token(&stub, Some(SERVICE_TOKEN)).await;
    assert_eq!(
        status(&app, put_request("t1", "m1", Some(SERVICE_TOKEN), "{}")).await,
        StatusCode::CREATED
    );
    assert_eq!(
        status(
            &app,
            get_request("/artifacts", "t1", "m1", Some(SERVICE_TOKEN))
        )
        .await,
        StatusCode::OK
    );
}

#[tokio::test]
async fn a_match_with_no_replay_is_not_found() {
    let stub = spawn_stub().await;
    let (app, _store, _objects) = app_with_service_token(&stub, Some(SERVICE_TOKEN)).await;
    assert_eq!(
        status(&app, put_request("t1", "m1", Some(SERVICE_TOKEN), "{}")).await,
        StatusCode::CREATED
    );

    for (tournament, game) in [("t1", "m2"), ("t2", "m1")] {
        assert_eq!(
            status(&app, get_request("", tournament, game, Some(SERVICE_TOKEN))).await,
            StatusCode::NOT_FOUND
        );
    }
}

#[tokio::test]
async fn both_routes_take_the_service_token_alone() {
    let stub = spawn_stub().await;
    let (app, _store, objects) = app_with_service_token(&stub, Some(SERVICE_TOKEN)).await;
    assert_eq!(
        status(&app, put_request("t1", "m1", Some(SERVICE_TOKEN), "{}")).await,
        StatusCode::CREATED
    );

    for token in [None, Some("not-the-secret")] {
        assert_eq!(
            status(&app, put_request("t1", "m1", token, "overwritten")).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status(&app, get_request("", "t1", "m1", token)).await,
            StatusCode::UNAUTHORIZED
        );
    }

    // A person, who may read a run's media, reads no replay and writes none: the
    // backend is the replays' one caller.
    let as_person = |method: &str| {
        Request::builder()
            .method(method)
            .uri("/tournaments/t1/matches/m1/replay")
            .header(LOCAL_IDENTITY_HEADER, "reviewer@example.com")
            .body(Body::from("overwritten"))
            .unwrap()
    };
    assert_eq!(
        status(&app, as_person("GET")).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        status(&app, as_person("PUT")).await,
        StatusCode::UNAUTHORIZED
    );

    assert_eq!(objects.keys().len(), 1);
    assert_eq!(served_replay(&app, "t1", "m1").await, "{}");
}

#[tokio::test]
async fn with_no_service_token_configured_every_caller_is_refused() {
    let stub = spawn_stub().await;
    let (app, _store, objects) = app_with_service_token(&stub, None).await;

    assert_eq!(
        status(&app, put_request("t1", "m1", Some(SERVICE_TOKEN), "{}")).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        status(&app, get_request("", "t1", "m1", Some(SERVICE_TOKEN))).await,
        StatusCode::UNAUTHORIZED
    );
    assert!(objects.keys().is_empty());
}

#[tokio::test]
async fn an_id_that_is_not_one_safe_segment_is_a_bad_request() {
    let stub = spawn_stub().await;
    let (app, _store, objects) = app_with_service_token(&stub, Some(SERVICE_TOKEN)).await;

    // Percent-encoded, so each reaches the handler as one path parameter that
    // decodes to a dot segment or one carrying a separator.
    for bad in ["..", "%2E%2E", "a%2Fb", "a%5Cb"] {
        for (tournament, game) in [(bad, "m1"), ("t1", bad)] {
            assert_eq!(
                status(
                    &app,
                    put_request(tournament, game, Some(SERVICE_TOKEN), "{}")
                )
                .await,
                StatusCode::BAD_REQUEST,
                "a write to ({tournament:?}, {game:?})"
            );
            assert_eq!(
                status(&app, get_request("", tournament, game, Some(SERVICE_TOKEN))).await,
                StatusCode::BAD_REQUEST,
                "a read of ({tournament:?}, {game:?})"
            );
        }
    }
    assert!(objects.keys().is_empty(), "nothing was written");
}

#[tokio::test]
async fn a_replay_over_the_limit_is_refused_and_leaves_the_earlier_one() {
    let stub = spawn_stub().await;
    let (app, _store, _objects) = app_with_limit(&stub, Some(SERVICE_TOKEN), 8).await;
    assert_eq!(
        status(&app, put_request("t1", "m1", Some(SERVICE_TOKEN), "{}")).await,
        StatusCode::CREATED
    );

    // Declared too long, refused before the body is read.
    let mut declared = put_request("t1", "m1", Some(SERVICE_TOKEN), "far too long a replay");
    declared
        .headers_mut()
        .insert(header::CONTENT_LENGTH, header::HeaderValue::from(21u64));
    assert_eq!(status(&app, declared).await, StatusCode::PAYLOAD_TOO_LARGE);
    // Undeclared, refused as the body passes the cap.
    let streamed = Request::builder()
        .method("PUT")
        .uri("/tournaments/t1/matches/m1/replay")
        .header("authorization", format!("Bearer {SERVICE_TOKEN}"))
        .body(Body::from_stream(futures_util::stream::iter(vec![
            Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"far too ")),
            Ok(bytes::Bytes::from_static(b"long a replay")),
        ])))
        .unwrap();
    assert_eq!(status(&app, streamed).await, StatusCode::PAYLOAD_TOO_LARGE);

    assert_eq!(served_replay(&app, "t1", "m1").await, "{}");
}

#[tokio::test]
async fn the_run_tree_routes_leave_the_replays_alone() {
    let stub = spawn_stub().await;
    let (app, _store, _objects) = app_with_service_token(&stub, Some(SERVICE_TOKEN)).await;
    seed_upload(&app, "run-a").await;
    assert_eq!(
        status(&app, put_request("t1", "m1", Some(SERVICE_TOKEN), "{}")).await,
        StatusCode::CREATED
    );

    // The listing reports the run and nothing of the replays' prefix, so the
    // backend's reclamation sweep never takes one for an orphaned tree.
    let response = app
        .clone()
        .oneshot(list_request(Some(SERVICE_TOKEN)))
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let ids: Vec<&str> = body["runs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|run| run["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["run-a"]);

    // Deleting a run's tree leaves the replay, and the replays' prefix is no run
    // id: it can be neither deleted nor uploaded over as one.
    assert_eq!(
        status(&app, delete_request("run-a", Some(SERVICE_TOKEN))).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        status(&app, delete_request("tournaments", Some(SERVICE_TOKEN))).await,
        StatusCode::BAD_REQUEST
    );
    let upload = Request::builder()
        .method("POST")
        .uri("/runs/tournaments/artifacts")
        .header(
            "authorization",
            format!("Bearer {}", super::tests::GOOD_JOB_TOKEN),
        )
        .header("x-tcab-job-id", super::tests::GOOD_JOB_ID)
        .body(Body::from(tarball(&[("run-record.json", b"{}")])))
        .unwrap();
    assert_eq!(status(&app, upload).await, StatusCode::BAD_REQUEST);

    assert_eq!(served_replay(&app, "t1", "m1").await, "{}");
}
