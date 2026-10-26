//! Tests for the tournament match replays over a store held in memory: the key
//! layout, the whole replace, the refusals, and that no run-tree operation
//! reaches a replay.

use super::*;
use crate::object::{body_from_bytes, collect_body};
use crate::store::ObjectStore as _;
use crate::store::testing::{NO_LIMIT, memory_store, seed, tar_of};

/// The replay stored for `tournament`'s `game`, `None` when there is none.
async fn replay_of(store: &ArtifactStore, tournament: &str, game: &str) -> Option<Vec<u8>> {
    let object = store
        .match_replay(tournament, game)
        .await
        .expect("read the replay")?;
    assert_eq!(
        object.properties.content_type.as_deref(),
        Some(MATCH_REPLAY_CONTENT_TYPE)
    );
    Some(collect_body(object.body).await.expect("read the body"))
}

#[tokio::test]
async fn a_replay_is_stored_at_its_match_key_and_read_back() {
    let (store, objects) = memory_store();
    let written = store
        .put_match_replay("t1", "m1", body_from_bytes(r#"{"ticks":[]}"#))
        .await
        .unwrap();

    assert_eq!(written, 12);
    assert_eq!(
        objects.keys(),
        vec!["tournaments/t1/matches/m1/replay.json".to_string()]
    );
    assert_eq!(
        replay_of(&store, "t1", "m1").await.as_deref(),
        Some(br#"{"ticks":[]}"#.as_slice())
    );
}

#[tokio::test]
async fn a_second_write_replaces_the_replay_whole() {
    let (store, objects) = memory_store();
    store
        .put_match_replay("t1", "m1", body_from_bytes("first, and longer"))
        .await
        .unwrap();
    store
        .put_match_replay("t1", "m1", body_from_bytes("second"))
        .await
        .unwrap();

    assert_eq!(objects.keys().len(), 1);
    assert_eq!(
        replay_of(&store, "t1", "m1").await.as_deref(),
        Some(b"second".as_slice())
    );
}

#[tokio::test]
async fn a_body_that_fails_leaves_the_earlier_replay() {
    let (store, _objects) = memory_store();
    store
        .put_match_replay("t1", "m1", body_from_bytes("kept"))
        .await
        .unwrap();
    let failing: ObjectBody = Box::pin(futures_util::stream::iter(vec![
        Ok(bytes::Bytes::from_static(b"part")),
        Err(std::io::Error::other("the client hung up")),
    ]));

    assert!(store.put_match_replay("t1", "m1", failing).await.is_err());
    assert_eq!(
        replay_of(&store, "t1", "m1").await.as_deref(),
        Some(b"kept".as_slice())
    );
}

#[tokio::test]
async fn a_match_with_no_replay_reads_as_none() {
    let (store, _objects) = memory_store();
    store
        .put_match_replay("t1", "m1", body_from_bytes("{}"))
        .await
        .unwrap();

    assert!(replay_of(&store, "t1", "m2").await.is_none());
    assert!(replay_of(&store, "t2", "m1").await.is_none());
}

#[tokio::test]
async fn an_unsafe_id_is_refused_and_writes_nothing() {
    let (store, objects) = memory_store();
    for bad in ["..", ".", "", "a/b", "../escape", "a\\b", "a\0b"] {
        for (tournament, game) in [(bad, "m1"), ("t1", bad)] {
            assert!(
                matches!(
                    store
                        .put_match_replay(tournament, game, body_from_bytes("{}"))
                        .await,
                    Err(StoreError::UnsafeSegment(_))
                ),
                "a write to ({tournament:?}, {game:?}) must be refused"
            );
            assert!(
                matches!(
                    store.match_replay(tournament, game).await,
                    Err(StoreError::UnsafeSegment(_))
                ),
                "a read of ({tournament:?}, {game:?}) must be refused"
            );
        }
    }
    assert!(objects.keys().is_empty(), "nothing was written");
}

#[tokio::test]
async fn the_run_listing_never_reports_the_replays() {
    let (store, objects) = memory_store();
    seed(&store, "r1", &[("run-record.json", b"{}")]).await;
    store
        .put_match_replay("t1", "m1", body_from_bytes("{}"))
        .await
        .unwrap();
    // Even an object at the place a run's pointer would be is not taken for a
    // tree: the prefix is not a run's.
    objects
        .put("tournaments/current", body_from_bytes("g"), "text/plain")
        .await
        .unwrap();

    let ids: Vec<String> = store
        .list_runs()
        .await
        .unwrap()
        .into_iter()
        .map(|tree| tree.id)
        .collect();
    assert_eq!(ids, vec!["r1".to_string()]);
}

#[tokio::test]
async fn no_run_tree_operation_reaches_the_replays() {
    let (store, objects) = memory_store();
    seed(&store, "r1", &[("run-record.json", b"{}")]).await;
    store
        .put_match_replay("t1", "m1", body_from_bytes("{}"))
        .await
        .unwrap();

    // Deleting a run's tree leaves every replay in place.
    store.delete_run("r1").await.unwrap();
    // The prefix is not a run id, so no run tree can be written over the replays,
    // read from them, or delete them.
    assert!(matches!(
        store.delete_run(TOURNAMENTS).await,
        Err(StoreError::ReservedId(_))
    ));
    assert!(matches!(
        store.run_tree(TOURNAMENTS).await,
        Err(StoreError::ReservedId(_))
    ));
    assert!(matches!(
        store
            .store_run(
                TOURNAMENTS,
                body_from_bytes(tar_of(&[("run-record.json", b"{}")])),
                NO_LIMIT
            )
            .await,
        Err(StoreError::ReservedId(_))
    ));

    assert_eq!(
        objects.keys(),
        vec!["tournaments/t1/matches/m1/replay.json".to_string()]
    );
    assert_eq!(
        replay_of(&store, "t1", "m1").await.as_deref(),
        Some(b"{}".as_slice())
    );
}
