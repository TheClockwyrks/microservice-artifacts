//! The `tcab-artifacts` binary stopped by a signal while a slow upload is in
//! flight: the control plane's "Graceful termination".
//!
//! The binary runs against a stub Blob service on a loopback socket, which
//! answers the container creation and listing the start makes and keeps every
//! blob written, so nothing outside the test is needed. The upload is written
//! by hand over a socket, with `Expect: 100-continue`, so the test knows the
//! handler is reading the body before it sends the signal, and holds the rest
//! of the body back until the service has stopped accepting connections.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};

/// The emulator's account and its well-known key (Azurite's), which the
/// service signs with; the stub does not check the signature.
const ACCOUNT: &str = "devstoreaccount1";
// cspell:disable-next-line
const KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
const RUNS: &str = "runs";
const PUBLIC: &str = "public";
const SERVICE_TOKEN: &str = "graceful-service-token";

/// Generous bounds for each wait: polled, never slept through.
const START_TIMEOUT: Duration = Duration::from_secs(90);
const STEP_TIMEOUT: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(25);

/// The blobs the stub holds, by `<container>/<name>`.
type Blobs = Arc<Mutex<HashMap<String, Vec<u8>>>>;

/// The stub's state: the blobs it holds, whether it refuses every listing (a
/// container the start cannot reach yet), and how many listings it was asked
/// for.
#[derive(Clone, Default)]
struct Stub {
    blobs: Blobs,
    refuse_listings: bool,
    listings: Arc<AtomicUsize>,
}

/// The stub Blob service: creates containers, lists none, keeps every
/// `Put Blob`.
async fn stub_blob(State(stub): State<Stub>, request: Request) -> Response {
    let query = request.uri().query().unwrap_or("").to_string();
    let path = request.uri().path().to_string();
    let name = path
        .strip_prefix(&format!("/{ACCOUNT}/"))
        .unwrap_or("")
        .to_string();
    let method = request.method().clone();
    if method == Method::PUT && query.contains("restype=container") {
        return StatusCode::CREATED.into_response();
    }
    if method == Method::GET && query.contains("comp=list") {
        stub.listings.fetch_add(1, Ordering::SeqCst);
        if stub.refuse_listings {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        return (
            StatusCode::OK,
            [("content-type", "application/xml")],
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>\
             <EnumerationResults><Blobs></Blobs><NextMarker/></EnumerationResults>",
        )
            .into_response();
    }
    if method == Method::PUT && query.is_empty() {
        let body = axum::body::to_bytes(request.into_body(), usize::MAX)
            .await
            .unwrap();
        stub.blobs.lock().unwrap().insert(name, body.to_vec());
        return StatusCode::CREATED.into_response();
    }
    StatusCode::NOT_IMPLEMENTED.into_response()
}

async fn spawn_stub() -> (SocketAddr, Blobs) {
    let stub = Stub::default();
    let blobs = Arc::clone(&stub.blobs);
    (serve_stub(stub).await, blobs)
}

async fn serve_stub(stub: Stub) -> SocketAddr {
    let app = Router::new().fallback(stub_blob).with_state(stub);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

/// A loopback port nothing listens on at the moment.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// The service's binary, started against the stub in an empty directory (so no
/// `.env.artifacts` is read) with only the variables it is given.
fn spawn_service(stub: SocketAddr, port: u16, dir: &std::path::Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_tcab-artifacts"))
        .current_dir(dir)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("RUST_LOG", "info")
        .env("TCAB_ARTIFACTS_BIND", format!("127.0.0.1:{port}"))
        .env("TCAB_ARTIFACTS_BLOB_ACCOUNT", ACCOUNT)
        .env("TCAB_ARTIFACTS_BLOB_CONTAINER", RUNS)
        .env(
            "TCAB_ARTIFACTS_BLOB_ENDPOINT",
            format!("http://{stub}/{ACCOUNT}"),
        )
        .env("TCAB_ARTIFACTS_BLOB_KEY", KEY)
        .env("TCAB_ARTIFACTS_PUBLIC_BLOB_CONTAINER", PUBLIC)
        .env("TCAB_BACKEND_SERVICE_TOKEN", SERVICE_TOKEN)
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("the service binary starts")
}

/// A client that opens a fresh connection per request, so every probe is a
/// new connection the listener has to accept.
fn probe_client() -> reqwest::Client {
    reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap()
}

/// What one probe of `GET /readyz` met.
#[derive(Debug, PartialEq)]
enum Probe {
    Status(u16),
    /// No answer: nothing accepts on the port, or a connection queued as the
    /// listener closed was reset.
    Refused,
}

async fn probe(client: &reqwest::Client, port: u16) -> Probe {
    match client
        .get(format!("http://127.0.0.1:{port}/readyz"))
        .send()
        .await
    {
        Ok(response) => Probe::Status(response.status().as_u16()),
        Err(_) => Probe::Refused,
    }
}

/// Poll until the service answers ready, failing if it exits first.
async fn wait_ready(service: &mut Child, client: &reqwest::Client, port: u16) {
    let deadline = tokio::time::Instant::now() + START_TIMEOUT;
    loop {
        if let Some(status) = service.try_wait().unwrap() {
            panic!("the service exited before it was ready: {status}");
        }
        if probe(client, port).await == Probe::Status(200) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the service never became ready"
        );
        tokio::time::sleep(POLL).await;
    }
}

fn signal(service: &Child, name: &str) {
    let pid = service.id().expect("the service is running").to_string();
    let status = std::process::Command::new("kill")
        .args([format!("-{name}"), pid])
        .status()
        .unwrap();
    assert!(status.success(), "kill -{name} failed");
}

async fn wait_exit(service: &mut Child, timeout: Duration) -> ExitStatus {
    tokio::time::timeout(timeout, service.wait())
        .await
        .expect("the service exits in time")
        .unwrap()
}

/// Read from `stream` until `marker` has arrived, answering everything read.
async fn read_until(stream: &mut TcpStream, marker: &str) -> String {
    let mut read = Vec::new();
    let mut buffer = [0u8; 1024];
    tokio::time::timeout(STEP_TIMEOUT, async {
        while !String::from_utf8_lossy(&read).contains(marker) {
            let n = stream.read(&mut buffer).await.unwrap();
            assert!(
                n > 0,
                "the connection closed before `{marker}`: {}",
                String::from_utf8_lossy(&read)
            );
            read.extend_from_slice(&buffer[..n]);
        }
    })
    .await
    .unwrap_or_else(|_| panic!("`{marker}` never arrived"));
    String::from_utf8_lossy(&read).into_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_drains_and_finishes_an_upload_in_flight() {
    let (stub, blobs) = spawn_stub().await;
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let mut service = spawn_service(stub, port, dir.path());
    let client = probe_client();
    wait_ready(&mut service, &client, port).await;

    // Begin a public write whose body arrives in two halves, and wait for the
    // `100 Continue` hyper sends once the handler reads the body: from here the
    // request is in flight.
    let first = b"the first half of the body, ".as_slice();
    let second = b"and the second, sent after the listener closed".as_slice();
    let mut upload = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let head = format!(
        "PUT /public/graceful/object.txt HTTP/1.1\r\n\
         Host: 127.0.0.1:{port}\r\n\
         Authorization: Bearer {SERVICE_TOKEN}\r\n\
         Content-Type: text/plain\r\n\
         Content-Length: {}\r\n\
         Expect: 100-continue\r\n\
         Connection: close\r\n\r\n",
        first.len() + second.len()
    );
    upload.write_all(head.as_bytes()).await.unwrap();
    read_until(&mut upload, "\r\n\r\n").await;
    upload.write_all(first).await.unwrap();

    signal(&service, "TERM");

    // Readiness fails at once while the listener still accepts (the drain
    // delay), and then the listener closes.
    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + STEP_TIMEOUT;
    loop {
        let answer = probe(&client, port).await;
        if seen.last() != Some(&answer) {
            seen.push(answer);
        }
        if seen.last() == Some(&Probe::Refused) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the service never stopped accepting: {seen:?}"
        );
        tokio::time::sleep(POLL).await;
    }
    // A probe raced with the signal may still have found it ready.
    if seen.first() == Some(&Probe::Status(200)) {
        seen.remove(0);
    }
    assert_eq!(
        seen,
        [Probe::Status(503), Probe::Refused],
        "readiness answers 503 through the drain delay, then nothing accepts"
    );
    assert!(
        service.try_wait().unwrap().is_none(),
        "the service exited with a request in flight"
    );

    // The request in flight still completes.
    upload.write_all(second).await.unwrap();
    let response = read_until(&mut upload, "\r\n\r\n").await;
    let status_line = response
        .lines()
        .find(|line| !line.starts_with("HTTP/1.1 100"))
        .unwrap_or("")
        .to_string();
    assert!(
        status_line.starts_with("HTTP/1.1 201"),
        "the upload in flight failed: {response}"
    );
    let stored = blobs
        .lock()
        .unwrap()
        .get(&format!("{PUBLIC}/graceful/object.txt"))
        .cloned();
    assert_eq!(stored.as_deref(), Some([first, second].concat().as_slice()));

    let status = wait_exit(&mut service, STEP_TIMEOUT).await;
    assert!(status.success(), "the service exited with {status}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_interrupt_skips_the_drain_delay_and_exits_cleanly() {
    let (stub, _blobs) = spawn_stub().await;
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let mut service = spawn_service(stub, port, dir.path());
    let client = probe_client();
    wait_ready(&mut service, &client, port).await;

    signal(&service, "INT");

    // With no request in flight and no drain delay, the exit comes well inside
    // the 5 s a `SIGTERM` would keep accepting for.
    let status = wait_exit(&mut service, Duration::from_secs(4)).await;
    assert!(status.success(), "the service exited with {status}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_during_the_start_exits_cleanly() {
    // A container the start cannot reach yet keeps it retrying.
    let stub = Stub {
        refuse_listings: true,
        ..Stub::default()
    };
    let listings = Arc::clone(&stub.listings);
    let addr = serve_stub(stub).await;
    let dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let mut service = spawn_service(addr, port, dir.path());

    // Wait until the start has been refused at least once: it is in its retry
    // loop, long after the handlers would have been installed.
    let deadline = tokio::time::Instant::now() + START_TIMEOUT;
    while listings.load(Ordering::SeqCst) == 0 {
        if let Some(status) = service.try_wait().unwrap() {
            panic!("the service exited during its start: {status}");
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the start never reached the container"
        );
        tokio::time::sleep(POLL).await;
    }

    signal(&service, "TERM");
    let status = wait_exit(&mut service, STEP_TIMEOUT).await;
    assert!(status.success(), "the service exited with {status}");
}
