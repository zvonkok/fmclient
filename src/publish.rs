// SPDX-License-Identifier: Apache-2.0
// Copyright (c) NVIDIA CORPORATION

//! Publish the fabric view to kata-device-plugin.
//!
//! The plugin hosts the view's Publish stream and hands the one container it
//! gives the fabric to an endpoint and a token through its environment
//! ([`pcilibs_rs::fabric::ENDPOINT_ENV`], [`pcilibs_rs::fabric::TOKEN_ENV`]).
//! This client opens one stream, sends the view whenever Fabric Manager's
//! lists change, resends it on a heartbeat so the plugin can tell a quiet
//! publisher from a dead one, and reconnects with backoff when the stream
//! ends.  The open stream is the plugin's liveness signal: it withdraws the
//! view when the stream closes.  So the stream opens only with a view in
//! hand, and it is closed when Fabric Manager has not answered for a
//! heartbeat: a view the publisher cannot refresh is withdrawn rather than
//! kept alive, and publishing resumes once Fabric Manager answers again.
//!
//! On the host the plugin also listens on a unix socket; `unix://<path>`
//! selects it, and no token is needed there because the socket's
//! permissions are the credential.

use std::error::Error;
use std::time::Duration;

use hyper_util::rt::TokioIo;
use pcilibs_rs::fabric::grpc::{with_token, FabricViewServiceClient};
use pcilibs_rs::fabric::v1::FabricView;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint, Uri};
use tonic::Request;
use tower::service_fn;

/// Where to publish and how to authenticate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    /// `http://host:port`, or `unix:///path` for the plugin's host socket.
    pub endpoint: String,
    /// Absent on the host, where the socket's permissions are the credential.
    pub token: Option<String>,
}

impl Target {
    /// From the values the plugin injects ([`pcilibs_rs::fabric::ENDPOINT_ENV`],
    /// [`pcilibs_rs::fabric::TOKEN_ENV`])
    /// or a flag; `None` without an endpoint, and an empty token is no token.
    pub fn from_vars(endpoint: Option<String>, token: Option<String>) -> Option<Self> {
        let endpoint = endpoint.filter(|endpoint| !endpoint.is_empty())?;
        Some(Self {
            endpoint,
            token: token.filter(|token| !token.is_empty()),
        })
    }
}

/// How often the lists are read, how long a stream may stay quiet before the
/// unchanged view is resent, and how long to wait between connection
/// attempts at most.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pacing {
    pub poll: Duration,
    pub heartbeat: Duration,
    pub backoff_max: Duration,
}

impl Default for Pacing {
    fn default() -> Self {
        Self {
            poll: Duration::from_secs(5),
            heartbeat: Duration::from_secs(30),
            backoff_max: Duration::from_secs(30),
        }
    }
}

/// Reads the view from Fabric Manager.  An error is logged and the read
/// retried at the next poll; the stream stays open meanwhile.
/// One reading of Fabric Manager's lists, or why there is none.
pub type Reading = Result<FabricView, String>;

/// Read `source` every `interval` and hand each reading to the publisher,
/// until the publisher is gone.  Meant for a thread of its own: the SDK
/// calls block, and a slow one must not stall the stream's keepalive
/// answers, which the plugin takes as the publisher being dead.
pub fn read_forever(
    mut source: impl FnMut() -> Result<FabricView, Box<dyn Error>>,
    interval: Duration,
    readings: mpsc::Sender<Reading>,
) {
    loop {
        let reading = source().map_err(|e| e.to_string());
        if readings.blocking_send(reading).is_err() {
            return;
        }
        std::thread::sleep(interval);
    }
}

/// Publish until `stop` is cancelled.  Returns early for a token that is
/// not valid gRPC metadata, which no plugin issues, and for a reader that
/// ended: retrying cannot fix either.
pub async fn run(
    target: &Target,
    mut readings: mpsc::Receiver<Reading>,
    pacing: Pacing,
    stop: CancellationToken,
    log: &mut dyn FnMut(String),
) -> Result<(), Box<dyn Error>> {
    let first_backoff = Duration::from_secs(1).min(pacing.backoff_max);
    let mut backoff = first_backoff;
    // A view not refreshed within this is withdrawn: one heartbeat, or two
    // readings when the reader is slower than that.  Fail-closed covers a
    // reader that fails and one that stalls alike.
    let stale_after = pacing.heartbeat.max(pacing.poll * 2);
    while !stop.is_cancelled() {
        // No stream without a view: an open stream tells the plugin the view
        // is current, which a publisher without a reading cannot say.
        let (first, mut answered) = loop {
            let reading = tokio::select! {
                _ = stop.cancelled() => return Ok(()),
                reading = readings.recv() => reading,
            };
            match reading {
                Some(Ok(view)) => break (view, tokio::time::Instant::now()),
                Some(Err(e)) => log(format!("fabric manager: {e}")),
                None => return Err("the fabric manager reader ended".into()),
            }
        };
        let connected = tokio::select! {
            _ = stop.cancelled() => return Ok(()),
            connected = connect(&target.endpoint) => connected,
        };
        let channel = match connected {
            Ok(channel) => channel,
            Err(e) => {
                log(format!("connect {}: {e}", target.endpoint));
                pause(backoff, &stop).await;
                backoff = (backoff * 2).min(pacing.backoff_max);
                continue;
            }
        };
        let (tx, rx) = mpsc::channel::<FabricView>(1);
        let mut request = Request::new(ReceiverStream::new(rx));
        if let Some(token) = &target.token {
            request = with_token(request, token)?;
        }
        let mut client = FabricViewServiceClient::new(channel);
        let mut rpc = Box::pin(client.publish(request));
        log(format!("publishing to {}", target.endpoint));

        let opened = tokio::time::Instant::now();
        let mut sent = opened;
        // Whether the last reading failed: the heartbeat then stops
        // resending a view that may be stale.
        let mut failing = false;
        let mut last: Option<FabricView> = None;
        if tx.try_send(first.clone()).is_ok() {
            log(format!("view changed: {first}"));
            last = Some(first);
        }
        // Sends never wait: with the stream's buffer full the send is skipped
        // and the next reading retries it, so cancellation and the rpc keep
        // being polled while the plugin is not draining.  A closed receiver
        // means the rpc is ending; its arm reports why.
        let outcome = loop {
            tokio::select! {
                _ = stop.cancelled() => break Outcome::Stopped,
                result = &mut rpc => break Outcome::Ended(result.map(drop)),
                reading = readings.recv() => match reading {
                    None => break Outcome::ReaderGone,
                    Some(Err(e)) => {
                        log(format!("fabric manager: {e}"));
                        failing = true;
                    }
                    Some(Ok(view)) => {
                        answered = tokio::time::Instant::now();
                        failing = false;
                        if last.as_ref() != Some(&view) && tx.try_send(view.clone()).is_ok() {
                            sent = tokio::time::Instant::now();
                            log(format!("view changed: {view}"));
                            last = Some(view);
                        }
                    }
                },
                _ = tokio::time::sleep_until(answered + stale_after) => break Outcome::Unanswered,
                // Independent of the readings, so a long --interval does not
                // lengthen the silence the plugin tolerates.
                _ = tokio::time::sleep_until(sent + pacing.heartbeat), if last.is_some() && !failing => {
                    if let Some(view) = &last {
                        if tx.try_send(view.clone()).is_ok() {
                            sent = tokio::time::Instant::now();
                        }
                    }
                }
            }
        };
        drop(tx);
        match outcome {
            Outcome::Stopped => return Ok(()),
            Outcome::ReaderGone => return Err("the fabric manager reader ended".into()),
            Outcome::Ended(Ok(())) => log("stream closed by the plugin".to_owned()),
            Outcome::Ended(Err(status)) => log(format!("stream ended: {status}")),
            Outcome::Unanswered => log(format!(
                "fabric manager unanswered for {stale_after:?}: closing the stream"
            )),
        }
        // A queued view proves nothing, the plugin may have refused the
        // stream at once; a stream that lived a heartbeat did carry views,
        // and earns a fresh start.  Anything shorter backs off like a failed
        // connect.
        backoff = if opened.elapsed() >= pacing.heartbeat {
            first_backoff
        } else {
            (backoff * 2).min(pacing.backoff_max)
        };
        pause(backoff, &stop).await;
    }
    Ok(())
}

/// Why a stream ended.
enum Outcome {
    Stopped,
    /// The plugin closed it, cleanly or with a status.
    Ended(Result<(), tonic::Status>),
    /// Fabric Manager stopped answering; this side closed it.
    Unanswered,
    /// The reader thread is gone; nothing will ever answer.
    ReaderGone,
}

async fn pause(duration: Duration, stop: &CancellationToken) {
    tokio::select! {
        _ = stop.cancelled() => {}
        _ = tokio::time::sleep(duration) => {}
    }
}

/// HTTP/2 keepalives catch a plugin that vanished without closing, which the
/// heartbeat alone would notice only on the next send; the connect timeout
/// bounds an endpoint that drops packets, which the OS would otherwise let
/// hang for minutes.
async fn connect(endpoint: &str) -> Result<Channel, tonic::transport::Error> {
    const KEEPALIVE: Duration = Duration::from_secs(10);
    let unix = endpoint.strip_prefix("unix://").map(str::to_owned);
    // For a socket the URI is a placeholder: the connector ignores it and
    // dials the path.
    let uri = unix
        .as_ref()
        .map_or(endpoint, |_| "http://[::]:0")
        .to_owned();
    let endpoint = Endpoint::from_shared(uri)?
        .connect_timeout(KEEPALIVE)
        .http2_keep_alive_interval(KEEPALIVE)
        .keep_alive_timeout(KEEPALIVE)
        .keep_alive_while_idle(true);
    match unix {
        Some(path) => {
            endpoint
                .connect_with_connector(service_fn(move |_: Uri| {
                    let path = path.clone();
                    async move {
                        tokio::net::UnixStream::connect(path)
                            .await
                            .map(TokioIo::new)
                    }
                }))
                .await
        }
        None => endpoint.connect().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use pcilibs_rs::fabric::grpc::{require_token, FabricViewService, FabricViewServiceServer};
    use pcilibs_rs::fabric::v1::{self, PublishSummary};
    use tokio::sync::Notify;
    use tokio_stream::wrappers::UnixListenerStream;
    use tonic::{Response, Status, Streaming};

    /// A plugin stand-in: records every view, wakes the test on each and on
    /// every stream's end, and optionally closes the stream after the first
    /// message.
    #[derive(Default)]
    struct Recorder {
        views: Mutex<Vec<FabricView>>,
        got_one: Notify,
        ended: Notify,
        close_after_first: bool,
    }

    /// The service handle tonic owns; the test keeps the Arc to read back.
    struct Service(Arc<Recorder>);

    #[tonic::async_trait]
    impl FabricViewService for Service {
        async fn publish(
            &self,
            request: Request<Streaming<FabricView>>,
        ) -> Result<Response<PublishSummary>, Status> {
            let recorder = &self.0;
            let mut stream = request.into_inner();
            let mut received = 0;
            while let Some(view) = stream.message().await? {
                recorder.views.lock().unwrap().push(view);
                received += 1;
                recorder.got_one.notify_one();
                if recorder.close_after_first {
                    break;
                }
            }
            recorder.ended.notify_one();
            Ok(Response::new(PublishSummary {
                views_received: received,
            }))
        }
    }

    struct Plugin {
        recorder: Arc<Recorder>,
        endpoint: String,
        stop: CancellationToken,
        _dir: tempfile::TempDir,
    }

    async fn plugin(token: &str, close_after_first: bool) -> Plugin {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fabric-view.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let recorder = Arc::new(Recorder {
            close_after_first,
            ..Recorder::default()
        });
        let stop = CancellationToken::new();
        let server = FabricViewServiceServer::with_interceptor(
            Service(recorder.clone()),
            require_token(token),
        );
        let shutdown = stop.clone();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(server)
                .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async move {
                    shutdown.cancelled().await
                }),
        );
        Plugin {
            recorder,
            endpoint: format!("unix://{}", path.display()),
            stop,
            _dir: dir,
        }
    }

    fn view(unit: &str, active: &[u32]) -> FabricView {
        FabricView {
            partitions: Some(v1::PartitionList {
                version: 1,
                max_num_partitions: 15,
                partition_info: active
                    .iter()
                    .map(|&id| v1::Partition {
                        partition_id: id,
                        is_active: true,
                        gpu_info: vec![v1::PartitionGpu::default(); 2],
                    })
                    .collect(),
            }),
            unsupported: None,
            unit: unit.to_owned(),
        }
    }

    /// A reader thread over a source, as main runs it.
    fn reader(
        source: impl FnMut() -> Result<FabricView, Box<dyn Error>> + Send + 'static,
        poll: Duration,
    ) -> mpsc::Receiver<Reading> {
        let (tx, rx) = mpsc::channel(1);
        std::thread::spawn(move || read_forever(source, poll, tx));
        rx
    }

    fn fast() -> Pacing {
        Pacing {
            poll: Duration::from_millis(20),
            heartbeat: Duration::from_secs(60),
            backoff_max: Duration::from_millis(100),
        }
    }

    #[test]
    fn target_from_vars() {
        assert_eq!(Target::from_vars(None, None), None);
        assert_eq!(
            Target::from_vars(Some(String::new()), Some("t".into())),
            None
        );
        assert_eq!(
            Target::from_vars(Some("unix:///run/x.sock".into()), Some(String::new())),
            Some(Target {
                endpoint: "unix:///run/x.sock".into(),
                token: None
            })
        );
        assert_eq!(
            Target::from_vars(Some("http://10.0.0.1:9000".into()), Some("t".into()))
                .unwrap()
                .token,
            Some("t".into())
        );
    }

    #[tokio::test]
    async fn sends_once_per_change_and_not_otherwise() {
        let plugin = plugin("t", false).await;
        let target = Target {
            endpoint: plugin.endpoint.clone(),
            token: Some("t".into()),
        };
        let reads = Arc::new(Mutex::new(0u32));
        let counter = reads.clone();
        // The same view three times, then a different one.
        let source = move || -> Result<FabricView, Box<dyn Error>> {
            let mut n = counter.lock().unwrap();
            *n += 1;
            Ok(if *n <= 3 {
                view("", &[1])
            } else {
                view("", &[2, 3])
            })
        };
        let stop = CancellationToken::new();
        let mut log = Vec::new();
        let waiter = plugin.recorder.clone();
        let stopper = stop.clone();
        tokio::spawn(async move {
            waiter.got_one.notified().await;
            waiter.got_one.notified().await;
            stopper.cancel();
        });
        run(
            &target,
            reader(source, fast().poll),
            fast(),
            stop,
            &mut |line| log.push(line),
        )
        .await
        .unwrap();
        let views = plugin.recorder.views.lock().unwrap().clone();
        assert_eq!(views, [view("", &[1]), view("", &[2, 3])]);
        assert!(*reads.lock().unwrap() >= 4);
        assert!(
            log.iter().any(|l| l.starts_with("publishing to unix://")),
            "{log:?}"
        );
        assert_eq!(
            log.iter().filter(|l| l.starts_with("view changed")).count(),
            2
        );
        plugin.stop.cancel();
    }

    #[tokio::test]
    async fn heartbeat_resends_an_unchanged_view() {
        let plugin = plugin("t", false).await;
        let target = Target {
            endpoint: plugin.endpoint.clone(),
            token: Some("t".into()),
        };
        let source = || -> Result<FabricView, Box<dyn Error>> { Ok(view("", &[1])) };
        let pacing = Pacing {
            heartbeat: Duration::from_millis(50),
            ..fast()
        };
        let stop = CancellationToken::new();
        let waiter = plugin.recorder.clone();
        let stopper = stop.clone();
        tokio::spawn(async move {
            for _ in 0..3 {
                waiter.got_one.notified().await;
            }
            stopper.cancel();
        });
        run(
            &target,
            reader(source, pacing.poll),
            pacing,
            stop,
            &mut |_| {},
        )
        .await
        .unwrap();
        let views = plugin.recorder.views.lock().unwrap();
        assert!(views.len() >= 3, "{}", views.len());
        assert!(views.iter().all(|v| *v == view("", &[1])));
        plugin.stop.cancel();
    }

    #[tokio::test]
    async fn the_heartbeat_does_not_wait_for_a_slow_poll() {
        // A long poll interval must not lengthen the silence: the cached
        // view is resent on the heartbeat without reading Fabric Manager.
        let plugin = plugin("t", false).await;
        let target = Target {
            endpoint: plugin.endpoint.clone(),
            token: Some("t".into()),
        };
        let reads = Arc::new(Mutex::new(0u32));
        let counter = reads.clone();
        let source = move || -> Result<FabricView, Box<dyn Error>> {
            *counter.lock().unwrap() += 1;
            Ok(view("", &[2]))
        };
        let pacing = Pacing {
            poll: Duration::from_secs(60),
            heartbeat: Duration::from_millis(40),
            backoff_max: Duration::from_millis(100),
        };
        let stop = CancellationToken::new();
        let waiter = plugin.recorder.clone();
        let stopper = stop.clone();
        tokio::spawn(async move {
            for _ in 0..3 {
                waiter.got_one.notified().await;
            }
            stopper.cancel();
        });
        run(
            &target,
            reader(source, pacing.poll),
            pacing,
            stop,
            &mut |_| {},
        )
        .await
        .unwrap();
        assert!(plugin.recorder.views.lock().unwrap().len() >= 3);
        assert_eq!(*reads.lock().unwrap(), 1, "one poll, the rest heartbeats");
        plugin.stop.cancel();
    }

    #[tokio::test]
    async fn stop_interrupts_a_connect_in_progress() {
        // A listener that never accepts: connect() hangs until the timeout,
        // and stop must still end run() at once.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("deaf.sock");
        let _listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let target = Target {
            endpoint: format!("unix://{}", path.display()),
            token: None,
        };
        let source = || -> Result<FabricView, Box<dyn Error>> { Ok(view("", &[])) };
        let stop = CancellationToken::new();
        let stopper = stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            stopper.cancel();
        });
        let started = tokio::time::Instant::now();
        run(
            &target,
            reader(source, fast().poll),
            fast(),
            stop,
            &mut |_| {},
        )
        .await
        .unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn reconnects_after_the_plugin_closes_the_stream() {
        let plugin = plugin("t", true).await;
        let target = Target {
            endpoint: plugin.endpoint.clone(),
            token: Some("t".into()),
        };
        let source = || -> Result<FabricView, Box<dyn Error>> { Ok(view("", &[4])) };
        let stop = CancellationToken::new();
        let mut log = Vec::new();
        let waiter = plugin.recorder.clone();
        let stopper = stop.clone();
        tokio::spawn(async move {
            waiter.got_one.notified().await;
            waiter.got_one.notified().await;
            stopper.cancel();
        });
        run(
            &target,
            reader(source, fast().poll),
            fast(),
            stop,
            &mut |line| log.push(line),
        )
        .await
        .unwrap();
        assert!(plugin.recorder.views.lock().unwrap().len() >= 2);
        assert!(
            log.iter().any(|l| l == "stream closed by the plugin"),
            "{log:?}"
        );
        assert!(
            log.iter()
                .filter(|l| l.starts_with("publishing to"))
                .count()
                >= 2
        );
        plugin.stop.cancel();
    }

    #[tokio::test]
    async fn a_wrong_token_is_refused_and_retried_not_fatal() {
        let plugin = plugin("right", false).await;
        let target = Target {
            endpoint: plugin.endpoint.clone(),
            token: Some("wrong".into()),
        };
        let source = || -> Result<FabricView, Box<dyn Error>> { Ok(view("", &[1])) };
        let stop = CancellationToken::new();
        let mut log = Vec::new();
        let stopper = stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            stopper.cancel();
        });
        run(
            &target,
            reader(source, fast().poll),
            fast(),
            stop,
            &mut |line| log.push(line),
        )
        .await
        .unwrap();
        assert!(plugin.recorder.views.lock().unwrap().is_empty());
        assert!(
            log.iter().any(|l| l.starts_with("stream ended:")
                && l.contains("fabric view token missing or wrong")),
            "{log:?}"
        );
        plugin.stop.cancel();
    }

    #[tokio::test]
    async fn an_unreachable_endpoint_is_retried_until_stopped() {
        let dir = tempfile::tempdir().unwrap();
        let target = Target {
            endpoint: format!("unix://{}", dir.path().join("absent.sock").display()),
            token: None,
        };
        let reads = Arc::new(Mutex::new(0u32));
        let counter = reads.clone();
        let source = move || -> Result<FabricView, Box<dyn Error>> {
            *counter.lock().unwrap() += 1;
            Ok(view("", &[]))
        };
        let stop = CancellationToken::new();
        let mut log = Vec::new();
        let stopper = stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(250)).await;
            stopper.cancel();
        });
        run(
            &target,
            reader(source, fast().poll),
            fast(),
            stop,
            &mut |line| log.push(line),
        )
        .await
        .unwrap();
        assert!(
            log.iter()
                .filter(|l| l.starts_with("connect unix://"))
                .count()
                >= 2,
            "{log:?}"
        );
        assert!(
            *reads.lock().unwrap() >= 1,
            "the reader runs without a stream"
        );
    }

    #[tokio::test]
    async fn a_failed_read_within_a_heartbeat_keeps_the_stream() {
        let plugin = plugin("t", false).await;
        let target = Target {
            endpoint: plugin.endpoint.clone(),
            token: Some("t".into()),
        };
        // The second read, the first one inside the stream, fails; the
        // heartbeat is far away, so the stream stays and the next read sends.
        let mut n = 0;
        let source = move || -> Result<FabricView, Box<dyn Error>> {
            n += 1;
            if n == 2 {
                Err("fm unavailable".into())
            } else {
                Ok(view("", &[n]))
            }
        };
        let stop = CancellationToken::new();
        let mut log = Vec::new();
        let waiter = plugin.recorder.clone();
        let stopper = stop.clone();
        tokio::spawn(async move {
            waiter.got_one.notified().await;
            waiter.got_one.notified().await;
            stopper.cancel();
        });
        run(
            &target,
            reader(source, fast().poll),
            fast(),
            stop,
            &mut |line| log.push(line),
        )
        .await
        .unwrap();
        assert!(
            log.iter().any(|l| l == "fabric manager: fm unavailable"),
            "{log:?}"
        );
        assert_eq!(
            log.iter()
                .filter(|l| l.starts_with("publishing to"))
                .count(),
            1
        );
        assert_eq!(
            *plugin.recorder.views.lock().unwrap(),
            [view("", &[1]), view("", &[3])]
        );
        plugin.stop.cancel();
    }

    #[tokio::test]
    async fn an_unanswered_fabric_manager_closes_the_stream_until_it_answers() {
        let plugin = plugin("t", false).await;
        let target = Target {
            endpoint: plugin.endpoint.clone(),
            token: Some("t".into()),
        };
        // Answers, falls silent, answers again with a different view; the
        // test moves it between those as it observes the stream.
        let phase = Arc::new(Mutex::new(Some(view("", &[1]))));
        let fm = phase.clone();
        let source = move || -> Result<FabricView, Box<dyn Error>> {
            fm.lock()
                .unwrap()
                .clone()
                .ok_or_else(|| "fm restarting".into())
        };
        let pacing = Pacing {
            poll: Duration::from_millis(20),
            heartbeat: Duration::from_millis(100),
            backoff_max: Duration::from_millis(50),
        };
        let stop = CancellationToken::new();
        let mut log = Vec::new();
        let waiter = plugin.recorder.clone();
        let stopper = stop.clone();
        tokio::spawn(async move {
            waiter.got_one.notified().await;
            *phase.lock().unwrap() = None;
            waiter.ended.notified().await;
            *phase.lock().unwrap() = Some(view("", &[2]));
            loop {
                waiter.got_one.notified().await;
                if waiter.views.lock().unwrap().last() == Some(&view("", &[2])) {
                    break;
                }
            }
            stopper.cancel();
        });
        run(
            &target,
            reader(source, pacing.poll),
            pacing,
            stop,
            &mut |line| log.push(line),
        )
        .await
        .unwrap();
        assert!(
            log.iter()
                .any(|l| l == "fabric manager unanswered for 100ms: closing the stream"),
            "{log:?}"
        );
        assert!(
            log.iter()
                .filter(|l| l.starts_with("publishing to"))
                .count()
                >= 2,
            "{log:?}"
        );
        let views = plugin.recorder.views.lock().unwrap().clone();
        assert_eq!(views.first(), Some(&view("", &[1])));
        assert_eq!(views.last(), Some(&view("", &[2])));
        plugin.stop.cancel();
    }

    #[tokio::test]
    async fn the_stream_opens_only_with_a_view() {
        let plugin = plugin("t", false).await;
        let target = Target {
            endpoint: plugin.endpoint.clone(),
            token: Some("t".into()),
        };
        let mut n = 0;
        let source = move || -> Result<FabricView, Box<dyn Error>> {
            n += 1;
            if n <= 2 {
                Err("fm not up".into())
            } else {
                Ok(view("", &[9]))
            }
        };
        let stop = CancellationToken::new();
        let mut log = Vec::new();
        let waiter = plugin.recorder.clone();
        let stopper = stop.clone();
        tokio::spawn(async move {
            waiter.got_one.notified().await;
            stopper.cancel();
        });
        run(
            &target,
            reader(source, fast().poll),
            fast(),
            stop,
            &mut |line| log.push(line),
        )
        .await
        .unwrap();
        let first_error = log.iter().position(|l| l == "fabric manager: fm not up");
        let opened = log.iter().position(|l| l.starts_with("publishing to"));
        assert!(first_error < opened, "{log:?}");
        assert_eq!(*plugin.recorder.views.lock().unwrap(), [view("", &[9])]);
        plugin.stop.cancel();
    }
}
