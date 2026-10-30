// SPDX-License-Identifier: Apache-2.0
// Copyright (c) NVIDIA CORPORATION

//! Publish the fabric view to kata-device-plugin.
//!
//! The plugin hosts the view's Publish stream and hands the one container it
//! gives the fabric to an endpoint and a token through its environment
//! ([`pcilibs_rs::fabric::ENDPOINT_ENV`], [`pcilibs_rs::fabric::TOKEN_ENV`]).  This client opens one stream, sends
//! the view whenever Fabric Manager's lists change, resends it on a
//! heartbeat so the plugin can tell a quiet publisher from a dead one, and
//! reconnects with backoff when the stream ends.  The open stream is the
//! plugin's liveness signal: it withdraws the view when the stream closes.
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
pub type Source<'a> = dyn FnMut() -> Result<FabricView, Box<dyn Error>> + 'a;

/// Publish until `stop` is cancelled.  Returns early only for a token that
/// is not valid gRPC metadata, which no plugin issues: retrying cannot fix
/// it.
pub async fn run(
    target: &Target,
    source: &mut Source<'_>,
    pacing: Pacing,
    stop: CancellationToken,
    log: &mut dyn FnMut(String),
) -> Result<(), Box<dyn Error>> {
    let first_backoff = Duration::from_secs(1).min(pacing.backoff_max);
    let mut backoff = first_backoff;
    while !stop.is_cancelled() {
        let channel = match connect(&target.endpoint).await {
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

        let mut ticker = tokio::time::interval(pacing.poll);
        let mut last: Option<FabricView> = None;
        let opened = tokio::time::Instant::now();
        let mut sent = opened;
        let outcome = loop {
            tokio::select! {
                _ = stop.cancelled() => break None,
                result = &mut rpc => break Some(result),
                _ = ticker.tick() => {
                    let view = match source() {
                        Ok(view) => view,
                        Err(e) => {
                            log(format!("fabric manager: {e}"));
                            continue;
                        }
                    };
                    let changed = last.as_ref() != Some(&view);
                    if changed || sent.elapsed() >= pacing.heartbeat {
                        if tx.send(view.clone()).await.is_err() {
                            // The receiver is gone: the rpc is ending and
                            // reports why on the next turn.
                            break Some(rpc.await);
                        }
                        sent = tokio::time::Instant::now();
                        if changed {
                            log(format!("view changed: {}", describe(&view)));
                        }
                        last = Some(view);
                    }
                }
            }
        };
        drop(tx);
        match outcome {
            None => return Ok(()),
            Some(Ok(_)) => log("stream closed by the plugin".to_owned()),
            Some(Err(status)) => log(format!("stream ended: {status}")),
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

async fn pause(duration: Duration, stop: &CancellationToken) {
    tokio::select! {
        _ = stop.cancelled() => {}
        _ = tokio::time::sleep(duration) => {}
    }
}

/// One line for the log: active partitions and their sizes.
fn describe(view: &FabricView) -> String {
    let active: Vec<String> = view
        .partitions
        .iter()
        .flat_map(|list| list.partition_info.iter())
        .filter(|partition| partition.is_active)
        .map(|partition| format!("{}x{}", partition.partition_id, partition.gpu_info.len()))
        .collect();
    format!(
        "unit {:?}, active partitions (id x gpus): {}",
        view.unit,
        if active.is_empty() {
            "none".to_owned()
        } else {
            active.join(" ")
        }
    )
}

/// HTTP/2 keepalives catch a plugin that vanished without closing, which the
/// heartbeat alone would notice only on the next send.
async fn connect(endpoint: &str) -> Result<Channel, tonic::transport::Error> {
    const KEEPALIVE: Duration = Duration::from_secs(10);
    match endpoint.strip_prefix("unix://") {
        Some(path) => {
            let path = path.to_owned();
            // The URI is a placeholder: the connector ignores it and dials
            // the socket.
            Endpoint::try_from("http://[::]:0")?
                .http2_keep_alive_interval(KEEPALIVE)
                .keep_alive_timeout(KEEPALIVE)
                .keep_alive_while_idle(true)
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
        None => {
            Endpoint::from_shared(endpoint.to_owned())?
                .http2_keep_alive_interval(KEEPALIVE)
                .keep_alive_timeout(KEEPALIVE)
                .keep_alive_while_idle(true)
                .connect()
                .await
        }
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

    /// A plugin stand-in: records every view, wakes the test on each, and
    /// optionally closes the stream after the first message.
    #[derive(Default)]
    struct Recorder {
        views: Mutex<Vec<FabricView>>,
        got_one: Notify,
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

    #[test]
    fn describe_names_active_partitions() {
        assert_eq!(
            describe(&view("gpu", &[])),
            "unit \"gpu\", active partitions (id x gpus): none"
        );
        assert_eq!(
            describe(&view("", &[2, 6])),
            "unit \"\", active partitions (id x gpus): 2x2 6x2"
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
        let mut source = move || -> Result<FabricView, Box<dyn Error>> {
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
        run(&target, &mut source, fast(), stop, &mut |line| {
            log.push(line)
        })
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
        let mut source = || -> Result<FabricView, Box<dyn Error>> { Ok(view("", &[1])) };
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
        run(&target, &mut source, pacing, stop, &mut |_| {})
            .await
            .unwrap();
        let views = plugin.recorder.views.lock().unwrap();
        assert!(views.len() >= 3, "{}", views.len());
        assert!(views.iter().all(|v| *v == view("", &[1])));
        plugin.stop.cancel();
    }

    #[tokio::test]
    async fn reconnects_after_the_plugin_closes_the_stream() {
        let plugin = plugin("t", true).await;
        let target = Target {
            endpoint: plugin.endpoint.clone(),
            token: Some("t".into()),
        };
        let mut source = || -> Result<FabricView, Box<dyn Error>> { Ok(view("", &[4])) };
        let stop = CancellationToken::new();
        let mut log = Vec::new();
        let waiter = plugin.recorder.clone();
        let stopper = stop.clone();
        tokio::spawn(async move {
            waiter.got_one.notified().await;
            waiter.got_one.notified().await;
            stopper.cancel();
        });
        run(&target, &mut source, fast(), stop, &mut |line| {
            log.push(line)
        })
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
        let mut source = || -> Result<FabricView, Box<dyn Error>> { Ok(view("", &[1])) };
        let stop = CancellationToken::new();
        let mut log = Vec::new();
        let stopper = stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            stopper.cancel();
        });
        run(&target, &mut source, fast(), stop, &mut |line| {
            log.push(line)
        })
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
        let mut reads = 0;
        let mut source = || -> Result<FabricView, Box<dyn Error>> {
            reads += 1;
            Ok(view("", &[]))
        };
        let stop = CancellationToken::new();
        let mut log = Vec::new();
        let stopper = stop.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(250)).await;
            stopper.cancel();
        });
        run(&target, &mut source, fast(), stop, &mut |line| {
            log.push(line)
        })
        .await
        .unwrap();
        assert!(
            log.iter()
                .filter(|l| l.starts_with("connect unix://"))
                .count()
                >= 2,
            "{log:?}"
        );
        assert_eq!(reads, 0, "no read without a stream");
    }

    #[tokio::test]
    async fn a_source_error_keeps_the_stream_open() {
        let plugin = plugin("t", false).await;
        let target = Target {
            endpoint: plugin.endpoint.clone(),
            token: Some("t".into()),
        };
        let mut n = 0;
        let mut source = move || -> Result<FabricView, Box<dyn Error>> {
            n += 1;
            if n == 1 {
                Err("fm unavailable".into())
            } else {
                Ok(view("", &[5]))
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
        run(&target, &mut source, fast(), stop, &mut |line| {
            log.push(line)
        })
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
        assert_eq!(plugin.recorder.views.lock().unwrap().len(), 1);
        plugin.stop.cancel();
    }
}
