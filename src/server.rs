use tokio::task::JoinHandle;

use crate::capability::negotiation;
use crate::error::RpcResult;
use crate::message::{Message, MessageType};
use crate::service::RpcService;
use crate::stream::{
    EncryptedRpcReceiver, EncryptedRpcSender, RpcAsyncReceiver, RpcAsyncSender, RpcReceiver,
    RpcSender,
};
use crate::transport::{TransportLayer, TransportListener};

/// RPC Server implementation.
pub struct RpcServer {
    // TODO: Track sessions.
    task: JoinHandle<()>,
}

impl RpcServer {
    pub async fn serve<A, L, H>(addr: A, service: H, encryption_required: bool) -> RpcResult<Self>
    where
        A: 'static,
        L: TransportListener<A> + Send + 'static,
        H: RpcService + 'static,
    {
        let listener = L::bind(addr).await?;

        let task = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((transport, _)) => {
                        // TODO: Safe abort.
                        tokio::spawn(Self::new_connection::<L::Transport, H>(
                            transport,
                            service.clone(),
                            encryption_required,
                        ));
                    }
                    Err(e) => log::error!("Accept failed: {e}"),
                }
            }
        });

        Ok(Self { task })
    }

    // Negotiates a new connection and starts a session over the transport layer upon success.
    async fn new_connection<T, H>(mut transport: T, service: H, encryption_required: bool)
    where
        T: TransportLayer + 'static,
        H: RpcService + 'static,
    {
        let proposed = match negotiation::read_frame(&mut transport).await {
            Ok(c) => c,
            Err(e) => {
                log::error!("Negotiation failed: {e}");
                return;
            }
        };

        if proposed.version != 1 {
            let _ = negotiation::reject(&mut transport).await;
            log::error!("Stream version mismatch");
            return;
        };

        // Always supported.
        if proposed.encryption {
            if let Err(e) = negotiation::confirm(&mut transport).await {
                log::error!("Failed to confirm: {e}");
                return;
            }

            let (r_key, w_key) = match negotiation::accept_key_exchange(&mut transport).await {
                Ok(keys) => keys,
                Err(e) => {
                    log::error!("Encryption session failed: {e}");
                    return;
                }
            };

            let (reader, writer) = transport.into_split();
            Self::new_session(
                EncryptedRpcReceiver::new(reader, r_key),
                EncryptedRpcSender::new(writer, w_key),
                service,
            )
            .await;
        } else {
            // Policy-dependent.
            if encryption_required {
                let _ = negotiation::reject(&mut transport).await;
                log::error!("Unencrypted session rejected by policy");
                return;
            }
            if let Err(e) = negotiation::confirm(&mut transport).await {
                log::error!("Failed to confirm: {e}");
                return;
            }
            let (reader, writer) = transport.into_split();
            Self::new_session(RpcReceiver::new(reader), RpcSender::new(writer), service).await;
        }
    }

    async fn new_session<R, W, H>(mut rx: R, mut tx: W, service: H)
    where
        R: RpcAsyncReceiver + 'static,
        W: RpcAsyncSender + 'static,
        H: RpcService + 'static,
    {
        loop {
            match rx.receive().await {
                Ok(message) => {
                    if let Err(e) = Self::process_incoming(&message, &service, &mut tx).await {
                        log::error!("Send error: {e}");
                        break;
                    }
                }
                Err(e) => {
                    log::info!("Receive error: {e}");
                    break;
                }
            }
        }
    }

    async fn process_incoming<H, S>(message: &Message, service: &H, sender: &mut S) -> RpcResult<()>
    where
        H: RpcService + 'static,
        S: RpcAsyncSender + 'static,
    {
        match &message.kind {
            MessageType::Call(call) => match service.call(call).await {
                Ok(result) => sender.send(&Message::reply(message.id, result)).await,
                Err(err) => sender.send(&Message::error(message.id, err)).await,
            },
            MessageType::Notification(notify) => {
                // Notification, no reply.
                let _ = service.notify(notify).await;
                Ok(())
            }
            MessageType::Ping => sender.send(&Message::pong(message.id)).await,
            _ => {
                log::warn!("Received unexpected message type: {:?}", message.kind);
                Ok(())
            }
        }
    }

    /// Consumes the instance and aborts its background task.
    pub fn shutdown(self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};

    use crate::capability::{self, RpcCapability};
    use crate::error::{ErrKind, RpcError};
    use crate::message::{Call, Reply};

    #[derive(Clone)]
    struct RpcTestService {
        counter: Arc<AtomicU32>,
    }

    impl RpcTestService {
        fn new() -> Self {
            Self {
                counter: Arc::new(AtomicU32::new(0)),
            }
        }
    }

    impl RpcService for RpcTestService {
        async fn call(&self, call: &Call) -> RpcResult<Reply> {
            match call.method {
                1 => {
                    let current = self.counter.fetch_add(1, Ordering::SeqCst);
                    Ok(Reply::with(&(current + 1)).unwrap())
                }
                _ => Err(RpcError::error(ErrKind::Unimplemented)),
            }
        }
    }

    #[tokio::test]
    async fn test_tcp_rpc_server() {
        let service = RpcTestService::new();
        let server =
            RpcServer::serve::<&str, TcpListener, RpcTestService>("127.0.0.1:8000", service, false)
                .await
                .unwrap();

        let mut transport = TcpStream::connect("127.0.0.1:8000").await.unwrap();

        // Perform capability negotiation on the client side.
        capability::negotiation::initiate(&mut transport, RpcCapability::new(1, false))
            .await
            .expect("client negotiation failed");

        let (reader, writer) = transport.into_split();
        let mut rpc_reader = RpcReceiver::new(reader);
        let mut rpc_writer = RpcSender::new(writer);

        let call_msg = Message::call_with(1, ()).unwrap();
        rpc_writer.send(&call_msg).await.unwrap();

        let reply_msg = rpc_reader.receive().await.unwrap();
        match reply_msg.kind {
            MessageType::Reply(reply) => {
                let response: u32 = crate::message::Message::decode_as(&reply.data).unwrap();
                assert!(response == 1);
            }
            _ => panic!("Expected reply"),
        }

        server.shutdown();
    }

    #[tokio::test]
    async fn test_tcp_rpc_server_encryption_policy() {
        let service = RpcTestService::new();

        // Server with encryption-only policy.
        let server =
            RpcServer::serve::<&str, TcpListener, RpcTestService>("127.0.0.1:8001", service, true)
                .await
                .unwrap();

        // Unencrypted session.
        {
            let mut transport = TcpStream::connect("127.0.0.1:8001").await.unwrap();
            let response =
                capability::negotiation::initiate(&mut transport, RpcCapability::new(1, false))
                    .await;
            assert!(response == Err(RpcError::error(ErrKind::CapabilityMismatch)));
        };

        // Encrypted session.
        let mut transport = TcpStream::connect("127.0.0.1:8001").await.unwrap();

        capability::negotiation::initiate(&mut transport, RpcCapability::new(1, true))
            .await
            .expect("client negotiation failed");

        let (r_key, w_key) = negotiation::initiate_key_exchange(&mut transport)
            .await
            .expect("client encryption setup failed");

        let (r, w) = transport.into_split();
        let mut rpc_rx = EncryptedRpcReceiver::new(r, r_key);
        let mut rpc_tx = EncryptedRpcSender::new(w, w_key);

        let call_msg = Message::call_with(1, ()).unwrap();
        rpc_tx.send(&call_msg).await.unwrap();

        let reply_msg = rpc_rx.receive().await.unwrap();
        match reply_msg.kind {
            MessageType::Reply(reply) => {
                let response: u32 = crate::message::Message::decode_as(&reply.data).unwrap();
                assert!(response == 1);
            }
            _ => panic!("Expected reply"),
        }

        server.shutdown();
    }

    #[tokio::test]
    async fn test_unix_rpc_server() {
        let path = "unix_server_test.sock";

        let service = RpcTestService::new();
        let server = RpcServer::serve::<&str, UnixListener, RpcTestService>(path, service, false)
            .await
            .unwrap();

        let mut transport = UnixStream::connect(path).await.unwrap();

        capability::negotiation::initiate(&mut transport, RpcCapability::new(1, false))
            .await
            .expect("client negotiation failed");

        let (reader, writer) = transport.into_split();
        let mut rpc_reader = RpcReceiver::new(reader);
        let mut rpc_writer = RpcSender::new(writer);

        tokio::time::sleep(Duration::from_millis(10)).await;

        let call_msg = Message::call_with(1, ()).unwrap();
        rpc_writer.send(&call_msg).await.unwrap();

        let reply_msg = rpc_reader.receive().await.unwrap();
        match reply_msg.kind {
            MessageType::Reply(reply) => {
                let response: u32 = crate::message::Message::decode_as(&reply.data).unwrap();
                assert!(response == 1);
            }
            _ => panic!("Expected reply"),
        }

        server.shutdown();
        std::fs::remove_file(path).unwrap();
    }
}
