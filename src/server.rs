use std::fmt::Debug;
use std::marker::PhantomData;

use tokio::task::JoinHandle;

use crate::capability::negotiation;
use crate::error::RpcResult;
use crate::message::{Message, MessageType};
use crate::service::RpcService;
use crate::stream::{
    EncryptedRpcReceiver, EncryptedRpcSender, RpcAsyncReceiver, RpcAsyncSender, RpcReceiver,
    RpcSender,
};
use crate::transport::{OwnedSplitTransport, TransportListener};

/// RPC Server implementation.
pub struct RpcServer<L, A, H>
where
    L: TransportListener<A>,
    A: Debug,
    H: RpcService,
{
    service: H,
    _l: PhantomData<L>,
    _a: PhantomData<A>,
}

impl<L, A, H> RpcServer<L, A, H>
where
    L: TransportListener<A> + Send + 'static,
    A: Debug + 'static,
    H: RpcService + 'static,
{
    pub fn new(service: H) -> Self {
        Self {
            service,
            _l: PhantomData,
            _a: PhantomData,
        }
    }

    pub async fn start(&mut self, addr: A, allow_unencrypted: bool) -> RpcResult<JoinHandle<()>> {
        let listener = L::bind(addr).await?;
        let service = self.service.clone();

        let handle = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((transport, _)) => {
                        tokio::spawn(Self::new_connection(
                            transport,
                            service.clone(),
                            allow_unencrypted,
                        ));
                    }
                    Err(e) => log::error!("Accept failed: {e}"),
                }
            }
        });

        Ok(handle)
    }

    // Negotiates a new connection and starts a session over the transport layer upon success.
    async fn new_connection(mut transport: L::Transport, service: H, allow_unencrypted: bool) {
        let proposed = match negotiation::read_frame(&mut transport).await {
            Ok(c) => c,
            Err(e) => {
                log::error!("Negotiation failed: {e}");
                return;
            }
        };

        if proposed.version != 1 {
            let _ = negotiation::reject(&mut transport).await;
            log::error!("Capability version mismatch");
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

            let (reader, writer) = transport.owned_split();
            Self::new_session(
                EncryptedRpcReceiver::new(reader, r_key),
                EncryptedRpcSender::new(writer, w_key),
                service,
            )
            .await;
        } else {
            // Policy-dependent.
            if !allow_unencrypted {
                let _ = negotiation::reject(&mut transport).await;
                log::error!("Unencrypted session rejected by policy");
                return;
            }
            if let Err(e) = negotiation::confirm(&mut transport).await {
                log::error!("Failed to confirm: {e}");
                return;
            }
            let (reader, writer) = transport.owned_split();
            Self::new_session(RpcReceiver::new(reader), RpcSender::new(writer), service).await;
        }
    }

    async fn new_session<R, W>(mut rx: R, mut tx: W, service: H)
    where
        R: RpcAsyncReceiver + 'static,
        W: RpcAsyncSender + 'static,
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

    async fn process_incoming<S>(message: &Message, service: &H, sender: &mut S) -> RpcResult<()>
    where
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};

    use crate::capability::{self, RpcCapability};
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
            let count = self.counter.fetch_add(1, Ordering::SeqCst);
            let response = format!(
                "Method called {} times. Data size: {}",
                count + 1,
                call.data.len()
            );
            Ok(Reply::with(&response)?)
        }
    }

    #[tokio::test]
    async fn test_tcp_rpc_server() {
        let service = RpcTestService::new();
        let mut server = RpcServer::<TcpListener, &str, RpcTestService>::new(service);

        let handle = server.start("127.0.0.1:8000", true).await.unwrap();

        let mut transport = TcpStream::connect("127.0.0.1:8000").await.unwrap();

        // Perform capability negotiation on the client side.
        capability::negotiation::initiate(&mut transport, RpcCapability::new(1, false))
            .await
            .expect("client negotiation failed");

        let (reader, writer) = transport.owned_split();
        let mut rpc_reader = RpcReceiver::new(reader);
        let mut rpc_writer = RpcSender::new(writer);

        let payload = b"hi there".to_vec();
        let call_msg = Message::call(1, payload.clone());
        rpc_writer.send(&call_msg).await.unwrap();

        let reply_msg = rpc_reader.receive().await.unwrap();
        match reply_msg.kind {
            MessageType::Reply(reply) => {
                let response: String = crate::message::Message::decode_as(&reply.data).unwrap();
                assert!(response.contains("Method called"));
                assert!(response.contains(&payload.len().to_string()));
            }
            _ => panic!("Expected reply"),
        }

        handle.abort();
    }

    #[tokio::test]
    async fn test_unix_rpc_server() {
        let path = "unix_server_test.sock";

        let service = RpcTestService::new();
        let mut server = RpcServer::<UnixListener, &str, RpcTestService>::new(service);

        let handle = server.start(path, true).await.unwrap();

        let mut transport = UnixStream::connect(path).await.unwrap();

        capability::negotiation::initiate(&mut transport, RpcCapability::new(1, false))
            .await
            .expect("client negotiation failed");

        let (reader, writer) = transport.owned_split();
        let mut rpc_reader = RpcReceiver::new(reader);
        let mut rpc_writer = RpcSender::new(writer);

        tokio::time::sleep(Duration::from_millis(10)).await;

        let payload = b"hi there".to_vec();
        let call_msg = Message::call(1, payload.clone());
        rpc_writer.send(&call_msg).await.unwrap();

        let reply_msg = rpc_reader.receive().await.unwrap();
        match reply_msg.kind {
            MessageType::Reply(reply) => {
                let response: String = crate::message::Message::decode_as(&reply.data).unwrap();
                assert!(response.contains("Method called"));
                assert!(response.contains(&payload.len().to_string()));
            }
            _ => panic!("Expected reply"),
        }

        handle.abort();
        std::fs::remove_file(path).unwrap();
    }
}
