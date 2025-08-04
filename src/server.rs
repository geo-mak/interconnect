use std::fmt::Debug;
use std::marker::PhantomData;

use tokio::task::JoinHandle;

use crate::capability::{RpcCapability, negotiation};
use crate::connection::RpcListener;
use crate::error::RpcResult;
use crate::message::{Message, MessageType};
use crate::service::RpcService;
use crate::transport::{
    EncryptedRpcReceiver, EncryptedRpcSender, OwnedSplitStream, RpcAsyncReceiver, RpcAsyncSender,
    RpcReceiver, RpcSender,
};

/// RPC Server implementation.
pub struct RpcServer<L, A, H>
where
    L: RpcListener<A>,
    A: Debug,
    H: RpcService,
{
    service: H,
    capability: RpcCapability,
    _l: PhantomData<L>,
    _a: PhantomData<A>,
}

impl<L, A, H> RpcServer<L, A, H>
where
    L: RpcListener<A> + Send + 'static,
    A: Debug + 'static,
    H: RpcService + 'static,
{
    pub fn new(service: H, capability: RpcCapability) -> Self {
        Self {
            service,
            capability,
            _l: PhantomData,
            _a: PhantomData,
        }
    }

    pub async fn start(&mut self, addr: A) -> RpcResult<JoinHandle<()>> {
        let listener = L::bind(addr).await?;
        let service = self.service.clone();
        let capability = self.capability;

        let handle = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let service = service.clone();
                        // Capability is cheap to copy.
                        tokio::spawn(Self::new_connection(stream, service, capability));
                    }
                    Err(e) => log::error!("Accept failed: {e}"),
                }
            }
        });

        Ok(handle)
    }

    // TODO: Safe abort, if requested.
    async fn new_connection(mut io_stream: L::Stream, service: H, capability: RpcCapability) {
        let consensus = match negotiation::accept_capability(&mut io_stream, capability).await {
            Ok(c) => c,
            Err(e) => {
                log::error!("Negotiation failed: {e}");
                return;
            }
        };

        if consensus.encryption {
            let (r_key, w_key) = match negotiation::accept_key_exchange(&mut io_stream).await {
                Ok(keys) => keys,
                Err(e) => {
                    log::error!("Encryption session failed: {e}");
                    return;
                }
            };
            let (reader, writer) = io_stream.owned_split();
            Self::handle_session(
                EncryptedRpcReceiver::new(reader, r_key),
                EncryptedRpcSender::new(writer, w_key),
                service,
            )
            .await;
        } else {
            let (reader, writer) = io_stream.owned_split();
            Self::handle_session(RpcReceiver::new(reader), RpcSender::new(writer), service).await;
        }
    }

    async fn handle_session<R, W>(mut rx: R, mut tx: W, service: H)
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

    use crate::capability;
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
        let mut server = RpcServer::<TcpListener, &str, RpcTestService>::new(
            service,
            RpcCapability::new(1, false),
        );

        let handle = server.start("127.0.0.1:8000").await.unwrap();

        let mut io_stream = TcpStream::connect("127.0.0.1:8000").await.unwrap();

        // Perform capability negotiation on the client side.
        capability::negotiation::initiate_capability(&mut io_stream, RpcCapability::new(1, false))
            .await
            .expect("client negotiation failed");

        let (reader, writer) = io_stream.owned_split();
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
        let mut server = RpcServer::<UnixListener, &str, RpcTestService>::new(
            service,
            RpcCapability::new(1, false),
        );

        let handle = server.start(path).await.unwrap();

        let mut io_stream = UnixStream::connect(path).await.unwrap();

        // Perform capability negotiation.
        capability::negotiation::initiate_capability(&mut io_stream, RpcCapability::new(1, false))
            .await
            .expect("client negotiation failed");

        let (reader, writer) = io_stream.owned_split();
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
