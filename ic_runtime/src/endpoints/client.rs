use core::marker::PhantomData;
use core::time::Duration;

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::codec::decode::Decode;
use crate::codec::decoder::{Decoded, Decoder};
use crate::codec::encode::Encode;
use crate::codec::encoder::Encoder;
use crate::codec::types::core::ProtocolType;
use crate::codec::types::message::TypeMessageHeader;
use crate::coop::traits::{ControlHandle, Executor, Timer};
use crate::endpoints::publishers::Publishers;
use crate::error::{ErrKind, ProtocolError, ProtocolResult};
use crate::mem::MemoryProvider;
use crate::reports::traits::{NoContent, Reporter};
use crate::transport::traits::{Transport, TransportReceiver, TransportSender};

struct ClientState<S, E, P, R>
where
    P: MemoryProvider,
{
    publishers: Publishers<ProtocolResult<P::ReceiveSegment>>,
    sender: Mutex<S>,
    provider: P,
    reporter: R,
    _executor: PhantomData<E>,
}

impl<S, E, P, R> ClientState<S, E, P, R>
where
    P: MemoryProvider,
{
    #[inline(always)]
    fn new(capacity: usize, sender: S, provider: P, reporter: R) -> ClientState<S, E, P, R> {
        ClientState {
            publishers: Publishers::new(capacity),
            sender: Mutex::const_new(sender),
            provider,
            reporter,
            _executor: PhantomData,
        }
    }
}

pub struct CoreClient<T, E, P, R>
where
    T: Transport,
    E: Executor,
    P: MemoryProvider,
{
    state: Arc<ClientState<T::Sender, E, P, R>>,
    recv_task: E::ControlHandle<()>,
}

impl<T, E, P, R> CoreClient<T, E, P, R>
where
    T: Transport,
    E: Executor + Send + Sync + 'static,
    R: Reporter + Send + Sync + 'static,
    P: MemoryProvider<SendSegment = T::SendSegment> + Send + Sync,
    P: MemoryProvider<ReceiveSegment = T::ReceiveSegment> + Send + Sync + 'static,
    T::SendSegment: Send,
    T::Sender: Send + 'static,
    T::ReceiveSegment: Send,
    T::Receiver: Send + 'static,
    P::SendSegment: Encoder,
    P::ReceiveSegment: Decoder,
{
    pub async fn start(
        capacity: usize,
        transport: T,
        executor: &E,
        provider: P,
        reporter: R,
    ) -> ProtocolResult<CoreClient<T, E, P, R>> {
        let (sender, mut receiver) = transport.split();

        let state = Arc::new(ClientState::new(capacity, sender, provider, reporter));
        let client_state = Arc::clone(&state);

        let recv_task = executor.spawn(async move {
            let reporter = &client_state.reporter;

            // TODO: Refine error-handling, the current implementation is too rigid regarding errors.
            loop {
                let Some(mut recv_segment) = client_state.provider.acquire_receive() else {
                    reporter.error("Failed to get memory for receiving", &NoContent);
                    break;
                };

                if let Err(err) = receiver.receive(&mut recv_segment).await {
                    reporter.error("Receiving error", &err);
                    break;
                };

                match TypeMessageHeader::decode_header(&mut recv_segment) {
                    Ok((id, _directive)) => {
                        // TODO: Directive matching according to directive-rules.
                        client_state.publishers.publish(id, Ok(recv_segment));
                    }
                    Err(err) => {
                        reporter.error("Failed to decode header", &err);
                        break;
                    }
                }
            }
        });

        Ok(Self { state, recv_task })
    }

    async fn send<'a, I, M, O>(
        &self,
        op: u64,
        message: &'a M,
    ) -> ProtocolResult<Decoded<O, P::ReceiveSegment>>
    where
        I: ProtocolType<Limits = ()>,
        &'a M: Encode<I, P::SendSegment>,
        O: ProtocolType<Limits = ()> + Decode<P::ReceiveSegment>,
    {
        // TODO: Generate ID according to id-rules.
        if let Some(publisher) = &self.state.publishers.acquire() {
            if let Some(mut segment) = self.state.provider.acquire_send() {
                TypeMessageHeader::encode_header(publisher.id, op, &mut segment)?;

                segment.encode_next(message, ())?;

                // TODO: Async-strategy regarding cancellation.
                self.state.sender.lock().await.send(&mut segment).await?;

                let result = E::Timer::timeout(Duration::from_secs(30), publisher.wait());

                // RT_ASSERT.
                match result.await?.unwrap() {
                    Ok(response) => {
                        let output = response.decode::<O>(())?;
                        return Ok(output);
                    }
                    Err(err) => return Err(err),
                }
            }
        }

        Err(ProtocolError::error(ErrKind::CapacityLimit))
    }

    async fn send_one_way<'a, I, M>(&self, op: u64, message: &'a M) -> ProtocolResult<()>
    where
        I: ProtocolType<Limits = ()>,
        &'a M: Encode<I, P::SendSegment>,
    {
        let Some(mut segment) = self.state.provider.acquire_send() else {
            return Err(ProtocolError::error(ErrKind::CapacityLimit));
        };

        TypeMessageHeader::encode_header(0, op, &mut segment)?;
        segment.encode_next(message, ())?;
        self.state.sender.lock().await.send(&mut segment).await
    }

    async fn send_nullary<O>(&self, op: u64) -> ProtocolResult<Decoded<O, P::ReceiveSegment>>
    where
        O: ProtocolType<Limits = ()> + Decode<P::ReceiveSegment>,
    {
        if let Some(publisher) = &self.state.publishers.acquire() {
            if let Some(mut segment) = self.state.provider.acquire_send() {
                TypeMessageHeader::encode_header(publisher.id, op, &mut segment)?;

                self.state.sender.lock().await.send(&mut segment).await?;

                let result = E::Timer::timeout(Duration::from_secs(30), publisher.wait());

                // RT_ASSERT.
                match result.await?.unwrap() {
                    Ok(response) => {
                        let decoded = response.decode::<O>(())?;
                        return Ok(decoded);
                    }
                    Err(err) => return Err(err),
                }
            }
        }

        Err(ProtocolError::error(ErrKind::CapacityLimit))
    }

    /// Sends a one-way nullary call without response.
    ///
    /// This call is untracked, if the target operation returns response,
    /// the response will be discarded.
    async fn send_nullary_one_way(&self, op: u64) -> ProtocolResult<()> {
        let Some(mut segment) = self.state.provider.acquire_send() else {
            return Err(ProtocolError::error(ErrKind::CapacityLimit));
        };

        TypeMessageHeader::encode_header(0, op, &mut segment)?;
        self.state.sender.lock().await.send(&mut segment).await
    }

    /// Closes its sender and shutdowns the receiving task in graceful manner.
    ///
    /// Buffered data will be sent followed by "FIN" message.
    ///
    /// Any attempts to send messages after this call will return `Broken pipe` I/O error.
    async fn terminate(&mut self) -> ProtocolResult<()> {
        // TODO:
        // Task can be canceled blindly because it doesn't currently delegate to external processors.
        // This will not be the case later.
        self.recv_task.abort();
        self.state.sender.lock().await.terminate().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::types::core::TypeU64;
    use crate::codec::types::message::TypeMessageHeader;
    use crate::coop::executors::TokioExecutor;
    use crate::mem::{IOPool, IOPoolSegment, IOSegment};
    use crate::transport::stream::uds::{UnixLink, UnixLinkServer};
    use crate::transport::traits::{TransportInitiator, TransportServer};

    #[tokio::test]
    async fn test_core_client_send_receive() {
        let path = "/tmp/test_core_client_send.sock";
        let _ = std::fs::remove_file(path);

        let capacity = 2;
        let pool = IOPool::new(3, 32);
        let executor = TokioExecutor;
        let reporter = ();

        let server = UnixLinkServer::create(&path).await.unwrap();

        let server_pool = pool.clone();

        let server_handle = tokio::spawn(async move {
            let (initiator, _) = server.accept().await.unwrap();
            let mut transport = initiator.initiate().await.unwrap();

            let mut segment = server_pool.acquire().unwrap();

            transport.receive(&mut segment).await.unwrap();

            let (id, op) = TypeMessageHeader::decode_header(&mut segment).unwrap();
            assert_eq!(op, 10);

            segment.clear();
            TypeMessageHeader::encode_header(id, op, &mut segment).unwrap();

            segment.encode_next(&TypeU64(200), ()).unwrap();

            transport.send(&mut segment).await.unwrap();
        });

        let transport = UnixLink::connect(&path).await.unwrap();

        let client = CoreClient::start(capacity, transport, &executor, pool, reporter)
            .await
            .unwrap();

        let decoded: Decoded<TypeU64, IOPoolSegment> =
            client.send(10, &TypeU64(100)).await.unwrap();

        assert_eq!(*decoded, 200);

        server_handle.await.unwrap();
        let _ = std::fs::remove_file(path);
    }
}
