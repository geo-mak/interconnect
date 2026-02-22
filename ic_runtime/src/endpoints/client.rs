use core::marker::PhantomData;
use core::time::Duration;

use std::sync::Arc;

use tokio::sync::Mutex;

use crate::codec::decode::Decode;
use crate::codec::decoder::Decoder;
use crate::codec::encode::Encode;
use crate::codec::encoder::Encoder;
use crate::coop::traits::{ControlHandle, Executor, Timer};
use crate::endpoints::publishers::Publishers;
use crate::error::{ErrKind, ProtocolError, ProtocolResult};
use crate::mem::MemoryProvider;
use crate::reports::traits::{NoContent, Reporter};
use crate::transport::traits::{Transport, TransportReceiver, TransportSender};
use crate::types::convert::{FromProtocolType, IntoNativeType};
use crate::types::core::ProtocolType;
use crate::types::limits::TypeLimits;
use crate::types::message::TypeMessageHeader;

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
            loop {
                let Some(mut recv_segment) = client_state.provider.acquire_receive() else {
                    reporter.error("Failed to get memory for receiving", &NoContent);
                    break;
                };

                match receiver.receive(&mut recv_segment).await {
                    Ok(_) => {
                        if let Err(err) = Self::process_message(recv_segment, &client_state).await {
                            reporter.error("Processing error", &err);
                            break;
                        }
                    }

                    Err(err) => {
                        reporter.error("Receiving error", &err);
                        break;
                    }
                }
            }
        });

        Ok(Self { state, recv_task })
    }

    async fn process_message(
        mut message: P::ReceiveSegment,
        state: &Arc<ClientState<T::Sender, E, P, R>>,
    ) -> ProtocolResult<()> {
        let (id, _directive) = { TypeMessageHeader::decode_header(&mut message)? };
        // TODO: Directive matching according to directive-rules.
        state.publishers.publish(id, Ok(message));
        Ok(())
    }

    async fn send<'a, A, M, V>(
        &self,
        op: u64,
        message: &'a M,
    ) -> ProtocolResult<<A as IntoNativeType>::NativeType>
    where
        A: ProtocolType + TypeLimits<Limits = ()>,
        &'a M: Encode<A, P::SendSegment>,
        A: Decode<P::ReceiveSegment> + TypeLimits<Limits = ()> + IntoNativeType,
        <A as IntoNativeType>::NativeType:
            for<'de> FromProtocolType<<A as ProtocolType>::Type<'de>>,
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
                        let decoded = response.decode::<A>(())?;
                        return Ok(decoded.into_native());
                    }
                    Err(err) => return Err(err),
                }
            }
        }

        Err(ProtocolError::error(ErrKind::CapacityLimit))
    }

    async fn send_one_way<'a, A, M>(&self, op: u64, message: &'a M) -> ProtocolResult<()>
    where
        A: ProtocolType + TypeLimits<Limits = ()>,
        &'a M: Encode<A, P::SendSegment>,
    {
        let Some(mut segment) = self.state.provider.acquire_send() else {
            return Err(ProtocolError::error(ErrKind::CapacityLimit));
        };

        TypeMessageHeader::encode_header(0, op, &mut segment)?;
        segment.encode_next(message, ())?;
        self.state.sender.lock().await.send(&mut segment).await
    }

    async fn send_nullary<V>(&self, op: u64) -> ProtocolResult<<V as IntoNativeType>::NativeType>
    where
        V: Decode<P::ReceiveSegment> + TypeLimits<Limits = ()> + IntoNativeType,
        <V as IntoNativeType>::NativeType:
            for<'de> FromProtocolType<<V as ProtocolType>::Type<'de>>,
    {
        if let Some(publisher) = &self.state.publishers.acquire() {
            if let Some(mut segment) = self.state.provider.acquire_send() {
                TypeMessageHeader::encode_header(publisher.id, op, &mut segment)?;

                self.state.sender.lock().await.send(&mut segment).await?;

                let result = E::Timer::timeout(Duration::from_secs(30), publisher.wait());

                // RT_ASSERT.
                match result.await?.unwrap() {
                    Ok(response) => {
                        let decoded = response.decode::<V>(())?;
                        return Ok(decoded.into_native());
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
    /// This call doesn't have immediate effect and may take longer time,
    /// because it allows critical regions to fully complete their execution.
    ///
    /// Buffered data will be sent followed by "FIN" message.
    ///
    /// Any attempts to send messages after this call will return `Broken pipe` I/O error.
    async fn terminate(&mut self) -> ProtocolResult<()> {
        // TODO:
        // Task can be canceled blindly because it is currently doesn't delegate to external processors.
        // This will not be the case later.
        self.recv_task.abort();
        self.state.sender.lock().await.terminate().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coop::executors::TokioExecutor;
    use crate::mem::{IOPool, IOPoolSegment, IOSegment};
    use crate::transport::stream::uds::{UnixLink, UnixLinkServer};
    use crate::transport::traits::{TransportInitiator, TransportServer};
    use crate::types::core::TypeU64;
    use crate::types::message::TypeMessageHeader;

    #[tokio::test]
    async fn test_core_client_send_receive() {
        let path = "/tmp/test_core_client_send.sock";
        let _ = std::fs::remove_file(path);

        let capacity = 2;
        let pool = IOPool::new(3, 32);
        let executor = TokioExecutor;
        let reporter = ();

        let server = UnixLinkServer::<IOPoolSegment, IOPoolSegment>::create(&path)
            .await
            .unwrap();

        let server_pool = pool.clone();
        let server_handle = tokio::spawn(async move {
            let (initiator, _) = server.accept().await.unwrap();
            let mut link = initiator.initiate().await.unwrap();

            let mut segment = server_pool.acquire().unwrap();

            // Receive.
            link.receive(&mut segment).await.unwrap();

            // Decode.
            let (id, op) = TypeMessageHeader::decode_header(&mut segment).unwrap();
            assert_eq!(op, 10);

            // Encode.
            segment.clear();
            TypeMessageHeader::encode_header(id, op, &mut segment).unwrap();

            segment.encode_next(&TypeU64(200), ()).unwrap();

            // Resend.
            link.send(&mut segment).await.unwrap();
        });

        // Connect.
        let transport = UnixLink::connect(&path).await.unwrap();
        let client = CoreClient::start(capacity, transport, &executor, pool, reporter)
            .await
            .unwrap();

        // Send.
        let response: u64 = client
            .send::<TypeU64, TypeU64, TypeU64>(10, &TypeU64(100))
            .await
            .unwrap();

        assert_eq!(response, 200);

        server_handle.await.unwrap();
        let _ = std::fs::remove_file(path);
    }
}
