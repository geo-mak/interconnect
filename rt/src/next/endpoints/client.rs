use core::time::Duration;

use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::next::codec::decode::Decode;
use crate::next::codec::decoder::Decoder;
use crate::next::codec::encode::Encode;
use crate::next::codec::encoder::Encoder;
use crate::next::endpoints::publishers::Publishers;
use crate::next::error::{ErrKind, ProtocolError, ProtocolResult};
use crate::next::mem::MemoryProvider;
use crate::next::report::{NoContent, Reporter};
use crate::next::transport::traits::{Transport, TransportReceiver, TransportSender};
use crate::next::types::convert::{FromProtocolType, IntoNativeType};
use crate::next::types::core::ProtocolType;
use crate::next::types::limits::TypeLimits;
use crate::next::types::message::TypeMessageHeader;

struct ClientState<P: MemoryProvider, S, E> {
    publishers: Publishers<ProtocolResult<P::ReceiveSegment>>,
    sender: Mutex<S>,
    provider: P,
    reporter: E,
}

impl<P: MemoryProvider, S, E> ClientState<P, S, E> {
    #[inline(always)]
    fn new(sender: S, capacity: usize, reporter: E, provider: P) -> ClientState<P, S, E> {
        ClientState {
            publishers: Publishers::new(capacity),
            sender: Mutex::const_new(sender),
            provider,
            reporter,
        }
    }
}

pub struct CoreClient<T: Transport, P: MemoryProvider, E> {
    state: Arc<ClientState<P, T::Sender, E>>,
    recv_task: JoinHandle<()>,
}

impl<T, P, E> CoreClient<T, P, E>
where
    T: Transport,
    T::SendSegment: Send,
    T::ReceiveSegment: Send,
    T::Sender: Send + 'static,
    T::Receiver: Send + 'static,
    P: MemoryProvider<SendSegment = T::SendSegment> + Send + Sync,
    P: MemoryProvider<ReceiveSegment = T::ReceiveSegment> + Send + Sync + 'static,
    P::SendSegment: Encoder,
    P::ReceiveSegment: Decoder,
    E: Reporter + Send + Sync + 'static,
{
    pub async fn start(
        capacity: usize,
        transport: T,
        provider: P,
        reporter: E,
    ) -> ProtocolResult<CoreClient<T, P, E>> {
        let (sender, mut receiver) = transport.split();

        let state = Arc::new(ClientState::new(sender, capacity, reporter, provider));
        let client_state = Arc::clone(&state);

        let recv_task = tokio::spawn(async move {
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
        state: &Arc<ClientState<P, T::Sender, E>>,
    ) -> ProtocolResult<()> {
        let (id, _directive) = { TypeMessageHeader::decode_header(&mut message)? };
        // TODO: Directive matching according to directive-rules.
        state.publishers.publish(id, Ok(message));
        Ok(())
    }

    async fn send<'a, V, M, R>(
        &self,
        op: u64,
        message: &'a M,
    ) -> ProtocolResult<<R as IntoNativeType>::NativeType>
    where
        V: ProtocolType + TypeLimits<Limits = ()>,
        &'a M: Encode<V, P::SendSegment>,
        R: Decode<P::ReceiveSegment> + TypeLimits<Limits = ()> + IntoNativeType,
        <R as IntoNativeType>::NativeType:
            for<'de> FromProtocolType<<R as ProtocolType>::Type<'de>>,
    {
        // TODO: Generate ID according to id-rules.
        if let Some(publisher) = &self.state.publishers.acquire() {
            if let Some(mut segment) = self.state.provider.acquire_send() {
                TypeMessageHeader::encode_header(publisher.id, op, &mut segment)?;

                segment.encode_next(message, ())?;

                self.state.sender.lock().await.send(&mut segment).await?;

                // TODO: make it external.
                let result = tokio::time::timeout(Duration::from_secs(30), publisher.wait());

                // RT_ASSERT.
                match result.await?.unwrap() {
                    Ok(response) => {
                        let decoded = response.decode::<R>(())?;
                        return Ok(decoded.into_native());
                    }
                    Err(err) => return Err(err),
                }
            }
        }

        Err(ProtocolError::error(ErrKind::CapacityLimit))
    }

    async fn send_one_way<'a, V, M>(&self, op: u64, message: &'a M) -> ProtocolResult<()>
    where
        V: ProtocolType + TypeLimits<Limits = ()>,
        &'a M: Encode<V, P::SendSegment>,
    {
        let Some(mut segment) = self.state.provider.acquire_send() else {
            return Err(ProtocolError::error(ErrKind::CapacityLimit));
        };

        TypeMessageHeader::encode_header(0, op, &mut segment)?;
        segment.encode_next(message, ())?;
        self.state.sender.lock().await.send(&mut segment).await
    }

    async fn send_nullary<R>(&self, op: u64) -> ProtocolResult<<R as IntoNativeType>::NativeType>
    where
        R: Decode<P::ReceiveSegment> + TypeLimits<Limits = ()> + IntoNativeType,
        <R as IntoNativeType>::NativeType:
            for<'de> FromProtocolType<<R as ProtocolType>::Type<'de>>,
    {
        // TODO: Generate ID according to id-rules.
        if let Some(publisher) = &self.state.publishers.acquire() {
            if let Some(mut segment) = self.state.provider.acquire_send() {
                TypeMessageHeader::encode_header(publisher.id, op, &mut segment)?;

                self.state.sender.lock().await.send(&mut segment).await?;

                // TODO: make it external.
                let result = tokio::time::timeout(Duration::from_secs(30), publisher.wait());

                // RT_ASSERT.
                match result.await?.unwrap() {
                    Ok(response) => {
                        let decoded = response.decode::<R>(())?;
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
    use crate::next::mem::{IOPool, IOPoolSegment, IOSegment};
    use crate::next::transport::stream::uds::{UnixLink, UnixLinkServer};
    use crate::next::transport::traits::{TransportInitiator, TransportServer};
    use crate::next::types::core::TypeU64;
    use crate::next::types::message::TypeMessageHeader;

    #[tokio::test]
    async fn test_core_client_send_receive() {
        let path = "/tmp/test_core_client_send.sock";
        let _ = std::fs::remove_file(path);

        let capacity = 2;
        let pool = IOPool::new(3, 1024);

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

        let transport = UnixLink::connect(&path).await.unwrap();

        let client = CoreClient::start(capacity, transport, pool, ())
            .await
            .unwrap();

        // Send two-way.
        let response: u64 = client
            .send::<TypeU64, TypeU64, TypeU64>(10, &TypeU64(100))
            .await
            .unwrap();

        assert_eq!(response, 200);

        server_handle.await.unwrap();
        let _ = std::fs::remove_file(path);
    }
}
