use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomPinned;
use std::mem;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use serde::{Deserialize, Serialize};

use crate::capability::{RpcCapability, negotiation};
use crate::core::{
    AsyncReceiver, AsyncSender, Directive, EncMessageReceiver, EncMessageSender, Message,
    MessageBuffer, MessageID, MessageReceiver, MessageSender,
};
use crate::error::{ErrKind, RpcError, RpcResult};
use crate::report::Reporter;
use crate::service::{Call, CallContext, RpcService};
use crate::sync::{DynamicLatch, NOOP_WAKER};
use crate::transport::TransportLayer;

/// The common RPC client interface of async clients.
pub trait AsyncRpcClient {
    fn call<P, R>(&self, method: u16, params: &P) -> impl Future<Output = RpcResult<R>>
    where
        P: Serialize + Sync,
        R: for<'de> Deserialize<'de>;

    fn call_timeout<P, R>(
        &self,
        method: u16,
        params: &P,
        timeout: Duration,
    ) -> impl Future<Output = RpcResult<R>>
    where
        P: Serialize + Sync,
        R: for<'de> Deserialize<'de>;

    fn call_one_way<P>(&self, method: u16, params: &P) -> impl Future<Output = RpcResult<()>>
    where
        P: Serialize + Sync;

    fn call_nullary<R>(&self, method: u16) -> impl Future<Output = RpcResult<R>>
    where
        R: for<'de> Deserialize<'de>;

    fn call_nullary_timeout<R>(
        &self,
        method: u16,
        timeout: Duration,
    ) -> impl Future<Output = RpcResult<R>>
    where
        R: for<'de> Deserialize<'de>;

    fn call_nullary_one_way(&self, method: u16) -> impl Future<Output = RpcResult<()>>;

    fn ping(&self, timeout: Duration) -> impl Future<Output = RpcResult<()>>;

    fn shutdown(&mut self) -> impl Future<Output = RpcResult<()>>;
}

struct UnicastDataBus<T> {
    state: AtomicU8,
    waker: UnsafeCell<Waker>,
    // TODO: Maybeuninit?
    value: UnsafeCell<Option<T>>,
    _pin: PhantomPinned,
}

const BUS_WAIT: u8 = 0;
const BUS_INIT: u8 = 1;
const BUS_READY: u8 = 2;

impl<T> UnicastDataBus<T> {
    #[inline(always)]
    const fn new() -> Self {
        Self {
            state: AtomicU8::new(BUS_WAIT),
            waker: UnsafeCell::new(NOOP_WAKER),
            value: UnsafeCell::new(None),
            _pin: PhantomPinned,
        }
    }

    #[inline(always)]
    const fn take_value(&self) -> Option<T> {
        let value_ptr = self.value.get();
        unsafe { (*value_ptr).take() }
    }
}

struct UnicastPublisher<T> {
    bus_ptr: NonNull<UnicastDataBus<T>>,
}

unsafe impl<T> Send for UnicastPublisher<T> {}
unsafe impl<T> Sync for UnicastPublisher<T> {}

impl<T> UnicastPublisher<T> {
    #[inline(always)]
    const fn new(bus: &UnicastDataBus<T>) -> Self {
        Self {
            bus_ptr: NonNull::from_ref(bus),
        }
    }

    #[inline]
    fn publish(mut self, value: T) {
        let bus = unsafe { self.bus_ptr.as_mut() };

        let value_ptr = bus.value.get();

        debug_assert!(unsafe { (*value_ptr).is_none() });

        unsafe { value_ptr.write(Some(value)) };

        if bus.state.fetch_or(BUS_READY, AcqRel) == BUS_WAIT {
            unsafe { (*bus.waker.get()).wake_by_ref() };
        }
    }
}

struct WaitPublishing<'a, T> {
    bus: &'a UnicastDataBus<T>,
}

impl<'a, T> WaitPublishing<'a, T> {
    #[inline(always)]
    const fn new(bus: &'a UnicastDataBus<T>) -> Self {
        Self { bus }
    }
}

impl<'a, T> Future for WaitPublishing<'a, T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let bus = unsafe { self.get_unchecked_mut().bus };
        let state = &bus.state;

        match state.compare_exchange(BUS_WAIT, BUS_INIT, Acquire, Acquire) {
            Ok(_) => {
                let waker_ptr = bus.waker.get();
                unsafe { *waker_ptr = cx.waker().clone() };

                match state.compare_exchange(BUS_INIT, BUS_WAIT, AcqRel, Acquire) {
                    Ok(_) => return Poll::Pending,
                    Err(current) => debug_assert!(current & BUS_READY != 0),
                };
            }
            Err(current) => debug_assert!(current & BUS_READY != 0),
        }

        // Safety: BUS_READY indicates that writing has been completed.
        Poll::Ready(())
    }
}

struct OnBusDrop<'a, I, T>
where
    I: Hash + Eq,
{
    id: &'a I,
    entries: &'a ReservationStation<I, T>,
}

impl<'a, I, T> OnBusDrop<'a, I, T>
where
    I: Hash + Eq,
{
    #[inline(always)]
    const fn new(id: &'a I, entries: &'a ReservationStation<I, T>) -> Self {
        Self { id, entries }
    }

    #[inline(always)]
    const fn do_nothing(self) {
        mem::forget(self);
    }
}

impl<'a, I, T> Drop for OnBusDrop<'a, I, T>
where
    I: Hash + Eq,
{
    fn drop(&mut self) {
        self.entries.remove(self.id);
    }
}

struct ReservationStation<I, T> {
    entries: parking_lot::Mutex<HashMap<I, UnicastPublisher<T>>>,
}

impl<I, T> ReservationStation<I, T>
where
    I: Hash + Eq,
{
    #[inline(always)]
    fn new(capacity: usize) -> Self {
        Self {
            entries: parking_lot::Mutex::new(HashMap::with_capacity(capacity)),
        }
    }

    #[inline(always)]
    fn store(&self, id: I, sender: UnicastPublisher<T>) {
        self.entries.lock().insert(id, sender);
    }

    #[inline(always)]
    fn remove(&self, id: &I) {
        self.entries.lock().remove(id);
    }

    #[inline]
    fn publish(&self, id: &I, value: T) {
        // Safety: Locking is required to prevent cancellation from dropping the bus while it is in use.
        let mut map_lock = self.entries.lock();
        if let Some(publisher) = map_lock.remove(id) {
            publisher.publish(value)
        }
    }
}

enum Response {
    Pong,
    Data(MessageBuffer),
}

struct ClientState<S, H, E> {
    abort_lock: DynamicLatch,
    rs: ReservationStation<MessageID, RpcResult<Response>>,
    sender: Mutex<S>,
    service: H,
    reporter: E,
}

impl<S, H, E> ClientState<S, H, E> {
    #[inline(always)]
    fn new(sender: S, capacity: usize, reporter: E, service: H) -> ClientState<S, H, E> {
        ClientState {
            abort_lock: DynamicLatch::new(),
            rs: ReservationStation::new(capacity),
            sender: Mutex::const_new(sender),
            service,
            reporter,
        }
    }
}

struct ClientContext<'a, S, H, E> {
    id: &'a MessageID,
    state: &'a ClientState<S, H, E>,
}

impl<'a, S, H, E> ClientContext<'a, S, H, E>
where
    S: AsyncSender + Send,
{
    #[inline(always)]
    const fn new(id: &'a MessageID, sender: &'a ClientState<S, H, E>) -> Self {
        Self { id, state: sender }
    }
}

impl<'a, S, H, E> CallContext for ClientContext<'a, S, H, E>
where
    S: AsyncSender + Send,
    H: RpcService + Sync,
    E: Reporter + Sync,
{
    type ID = MessageID;

    #[inline(always)]
    fn id(&self) -> &Self::ID {
        self.id
    }

    #[inline]
    async fn send_reply<R: Serialize + Sync>(&mut self, reply: &R) -> RpcResult<()> {
        self.state.sender.lock().await.reply(self.id, reply).await
    }

    #[inline]
    async fn send_error(&mut self, err: RpcError) -> RpcResult<()> {
        self.state.sender.lock().await.error(self.id, err).await
    }

    #[inline]
    async fn call<P: Serialize + Sync>(&mut self, method: u16, params: &P) -> RpcResult<()> {
        self.state
            .sender
            .lock()
            .await
            .call(self.id, method, &params)
            .await
    }

    #[inline]
    async fn call_nullary(&mut self, method: u16) -> RpcResult<()> {
        self.state
            .sender
            .lock()
            .await
            .call_nullary(self.id, method)
            .await
    }
}

/// An async RPC client implementation.
/// This implementation utilizes single shared transport instance,
/// which makes it very lightweight at the cost of some synchronization overhead.
pub struct RpcAsyncClient<S, H, E> {
    state: Arc<ClientState<S, H, E>>,
    recv_task: JoinHandle<()>,
}

// Core private implementation.
impl<S, H, E> RpcAsyncClient<S, H, E>
where
    S: AsyncSender + Send + 'static,
    H: RpcService + Send + Sync + 'static,
    E: Reporter + Send + Sync + 'static,
{
    fn init<R>(
        capacity: usize,
        sender: S,
        mut receiver: R,
        reporter: E,
        handler: H,
    ) -> RpcAsyncClient<S, H, E>
    where
        S: AsyncSender + Send + 'static,
        R: AsyncReceiver + Send + Sync + 'static,
    {
        let state = Arc::new(ClientState::new(sender, capacity, reporter, handler));
        let client_state = Arc::clone(&state);

        let recv_task = tokio::spawn(async move {
            loop {
                match receiver.receive().await {
                    Ok(_) => {
                        if let Err(err) = Self::process_message(&receiver, &client_state).await {
                            client_state.reporter.error("Handling error", &err);
                            break;
                        }
                    }
                    Err(err) => {
                        client_state.reporter.error("Receiving error", &err);
                        break;
                    }
                }
            }
        });

        Self { state, recv_task }
    }

    async fn process_message<R>(receiver: &R, state: &Arc<ClientState<S, H, E>>) -> RpcResult<()>
    where
        R: AsyncReceiver,
    {
        let message = receiver.message();
        let header = Message::decode_header(message)?;
        match header.directive {
            Directive::Return => {
                let data = Message::returned_data(message);
                let mut ret = MessageBuffer::with_capacity(data.len());
                unsafe { ret.copy_from(data) };
                state.rs.publish(&header.id, Ok(Response::Data(ret)));
            }
            Directive::Error => {
                let err = Message::decode_error(message)?;
                state.rs.publish(&header.id, Err(err))
            }
            Directive::Pong => state.rs.publish(&header.id, Ok(Response::Pong)),
            Directive::Call => {
                if let Some(_lock) = state.abort_lock.acquire() {
                    let method = Message::decode_method(message)?;
                    let params = Message::param_data(message);
                    let mut context = ClientContext::new(&header.id, state);
                    return state
                        .service
                        .call(Call { method, params }, &mut context)
                        .await;
                }
            }
            Directive::NullaryCall => {
                if let Some(_lock) = state.abort_lock.acquire() {
                    let method = Message::decode_method(message)?;
                    let mut context = ClientContext::new(&header.id, state);
                    return state.service.call_nullary(method, &mut context).await;
                }
            }
            Directive::Ping => return state.sender.lock().await.pong(&header.id).await,
        }
        Ok(())
    }
}

impl<T, H, E> RpcAsyncClient<MessageSender<T>, H, E>
where
    T: TransportLayer + 'static,
    H: RpcService + Send + Sync + 'static,
    E: Reporter + Send + Sync + 'static,
{
    pub async fn connect(
        capacity: usize,
        mut transport: T,
        reporter: E,
        handler: H,
    ) -> RpcResult<RpcAsyncClient<MessageSender<T::OwnedWriteHalf>, H, E>> {
        negotiation::initiate(&mut transport, RpcCapability::new(1, false)).await?;

        let (r, w) = transport.into_split();

        let instance = RpcAsyncClient::init(
            capacity,
            MessageSender::new(w),
            MessageReceiver::new(r),
            reporter,
            handler,
        );

        Ok(instance)
    }
}

impl<T, H, E> RpcAsyncClient<EncMessageSender<T>, H, E>
where
    T: TransportLayer + 'static,
    H: RpcService + Send + Sync + 'static,
    E: Reporter + Send + Sync + 'static,
{
    pub async fn connect_encrypted(
        capacity: usize,
        mut transport: T,
        reporter: E,
        handler: H,
    ) -> RpcResult<RpcAsyncClient<EncMessageSender<T::OwnedWriteHalf>, H, E>> {
        negotiation::initiate(&mut transport, RpcCapability::new(1, true)).await?;

        let (r_key, w_key) = negotiation::initiate_key_exchange(&mut transport).await?;

        let (r, w) = transport.into_split();

        let instance = RpcAsyncClient::init(
            capacity,
            EncMessageSender::new(w, w_key),
            EncMessageReceiver::new(r, r_key),
            reporter,
            handler,
        );

        Ok(instance)
    }
}

impl<S, H, E> AsyncRpcClient for RpcAsyncClient<S, H, E>
where
    S: AsyncSender + Send + 'static,
    H: RpcService + Send + Sync + 'static,
    E: Reporter + Send + Sync + 'static,
{
    /// Makes a remote procedure call.
    /// Default timeout is `30` seconds.
    async fn call<P, R>(&self, method: u16, params: &P) -> RpcResult<R>
    where
        P: Serialize + Sync,
        R: for<'de> Deserialize<'de>,
    {
        self.call_timeout(method, params, Duration::from_secs(30))
            .await
    }

    /// Makes a remote procedure call with custom timeout.
    async fn call_timeout<P, R>(&self, method: u16, params: &P, timeout: Duration) -> RpcResult<R>
    where
        P: Serialize + Sync,
        R: for<'de> Deserialize<'de>,
    {
        // Safety: This value must not move.
        let pinned_bus = UnicastDataBus::new();

        let entries = &self.state.rs;

        let id = MessageID::new_v4();

        entries.store(id, UnicastPublisher::new(&pinned_bus));

        let on_drop = OnBusDrop::new(&id, entries);

        self.state
            .sender
            .lock()
            .await
            .call(&id, method, params)
            .await?;

        tokio::time::timeout(timeout, WaitPublishing::new(&pinned_bus)).await?;

        on_drop.do_nothing();

        // RT_ASSERT
        let published = pinned_bus.take_value().unwrap();

        match published {
            Ok(response) => {
                if let Response::Data(reply) = response {
                    return Message::decode_from_slice(&reply.data);
                }
                Err(RpcError::error(ErrKind::UnexpectedMsg))
            }
            Err(err) => Err(err),
        }
    }

    /// Sends a one-way call without response.
    ///
    /// This call is untracked, if the target method returns response,
    /// the response will be discarded.
    async fn call_one_way<P>(&self, method: u16, params: &P) -> RpcResult<()>
    where
        P: Serialize + Sync,
    {
        let id = MessageID::new_v4();
        self.state
            .sender
            .lock()
            .await
            .call(&id, method, params)
            .await
    }

    /// Makes a remote procedure call.
    /// Default timeout is `30` seconds.
    async fn call_nullary<R>(&self, method: u16) -> RpcResult<R>
    where
        R: for<'de> Deserialize<'de>,
    {
        self.call_nullary_timeout(method, Duration::from_secs(30))
            .await
    }

    /// Makes a remote procedure call with custom timeout.
    async fn call_nullary_timeout<R>(&self, method: u16, timeout: Duration) -> RpcResult<R>
    where
        R: for<'de> Deserialize<'de>,
    {
        // Safety: This value must not move.
        let pinned_bus = UnicastDataBus::new();

        let entries = &self.state.rs;

        let id = MessageID::new_v4();

        entries.store(id, UnicastPublisher::new(&pinned_bus));

        let on_drop = OnBusDrop::new(&id, entries);

        self.state
            .sender
            .lock()
            .await
            .call_nullary(&id, method)
            .await?;

        tokio::time::timeout(timeout, WaitPublishing::new(&pinned_bus)).await?;

        on_drop.do_nothing();

        // RT_ASSERT
        let published = pinned_bus.take_value().unwrap();

        match published {
            Ok(response) => {
                if let Response::Data(reply) = response {
                    return Message::decode_from_slice(&reply.data);
                }
                Err(RpcError::error(ErrKind::UnexpectedMsg))
            }
            Err(err) => Err(err),
        }
    }

    /// Sends a one-way nullary call without response.
    ///
    /// This call is untracked, if the target method returns response,
    /// the response will be discarded.
    async fn call_nullary_one_way(&self, method: u16) -> RpcResult<()> {
        let id = MessageID::new_v4();
        self.state
            .sender
            .lock()
            .await
            .call_nullary(&id, method)
            .await
    }

    /// Sends a `ping`` message.
    async fn ping(&self, timeout: Duration) -> RpcResult<()> {
        // Safety: This value must not move.
        let pinned_bus = UnicastDataBus::new();

        let entries = &self.state.rs;

        let id = MessageID::new_v4();

        entries.store(id, UnicastPublisher::new(&pinned_bus));

        let on_drop = OnBusDrop::new(&id, entries);

        self.state.sender.lock().await.ping(&id).await?;

        tokio::time::timeout(timeout, WaitPublishing::new(&pinned_bus)).await?;

        on_drop.do_nothing();
        Ok(())
    }

    /// Closes its sender and shutdowns the receiving task in graceful manner.
    ///
    /// This call doesn't have immediate effect and may take longer time,
    /// because it allows critical regions to fully complete their execution.
    ///
    /// Buffered data will be sent followed by FIN message.
    ///
    /// Any attempts to send messages after this call will return `Broken pipe` I/O error.
    async fn shutdown(&mut self) -> RpcResult<()> {
        self.state.abort_lock.open();

        self.state.abort_lock.wait().await;

        self.recv_task.abort();

        self.state.sender.lock().await.close().await?;

        self.state.service.shutdown().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::net::TcpStream;

    use crate::report::STDIOReporter;

    #[tokio::test]
    async fn test_client_calls() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let (mut transport, _) = listener.accept().await.unwrap();

            negotiation::read_frame(&mut transport)
                .await
                .expect("server negotiation failed");

            negotiation::confirm(&mut transport)
                .await
                .expect("Failed to send confirmation");

            let (r, w) = transport.into_split();

            let mut msg_sender = MessageSender::new(w);
            let mut msg_receiver = MessageReceiver::new(r);

            loop {
                match msg_receiver.receive().await {
                    Ok(_) => {
                        let header = Message::decode_header(msg_receiver.message()).unwrap();
                        match header.directive {
                            Directive::Call => {
                                let method =
                                    Message::decode_method(msg_receiver.message()).unwrap();
                                match method {
                                    1 => {
                                        let params: String =
                                            Message::decode_params(msg_receiver.message()).unwrap();
                                        assert_eq!(params, "call");

                                        msg_sender.reply(&header.id, &"reply").await.unwrap();
                                    }
                                    2 => {
                                        msg_sender
                                            .error(
                                                &header.id,
                                                RpcError::error(ErrKind::Unimplemented),
                                            )
                                            .await
                                            .unwrap();
                                    }
                                    _ => panic!("undefined method"),
                                }
                            }
                            Directive::NullaryCall => {
                                let method =
                                    Message::decode_method(msg_receiver.message()).unwrap();
                                assert_eq!(method, 1);

                                msg_sender
                                    .reply(&header.id, &"nullary call reply")
                                    .await
                                    .unwrap();
                            }
                            _ => panic!("Expected call"),
                        }
                    }
                    Err(e) => {
                        println!("Server error: {e}");
                        break;
                    }
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let transport = TcpStream::connect(addr).await.unwrap();
        let mut client = RpcAsyncClient::connect(1, transport, STDIOReporter::new(), ())
            .await
            .unwrap();

        let reply: String = client.call(1, &"call").await.unwrap();
        assert_eq!(reply, "reply");

        let reply_nullary: String = client.call_nullary(1).await.unwrap();
        assert_eq!(reply_nullary, "nullary call reply");

        let err: RpcError = client.call::<&str, String>(2, &"call").await.unwrap_err();
        assert!(err.kind == ErrKind::Unimplemented);

        client.shutdown().await.unwrap();
        server_task.await.unwrap()
    }

    #[tokio::test]
    async fn test_client_encrypted_call_reply() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let (mut transport, _) = listener.accept().await.unwrap();

            let proposed = negotiation::read_frame(&mut transport)
                .await
                .expect("server negotiation failed");

            assert!(proposed.encryption);

            negotiation::confirm(&mut transport)
                .await
                .expect("Failed to send confirmation");

            assert!(proposed.encryption);

            let (r_key, w_key) = negotiation::accept_key_exchange(&mut transport)
                .await
                .expect("Server encryption setup failed");

            let (r, w) = transport.into_split();

            let mut msg_sender = EncMessageSender::new(w, w_key);
            let mut msg_receiver = EncMessageReceiver::new(r, r_key);

            match msg_receiver.receive().await {
                Ok(_) => {
                    let header = Message::decode_header(msg_receiver.message()).unwrap();
                    match header.directive {
                        Directive::Call => {
                            let method = Message::decode_method(msg_receiver.message()).unwrap();
                            assert_eq!(method, 1);
                            let params: String =
                                Message::decode_params(msg_receiver.message()).unwrap();
                            assert_eq!(params, "call");

                            msg_sender.reply(&header.id, &"reply").await.unwrap();
                        }
                        _ => panic!("Expected call message"),
                    }
                }
                Err(e) => {
                    println!("Server error: {e}");
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let transport = TcpStream::connect(addr).await.unwrap();
        let mut client = RpcAsyncClient::connect_encrypted(1, transport, STDIOReporter::new(), ())
            .await
            .unwrap();

        let reply = client.call::<&str, String>(1, &"call").await.unwrap();
        assert_eq!(reply, "reply");

        server_task.await.unwrap();
        client.shutdown().await.unwrap();
    }
}
