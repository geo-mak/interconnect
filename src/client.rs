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
    AsyncReceiver, AsyncSender, EncMessageReceiver, EncMessageSender, Message, MessageBuffer,
    MessageID, MessageReceiver, MessageSender, MessageType,
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

struct Oneshot<T> {
    state: AtomicU8,
    waker: UnsafeCell<Waker>,
    // TODO: Maybeuninit?
    value: UnsafeCell<Option<T>>,
    _pin: PhantomPinned,
}

const RCV_WAIT: u8 = 0;
const RCV_SET: u8 = 1;
const RCV_READY: u8 = 2;

impl<T> Oneshot<T> {
    #[inline(always)]
    const fn new() -> Self {
        Self {
            state: AtomicU8::new(RCV_WAIT),
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

struct OneshotSender<T> {
    stack_ptr: NonNull<Oneshot<T>>,
}

unsafe impl<T> Send for OneshotSender<T> {}
unsafe impl<T> Sync for OneshotSender<T> {}

impl<T> OneshotSender<T> {
    #[inline(always)]
    const fn new(oneshot: &Oneshot<T>) -> Self {
        Self {
            stack_ptr: NonNull::from_ref(oneshot),
        }
    }

    #[inline]
    fn send(mut self, value: T) {
        let oneshot = unsafe { self.stack_ptr.as_mut() };

        let value_ptr = oneshot.value.get();

        debug_assert!(unsafe { (*value_ptr).is_none() });

        unsafe { value_ptr.write(Some(value)) };

        if oneshot.state.fetch_or(RCV_READY, AcqRel) == RCV_WAIT {
            unsafe { (*oneshot.waker.get()).wake_by_ref() };
        }
    }
}

struct OneshotReceiver<'a, T> {
    oneshot: &'a Oneshot<T>,
}

impl<'a, T> OneshotReceiver<'a, T> {
    #[inline(always)]
    const fn new(oneshot: &'a Oneshot<T>) -> Self {
        Self { oneshot }
    }
}

impl<'a, T> Future for OneshotReceiver<'a, T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let oneshot = unsafe { self.get_unchecked_mut().oneshot };
        let state = &oneshot.state;

        match state.compare_exchange(RCV_WAIT, RCV_SET, Acquire, Acquire) {
            Ok(_) => {
                let waker_ptr = oneshot.waker.get();
                unsafe { *waker_ptr = cx.waker().clone() };

                match state.compare_exchange(RCV_SET, RCV_WAIT, AcqRel, Acquire) {
                    Ok(_) => return Poll::Pending,
                    Err(current) => debug_assert!(current & RCV_READY != 0),
                };
            }
            Err(current) => debug_assert!(current & RCV_READY != 0),
        }

        // Safety: RCV_READY indicates that writing has been completed.
        Poll::Ready(())
    }
}

struct OnOneshotDrop<'a, I, T>
where
    I: Hash + Eq,
{
    id: &'a I,
    entries: &'a PendingStore<I, T>,
}

impl<'a, I, T> OnOneshotDrop<'a, I, T>
where
    I: Hash + Eq,
{
    #[inline(always)]
    const fn new(id: &'a I, entries: &'a PendingStore<I, T>) -> Self {
        Self { id, entries }
    }

    #[inline(always)]
    const fn do_nothing(self) {
        mem::forget(self);
    }
}

impl<'a, I, T> Drop for OnOneshotDrop<'a, I, T>
where
    I: Hash + Eq,
{
    fn drop(&mut self) {
        self.entries.remove(self.id);
    }
}

struct PendingStore<I, T> {
    entries: parking_lot::Mutex<HashMap<I, OneshotSender<T>>>,
}

impl<I, T> PendingStore<I, T>
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
    fn store(&self, id: I, sender: OneshotSender<T>) {
        self.entries.lock().insert(id, sender);
    }

    #[inline(always)]
    fn remove(&self, id: &I) {
        self.entries.lock().remove(id);
    }

    #[inline]
    fn send_back(&self, id: &I, value: T) {
        // Safety: Locking is required to prevent receiver from dropping the state while in use.
        let mut map_lock = self.entries.lock();
        if let Some(sender) = map_lock.remove(id) {
            sender.send(value)
        }
    }
}

enum Response {
    Pong,
    Reply(MessageBuffer),
}

struct ClientState<S, H, E> {
    abort_lock: DynamicLatch,
    pending: PendingStore<MessageID, RpcResult<Response>>,
    sender: Mutex<S>,
    service: H,
    reporter: E,
}

impl<S, H, E> ClientState<S, H, E> {
    #[inline(always)]
    fn new(sender: S, capacity: usize, reporter: E, service: H) -> ClientState<S, H, E> {
        ClientState {
            abort_lock: DynamicLatch::new(),
            pending: PendingStore::new(capacity),
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
        match header.kind {
            MessageType::Reply => {
                let data = Message::reply_data(message);
                let mut reply = MessageBuffer::with_capacity(data.len());
                unsafe { reply.copy_from(data) };
                state
                    .pending
                    .send_back(&header.id, Ok(Response::Reply(reply)));
            }
            MessageType::Error => {
                let err = Message::decode_error(message)?;
                state.pending.send_back(&header.id, Err(err))
            }
            MessageType::Pong => state.pending.send_back(&header.id, Ok(Response::Pong)),
            MessageType::Call => {
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
            MessageType::NullaryCall => {
                if let Some(_lock) = state.abort_lock.acquire() {
                    let method = Message::decode_method(message)?;
                    let mut context = ClientContext::new(&header.id, state);
                    return state.service.call_nullary(method, &mut context).await;
                }
            }
            MessageType::Ping => return state.sender.lock().await.pong(&header.id).await,
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
        let pinned_oneshot = Oneshot::new();

        let entries = &self.state.pending;

        let id = MessageID::new_v4();

        entries.store(id, OneshotSender::new(&pinned_oneshot));

        let on_drop = OnOneshotDrop::new(&id, entries);

        self.state
            .sender
            .lock()
            .await
            .call(&id, method, params)
            .await?;

        match tokio::time::timeout(timeout, OneshotReceiver::new(&pinned_oneshot)).await {
            Ok(_) => {
                on_drop.do_nothing();
                // RT_ASSERT
                let result = pinned_oneshot.take_value().unwrap();
                match result {
                    Ok(response) => {
                        if let Response::Reply(reply) = response {
                            return Message::decode_from_slice(&reply.data);
                        }
                        Err(RpcError::error(ErrKind::UnexpectedMsg))
                    }
                    Err(err) => Err(err),
                }
            }
            Err(_) => Err(RpcError::error(ErrKind::Timeout)),
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
        let pinned_oneshot = Oneshot::new();

        let entries = &self.state.pending;

        let id = MessageID::new_v4();

        entries.store(id, OneshotSender::new(&pinned_oneshot));

        let on_drop = OnOneshotDrop::new(&id, entries);

        self.state
            .sender
            .lock()
            .await
            .call_nullary(&id, method)
            .await?;

        match tokio::time::timeout(timeout, OneshotReceiver::new(&pinned_oneshot)).await {
            Ok(_) => {
                on_drop.do_nothing();
                // RT_ASSERT
                let result = pinned_oneshot.take_value().unwrap();
                match result {
                    Ok(response) => {
                        if let Response::Reply(reply) = response {
                            return Message::decode_from_slice(&reply.data);
                        }
                        Err(RpcError::error(ErrKind::UnexpectedMsg))
                    }
                    Err(err) => Err(err),
                }
            }
            Err(_) => Err(RpcError::error(ErrKind::Timeout)),
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
        let pinned_oneshot = Oneshot::new();

        let entries = &self.state.pending;

        let id = MessageID::new_v4();

        entries.store(id, OneshotSender::new(&pinned_oneshot));

        let on_drop = OnOneshotDrop::new(&id, entries);

        self.state.sender.lock().await.ping(&id).await?;

        match tokio::time::timeout(timeout, OneshotReceiver::new(&pinned_oneshot)).await {
            Ok(_) => {
                on_drop.do_nothing();
                Ok(())
            }
            Err(_) => Err(RpcError::error(ErrKind::Timeout)),
        }
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
                        match header.kind {
                            MessageType::Call => {
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
                            MessageType::NullaryCall => {
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
                    match header.kind {
                        MessageType::Call => {
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
