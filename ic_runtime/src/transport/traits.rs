use crate::error::ProtocolResult;

/// The sender of the transport-component.
pub trait TransportSender {
    type SendSegment;

    fn send(
        &mut self,
        source: &mut Self::SendSegment,
    ) -> impl Future<Output = ProtocolResult<()>> + Send;
    fn terminate(&mut self) -> impl Future<Output = ProtocolResult<()>> + Send;
}

/// The receiver of the transport-component.
pub trait TransportReceiver {
    type ReceiveSegment;

    fn receive(
        &mut self,
        destination: &mut Self::ReceiveSegment,
    ) -> impl Future<Output = ProtocolResult<()>> + Send;
}

/// A type that can act as transport layer.
///
/// This trait is a continuity to the effort of isolating memory from the actual logic of transferring data.
///
/// Types that implement this trait shall ask for their memory from a specified allocator or `provider`.
pub trait Transport: Sized {
    /// TODO: Add associated error-type?

    type Parameters;

    type SendSegment;

    type Sender: TransportSender<SendSegment = Self::SendSegment>;

    type ReceiveSegment;

    type Receiver: TransportReceiver<ReceiveSegment = Self::ReceiveSegment>;

    fn connect(parameters: &Self::Parameters) -> impl Future<Output = ProtocolResult<Self>> + Send;

    fn send(
        &mut self,
        source: &mut Self::SendSegment,
    ) -> impl Future<Output = ProtocolResult<()>> + Send;

    fn receive(
        &mut self,
        destination: &mut Self::ReceiveSegment,
    ) -> impl Future<Output = ProtocolResult<()>> + Send;

    fn terminate(&mut self) -> impl Future<Output = ProtocolResult<()>> + Send;

    fn split(self) -> (Self::Sender, Self::Receiver);
}

/// A type that establishes a connection after being accepted by the transport-server.
pub trait TransportInitiator: Sized {
    type Transport: Transport;

    fn initiate(self) -> impl Future<Output = ProtocolResult<Self::Transport>> + Send;
}

/// A type that serves transport-components.
///
/// This trait can be implemented by transport-components that support multi-endpoint connections.
///
/// Types implementing this trait are used by multi-client server-implementations.
pub trait TransportServer: Sized {
    type Transport: Transport;

    type Initiator: TransportInitiator<Transport = Self::Transport>;

    type Parameter;

    type ID;

    fn create(parameter: &Self::Parameter) -> impl Future<Output = ProtocolResult<Self>>;

    fn accept(&self) -> impl Future<Output = ProtocolResult<(Self::Initiator, Self::ID)>> + Send;

    fn id(&self) -> ProtocolResult<Self::ID>;

    fn terminate(&mut self) -> impl Future<Output = ProtocolResult<()>> + Send;
}
