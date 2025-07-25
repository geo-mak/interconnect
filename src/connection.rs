use std::net::SocketAddr;
use std::path::Path;

use tokio::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::ToSocketAddrs;
use tokio::net::tcp;
use tokio::net::unix;
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};

pub trait OwnedSplitStream {
    type OwnedReadHalf: AsyncReadExt + Send + Sync + Unpin;
    type OwnedWriteHalf: AsyncWriteExt + Send + Sync + Unpin;
    fn owned_split(self) -> (Self::OwnedReadHalf, Self::OwnedWriteHalf);
}

impl OwnedSplitStream for TcpStream {
    type OwnedReadHalf = tcp::OwnedReadHalf;
    type OwnedWriteHalf = tcp::OwnedWriteHalf;

    #[inline(always)]
    fn owned_split(self) -> (Self::OwnedReadHalf, Self::OwnedWriteHalf) {
        TcpStream::into_split(self)
    }
}

impl OwnedSplitStream for UnixStream {
    type OwnedReadHalf = unix::OwnedReadHalf;
    type OwnedWriteHalf = unix::OwnedWriteHalf;

    #[inline(always)]
    fn owned_split(self) -> (Self::OwnedReadHalf, Self::OwnedWriteHalf) {
        UnixStream::into_split(self)
    }
}

pub trait RpcListener<A>: Sized + Send {
    type Address;
    type Stream: OwnedSplitStream + Send;

    fn bind(addr: A) -> impl Future<Output = io::Result<Self>>;

    fn accept(&self) -> impl Future<Output = io::Result<(Self::Stream, Self::Address)>> + Send;

    fn local_addr(&self) -> io::Result<Self::Address>;
}

impl<A: ToSocketAddrs> RpcListener<A> for TcpListener {
    type Address = SocketAddr;
    type Stream = TcpStream;

    #[inline(always)]
    async fn bind(addr: A) -> io::Result<Self> {
        TcpListener::bind(addr).await
    }

    #[inline(always)]
    async fn accept(&self) -> io::Result<(Self::Stream, SocketAddr)> {
        self.accept().await
    }

    #[inline(always)]
    fn local_addr(&self) -> io::Result<Self::Address> {
        TcpListener::local_addr(self)
    }
}

impl<A: AsRef<Path>> RpcListener<A> for UnixListener {
    type Address = unix::SocketAddr;
    type Stream = UnixStream;

    #[inline(always)]
    async fn bind(addr: A) -> io::Result<Self> {
        UnixListener::bind(addr)
    }

    #[inline(always)]
    async fn accept(&self) -> io::Result<(UnixStream, Self::Address)> {
        self.accept().await
    }

    #[inline(always)]
    fn local_addr(&self) -> io::Result<Self::Address> {
        UnixListener::local_addr(self)
    }
}
