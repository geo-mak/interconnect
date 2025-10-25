use std::io;
use std::net::SocketAddr;
use std::path::Path;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::ToSocketAddrs;
use tokio::net::tcp;
use tokio::net::unix;
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};

use crate::io::{AsyncIORead, AsyncIOWrite};

pub trait TransportListener<A>: Sized + Send {
    type Address;
    type Transport: TransportLayer + Send;

    fn bind(addr: A) -> impl Future<Output = io::Result<Self>>;

    fn accept(&self) -> impl Future<Output = io::Result<(Self::Transport, Self::Address)>> + Send;

    fn local_addr(&self) -> io::Result<Self::Address>;
}

impl<A: ToSocketAddrs> TransportListener<A> for TcpListener {
    type Address = SocketAddr;
    type Transport = TcpStream;

    #[inline(always)]
    async fn bind(addr: A) -> io::Result<Self> {
        TcpListener::bind(addr).await
    }

    #[inline(always)]
    async fn accept(&self) -> io::Result<(Self::Transport, SocketAddr)> {
        TcpListener::accept(self).await
    }

    #[inline(always)]
    fn local_addr(&self) -> io::Result<Self::Address> {
        TcpListener::local_addr(self)
    }
}

impl<A: AsRef<Path>> TransportListener<A> for UnixListener {
    type Address = unix::SocketAddr;
    type Transport = UnixStream;

    #[inline(always)]
    async fn bind(addr: A) -> io::Result<Self> {
        UnixListener::bind(addr)
    }

    #[inline(always)]
    async fn accept(&self) -> io::Result<(UnixStream, Self::Address)> {
        UnixListener::accept(self).await
    }

    #[inline(always)]
    fn local_addr(&self) -> io::Result<Self::Address> {
        UnixListener::local_addr(self)
    }
}

/// Trait represents types that can act as transport layers.
///
/// Types that implement this trait should comply with the following requirements:
///
/// - They provide two modes of operation: single-mode and split-mode.
///
/// - In split-mode, the two halves must be owned handles, that allow **parallel** access.
pub trait TransportLayer: AsyncIORead + AsyncIOWrite + Send + Unpin {
    type OwnedReadHalf: AsyncIORead + Send + Sync + Unpin;
    type OwnedWriteHalf: AsyncIOWrite + Send + Sync + Unpin;
    fn into_split(self) -> (Self::OwnedReadHalf, Self::OwnedWriteHalf);
}

impl AsyncIORead for TcpStream {
    #[inline(always)]
    async fn read<'a>(&'a mut self, output: &'a mut [u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncReadExt::read(self, output).await
    }

    #[inline(always)]
    async fn read_exact<'a>(&'a mut self, output: &'a mut [u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncReadExt::read_exact(self, output).await
    }
}

impl AsyncIOWrite for TcpStream {
    #[inline(always)]
    async fn write<'a>(&'a mut self, input: &'a [u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncWriteExt::write(self, input).await
    }

    #[inline(always)]
    async fn write_all<'a>(&'a mut self, input: &'a [u8]) -> std::io::Result<()>
    where
        Self: Unpin,
    {
        AsyncWriteExt::write_all(self, input).await
    }

    #[inline(always)]
    async fn shutdown(&mut self) -> std::io::Result<()>
    where
        Self: Unpin,
    {
        AsyncWriteExt::shutdown(self).await
    }
}

impl AsyncIORead for tcp::OwnedReadHalf {
    #[inline(always)]
    async fn read<'a>(&'a mut self, output: &'a mut [u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncReadExt::read(self, output).await
    }

    #[inline(always)]
    async fn read_exact<'a>(&'a mut self, output: &'a mut [u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncReadExt::read_exact(self, output).await
    }
}

impl AsyncIOWrite for tcp::OwnedWriteHalf {
    #[inline(always)]
    async fn write<'a>(&'a mut self, input: &'a [u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncWriteExt::write(self, input).await
    }

    #[inline(always)]
    async fn write_all<'a>(&'a mut self, input: &'a [u8]) -> std::io::Result<()>
    where
        Self: Unpin,
    {
        AsyncWriteExt::write_all(self, input).await
    }

    #[inline(always)]
    async fn shutdown(&mut self) -> std::io::Result<()>
    where
        Self: Unpin,
    {
        AsyncWriteExt::shutdown(self).await
    }
}

impl AsyncIORead for UnixStream {
    #[inline(always)]
    async fn read<'a>(&'a mut self, output: &'a mut [u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncReadExt::read(self, output).await
    }

    #[inline(always)]
    async fn read_exact<'a>(&'a mut self, output: &'a mut [u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncReadExt::read_exact(self, output).await
    }
}

impl AsyncIOWrite for UnixStream {
    #[inline(always)]
    async fn write<'a>(&'a mut self, input: &'a [u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncWriteExt::write(self, input).await
    }

    #[inline(always)]
    async fn write_all<'a>(&'a mut self, input: &'a [u8]) -> std::io::Result<()>
    where
        Self: Unpin,
    {
        AsyncWriteExt::write_all(self, input).await
    }

    #[inline(always)]
    async fn shutdown(&mut self) -> std::io::Result<()>
    where
        Self: Unpin,
    {
        AsyncWriteExt::shutdown(self).await
    }
}

impl AsyncIORead for unix::OwnedReadHalf {
    #[inline(always)]
    async fn read<'a>(&'a mut self, output: &'a mut [u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncReadExt::read(self, output).await
    }

    #[inline(always)]
    async fn read_exact<'a>(&'a mut self, output: &'a mut [u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncReadExt::read_exact(self, output).await
    }
}

impl AsyncIOWrite for unix::OwnedWriteHalf {
    #[inline(always)]
    async fn write<'a>(&'a mut self, input: &'a [u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncWriteExt::write(self, input).await
    }

    #[inline(always)]
    async fn write_all<'a>(&'a mut self, input: &'a [u8]) -> std::io::Result<()>
    where
        Self: Unpin,
    {
        AsyncWriteExt::write_all(self, input).await
    }

    #[inline(always)]
    async fn shutdown(&mut self) -> std::io::Result<()>
    where
        Self: Unpin,
    {
        AsyncWriteExt::shutdown(self).await
    }
}

impl TransportLayer for TcpStream {
    type OwnedReadHalf = tcp::OwnedReadHalf;
    type OwnedWriteHalf = tcp::OwnedWriteHalf;

    #[inline(always)]
    fn into_split(self) -> (Self::OwnedReadHalf, Self::OwnedWriteHalf) {
        TcpStream::into_split(self)
    }
}

impl TransportLayer for UnixStream {
    type OwnedReadHalf = unix::OwnedReadHalf;
    type OwnedWriteHalf = unix::OwnedWriteHalf;

    #[inline(always)]
    fn into_split(self) -> (Self::OwnedReadHalf, Self::OwnedWriteHalf) {
        UnixStream::into_split(self)
    }
}
