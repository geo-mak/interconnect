use std::io;
use std::net::SocketAddr;
use std::path::Path;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::ToSocketAddrs;
use tokio::net::tcp;
use tokio::net::unix;
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};

use crate::io::{AsyncIORead, AsyncIOWrite};

pub trait TransportListener<A> {
    type Address;
    type Transport: TransportLayer;

    fn bind(addr: A) -> impl Future<Output = io::Result<Self>>
    where
        Self: Sized;

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
/// Transport layer is untyped byte-stream oriented API.
///
/// Types that implement this trait should comply with the following requirements:
///
/// - They provide two modes of operation: single-mode and split-mode.
///
/// - In split-mode, the reader and writer shall enable unconstrained full-duplex communication style.
pub trait TransportLayer: AsyncIORead + AsyncIOWrite + Send + Unpin {
    type Reader: AsyncIORead + Send + Sync + Unpin;
    type Writer: AsyncIOWrite + Send + Sync + Unpin;
    fn into_split(self) -> (Self::Reader, Self::Writer);
}

impl AsyncIORead for TcpStream {
    #[inline(always)]
    async fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncReadExt::read(self, output).await
    }

    #[inline(always)]
    async fn read_exact(&mut self, output: &mut [u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncReadExt::read_exact(self, output).await
    }
}

impl AsyncIOWrite for TcpStream {
    #[inline(always)]
    async fn write(&mut self, input: &[u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncWriteExt::write(self, input).await
    }

    #[inline(always)]
    async fn write_all(&mut self, input: &[u8]) -> std::io::Result<()>
    where
        Self: Unpin,
    {
        AsyncWriteExt::write_all(self, input).await
    }

    #[inline(always)]
    async fn terminate(&mut self) -> std::io::Result<()>
    where
        Self: Unpin,
    {
        AsyncWriteExt::shutdown(self).await
    }
}

impl AsyncIORead for tcp::OwnedReadHalf {
    #[inline(always)]
    async fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncReadExt::read(self, output).await
    }

    #[inline(always)]
    async fn read_exact(&mut self, output: &mut [u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncReadExt::read_exact(self, output).await
    }
}

impl AsyncIOWrite for tcp::OwnedWriteHalf {
    #[inline(always)]
    async fn write(&mut self, input: &[u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncWriteExt::write(self, input).await
    }

    #[inline(always)]
    async fn write_all(&mut self, input: &[u8]) -> std::io::Result<()>
    where
        Self: Unpin,
    {
        AsyncWriteExt::write_all(self, input).await
    }

    #[inline(always)]
    async fn terminate(&mut self) -> std::io::Result<()>
    where
        Self: Unpin,
    {
        AsyncWriteExt::shutdown(self).await
    }
}

impl AsyncIORead for UnixStream {
    #[inline(always)]
    async fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncReadExt::read(self, output).await
    }

    #[inline(always)]
    async fn read_exact(&mut self, output: &mut [u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncReadExt::read_exact(self, output).await
    }
}

impl AsyncIOWrite for UnixStream {
    #[inline(always)]
    async fn write(&mut self, input: &[u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncWriteExt::write(self, input).await
    }

    #[inline(always)]
    async fn write_all(&mut self, input: &[u8]) -> std::io::Result<()>
    where
        Self: Unpin,
    {
        AsyncWriteExt::write_all(self, input).await
    }

    #[inline(always)]
    async fn terminate(&mut self) -> std::io::Result<()>
    where
        Self: Unpin,
    {
        AsyncWriteExt::shutdown(self).await
    }
}

impl AsyncIORead for unix::OwnedReadHalf {
    #[inline(always)]
    async fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncReadExt::read(self, output).await
    }

    #[inline(always)]
    async fn read_exact(&mut self, output: &mut [u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncReadExt::read_exact(self, output).await
    }
}

impl AsyncIOWrite for unix::OwnedWriteHalf {
    #[inline(always)]
    async fn write(&mut self, input: &[u8]) -> std::io::Result<usize>
    where
        Self: Unpin,
    {
        AsyncWriteExt::write(self, input).await
    }

    #[inline(always)]
    async fn write_all(&mut self, input: &[u8]) -> std::io::Result<()>
    where
        Self: Unpin,
    {
        AsyncWriteExt::write_all(self, input).await
    }

    #[inline(always)]
    async fn terminate(&mut self) -> std::io::Result<()>
    where
        Self: Unpin,
    {
        AsyncWriteExt::shutdown(self).await
    }
}

impl TransportLayer for TcpStream {
    type Reader = tcp::OwnedReadHalf;
    type Writer = tcp::OwnedWriteHalf;

    #[inline(always)]
    fn into_split(self) -> (Self::Reader, Self::Writer) {
        TcpStream::into_split(self)
    }
}

impl TransportLayer for UnixStream {
    type Reader = unix::OwnedReadHalf;
    type Writer = unix::OwnedWriteHalf;

    #[inline(always)]
    fn into_split(self) -> (Self::Reader, Self::Writer) {
        UnixStream::into_split(self)
    }
}
