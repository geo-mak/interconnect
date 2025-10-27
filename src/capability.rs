use std::io;

use aead::{AeadInPlace, Buffer, KeyInit, Nonce, OsRng};
use aes_gcm::Aes128Gcm;
use hkdf::Hkdf;
use sha2::Sha256;

use crate::error::{ErrKind, RpcError, RpcResult};
use crate::opt::branch_prediction::unlikely;

// ----------------------------------------------
// |         RPC CAPABILITY PROTOCOL            |
// ----------------------------------------------
//
// Currently, the client initiates capability negotiation by sending an 8-byte
// capability frame. The server either accepts it or rejects it with a 1-byte
// confirmation:
//
//                         ----------------
//                         |  DATA FLOW   |
//                         ----------------
//
//              CLIENT                           SERVER
//                |                                 |
//                | Capability Frame (8 bytes)      |
//                |-------------------------------> |
//                |                                 |
//                | Server applies its policy then: |
//                |                                 |
//                | 1-byte Confirmation             |
//                |    0x01 = Accepted              |
//                |    0x00 = Rejected              |
//                | <-------------------------------|
//                |                                 |
//
//                If encryption is enabled:
//
//                | Ephemeral X25519 Public Key     |
//                |-------------------------------> |
//                | Ephemeral X25519 Public Key     |
//                | <-------------------------------|
//
// ------------------------                 ------------------------
// | derive shared secret |                 | derive shared secret |
// | via x25519 + HKDF    |                 | via x25519 + HKDF    |
// ------------------------                 ------------------------
//
//                       ENCRYPTED SESSION BEGINS
//

// ----------------------------------------------
// |           CAPABILITY FRAME DATA            |
// ----------------------------------------------
//
// Capability frame (8 bytes)
// [0..4]   protocol signature (and version)
// [4]      rpc stream version
// [5]      other flags:
//            0x01 = encryption enabled
//            0x02 = identity required (not implemented, future use)
// [6..8]   reserved = 0 (2 bytes)
//
// Confirmation byte (1 byte):
// 0x01 = accepted
// 0x00 = rejected/abort
//
// Key exchange (currently):
// [0..32]  Client ephemeral X25519 public key
// [0..32]  Server ephemeral X25519 public key

const CAPABILITY_FRAME_LEN: usize = 8;

/// Protocol flags.
const PROTO: &[u8; 4] = b"RPC0";

#[derive(Debug, Clone, Copy)]
pub struct RpcCapability {
    /// Announced stream version. This is similar to the version of application data in TLS.
    pub version: u8,
    pub encryption: bool,
}

impl RpcCapability {
    #[inline(always)]
    pub const fn new(version: u8, encryption: bool) -> Self {
        Self {
            version,
            encryption,
        }
    }
}

pub type ReadKey = [u8; 16];
pub type WriteKey = [u8; 16];
pub type NonceBase = [u8; 4];

pub type ReadState = EncryptionState;
pub type WriteState = EncryptionState;

/// Stores the cipher-state and provides encryption and decryption methods.
pub struct EncryptionState {
    cipher: Aes128Gcm,
    sequence: u64,
    nonce_base: [u8; 4],
}

impl EncryptionState {
    pub fn new(key: &[u8], nonce_base: [u8; 4]) -> RpcResult<Self> {
        let cipher =
            Aes128Gcm::new_from_slice(key).map_err(|_| RpcError::error(ErrKind::InvalidKey))?;
        Ok(Self {
            cipher,
            sequence: 0,
            nonce_base,
        })
    }

    #[inline]
    fn next_nonce(&mut self) -> [u8; 12] {
        let mut nonce = [0u8; 12];
        nonce[0..4].copy_from_slice(&self.nonce_base);
        nonce[4..12].copy_from_slice(&self.sequence.to_le_bytes());
        self.sequence += 1;
        nonce
    }

    /// Encrypts the data in the buffer in-place.
    /// The buffer will be resized if needed.
    pub fn encrypt(&mut self, data: &mut impl Buffer, associated_data: &[u8]) -> RpcResult<()> {
        // TODO: Make limit configurable.
        if unlikely(self.sequence == u64::MAX) {
            return Err(RpcError::error(ErrKind::MaxLimit));
        }
        let next = self.next_nonce();
        let nonce = Nonce::<Aes128Gcm>::from_slice(&next);
        self.cipher
            .encrypt_in_place(nonce, associated_data, data)
            .map_err(|_| RpcError::error(ErrKind::Encryption))
    }

    /// Decrypts the message in-place to its original format.
    /// The buffer will be truncated to the length of the original data upon success.
    pub fn decrypt(&mut self, data: &mut impl Buffer, associated_data: &[u8]) -> RpcResult<()> {
        // TODO: Resigned the interface without requiring `Buffer` trait.
        let next = self.next_nonce();
        let nonce = Nonce::<Aes128Gcm>::from_slice(&next);
        self.cipher
            .decrypt_in_place(nonce, associated_data, data)
            .map_err(|_| RpcError::error(ErrKind::Decryption))
    }
}

pub mod negotiation {
    use super::*;
    use crate::TransportLayer;
    use x25519_dalek::{EphemeralSecret, PublicKey};

    pub async fn read_frame<T>(transport: &mut T) -> io::Result<RpcCapability>
    where
        T: TransportLayer,
    {
        let mut buf = [0u8; CAPABILITY_FRAME_LEN];
        transport.read_exact(&mut buf).await?;

        if &buf[0..4] != PROTO {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Unsupported protocol",
            ));
        }

        let version = buf[4];
        let flags = buf[5];
        let encryption = (flags & 0x01) != 0;

        Ok(RpcCapability {
            version,
            encryption,
        })
    }

    pub async fn write_frame<T>(transport: &mut T, capability: &RpcCapability) -> io::Result<()>
    where
        T: TransportLayer,
    {
        let mut buf = [0u8; CAPABILITY_FRAME_LEN];
        buf[0..4].copy_from_slice(PROTO);
        buf[4] = capability.version;
        buf[5] = capability.encryption as u8;
        buf[6..8].copy_from_slice(&0u16.to_le_bytes());
        transport.write_all(&buf).await
    }

    /// Send a confirmation (0x01) to the transport.
    #[inline(always)]
    pub async fn confirm<T>(transport: &mut T) -> io::Result<()>
    where
        T: TransportLayer,
    {
        transport.write_all(&[0x01]).await
    }

    /// Send a rejection (0x00) to the transport.
    #[inline(always)]
    pub async fn reject<T>(transport: &mut T) -> io::Result<()>
    where
        T: TransportLayer,
    {
        transport.write_all(&[0x00]).await
    }

    /// Initiates a capability negotiation.
    pub async fn initiate<T>(transport: &mut T, capability: RpcCapability) -> RpcResult<()>
    where
        T: TransportLayer,
    {
        self::write_frame(transport, &capability).await?;

        let mut confirm = [0u8; 1];
        transport.read_exact(&mut confirm).await?;

        match confirm[0] {
            0x01 => Ok(()),
            0x00 => Err(RpcError::error(ErrKind::CapabilityMismatch)),
            _ => Err(RpcError::error(ErrKind::InvalidNegotiation)),
        }
    }

    /// Initiates an expected cryptographic key-exchange session.
    pub async fn initiate_key_exchange<T>(transport: &mut T) -> RpcResult<(ReadState, WriteState)>
    where
        T: TransportLayer,
    {
        let client_secret = EphemeralSecret::random_from_rng(OsRng);
        let client_public = PublicKey::from(&client_secret);
        transport.write_all(client_public.as_bytes()).await?;

        let mut server_pub_bytes = [0u8; 32];
        transport.read_exact(&mut server_pub_bytes).await?;
        let server_public = PublicKey::from(server_pub_bytes);

        let shared = client_secret.diffie_hellman(&server_public);
        let (r_key, w_key, nonce_base) = derive_session_keys(shared.as_bytes())?;

        Ok((
            EncryptionState::new(&r_key, nonce_base)?,
            EncryptionState::new(&w_key, nonce_base)?,
        ))
    }

    /// Accepts an expected cryptographic key-exchange session.
    pub async fn accept_key_exchange<T>(transport: &mut T) -> RpcResult<(ReadState, WriteState)>
    where
        T: TransportLayer,
    {
        let mut client_pub_bytes = [0u8; 32];
        transport.read_exact(&mut client_pub_bytes).await?;
        let client_public = PublicKey::from(client_pub_bytes);

        let server_secret = EphemeralSecret::random_from_rng(OsRng);
        let server_public = PublicKey::from(&server_secret);
        transport.write_all(server_public.as_bytes()).await?;

        let shared = server_secret.diffie_hellman(&client_public);
        let (w_key, r_key, nonce_base) = derive_session_keys(shared.as_bytes())?;

        Ok((
            EncryptionState::new(&r_key, nonce_base)?,
            EncryptionState::new(&w_key, nonce_base)?,
        ))
    }

    /// HMAC-based key-derivation function.
    fn derive_session_keys(shared_secret: &[u8]) -> RpcResult<(ReadKey, WriteKey, NonceBase)> {
        let hkdf = Hkdf::<Sha256>::new(Some(b"rpc-handshake"), shared_secret);

        let mut r_key = [0u8; 16];
        let mut w_key = [0u8; 16];
        let mut nonce_base = [0u8; 4];

        let map_err = |_| RpcError::error(ErrKind::KeyDerivation);

        hkdf.expand(b"rpc-session-read", &mut r_key)
            .map_err(map_err)?;
        hkdf.expand(b"rpc-session-write", &mut w_key)
            .map_err(map_err)?;
        hkdf.expand(b"rpc-nonce-base", &mut nonce_base)
            .map_err(map_err)?;

        Ok((r_key, w_key, nonce_base))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;
    use tokio::net::{TcpListener, TcpStream};

    use crate::io::{AsyncIORead, AsyncIOWrite};

    #[tokio::test]
    async fn test_negotiation_with_encryption() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut transport, _) = listener.accept().await.expect("accept failed");

            let proposed = negotiation::read_frame(&mut transport)
                .await
                .expect("server negotiation failed");

            negotiation::confirm(&mut transport)
                .await
                .expect("Failed to send confirmation");

            let (mut r_key, _w_key) = if proposed.encryption {
                negotiation::accept_key_exchange(&mut transport)
                    .await
                    .expect("server encryption failed")
            } else {
                panic!("expected encryption");
            };

            // First message.
            let mut bytes = [0u8; 2];
            transport.read(&mut bytes).await.unwrap();
            let len = u16::from_le_bytes(bytes) as usize;

            let mut buffer = vec![0u8; len];
            transport.read_exact(&mut buffer).await.unwrap();

            r_key.decrypt(&mut buffer, b"").unwrap();
            assert_eq!(&buffer, b"first message!");

            // Second message.
            transport.read(&mut bytes).await.unwrap();
            let len = u16::from_le_bytes(bytes) as usize;

            let mut buffer = vec![0u8; len];
            transport.read_exact(&mut buffer).await.unwrap();

            r_key.decrypt(&mut buffer, b"").unwrap();
            assert_eq!(&buffer, b"second message!");
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut transport = TcpStream::connect(addr).await.unwrap();

        let capability = RpcCapability {
            version: 1,
            encryption: true,
        };

        negotiation::initiate(&mut transport, capability)
            .await
            .expect("client negotiation failed");

        let (_r_key, mut w_key) = negotiation::initiate_key_exchange(&mut transport)
            .await
            .expect("client encryption failed");

        // First message.
        let mut buffer = b"first message!".to_vec();
        w_key.encrypt(&mut buffer, b"").unwrap();

        let mut bytes = (buffer.len() as u16).to_le_bytes();
        transport.write(&mut bytes).await.unwrap();
        transport.write_all(&buffer).await.unwrap();

        // Second message.
        let mut buffer = b"second message!".to_vec();
        w_key.encrypt(&mut buffer, b"").unwrap();

        let mut bytes = (buffer.len() as u16).to_le_bytes();
        transport.write(&mut bytes).await.unwrap();
        transport.write_all(&buffer).await.unwrap();

        server.await.unwrap();
    }
}
