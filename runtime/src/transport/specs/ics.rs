//! Interconnect Connection Specification Protocol.
use aead::{AeadInPlace, Buffer, Key, KeyInit, Nonce, OsRng};
use aes_gcm::Aes128Gcm;
use hkdf::Hkdf;
use sha2::Sha256;

use crate::error::ProtocolResult;
use crate::error::{ErrKind, ProtocolError};
use crate::opt::branch_hints::unlikely;

const ICS_FRAME_LEN: usize = 8;

/// Protocol signature.
/// `ICS0` = Interconnect Connection Specification Variant 0.
const ICS_SIG_0: &[u8; 4] = b"ICS0";

#[derive(Debug, Clone, Copy)]
pub struct ConnectionSpecs {
    /// Announced ABI version.
    pub version: u8,
    pub encrypted: bool,
}

impl ConnectionSpecs {
    #[inline(always)]
    pub const fn new(version: u8, encrypted: bool) -> Self {
        Self { version, encrypted }
    }
}

pub type NonceBase = [u8; 4];

pub type SenderKey = [u8; 16];
pub type SenderState = EncryptionProvider;

pub type ReceiverKey = [u8; 16];
pub type ReceiverState = EncryptionProvider;

/// Provides encryption and decryption functionalities.
///
/// This provider uses `AES-GCM` cipher with 128-bit key.
///
/// It offers round limit up to `u64::MAX` before requiring rekeying.
pub struct EncryptionProvider {
    cipher: Aes128Gcm,
    sequence: u64,
    nonce_base: [u8; 4],
}

impl EncryptionProvider {
    pub fn new(key: [u8; 16], nonce_base: [u8; 4]) -> Self {
        let aes_key = Key::<Aes128Gcm>::from(key);
        let cipher = Aes128Gcm::new(&aes_key);
        Self {
            cipher,
            sequence: 0,
            nonce_base,
        }
    }

    #[inline]
    fn next_nonce(&mut self) -> [u8; 12] {
        // Note: Nonce must be 12-bytes in order to use it directly and avoid rehashing.
        let mut nonce = [0u8; 12];
        nonce[0..4].copy_from_slice(&self.nonce_base);
        nonce[4..12].copy_from_slice(&self.sequence.to_le_bytes());
        self.sequence += 1;
        nonce
    }

    /// Encrypts the data in the buffer in-place.
    /// The buffer will be resized if needed.
    pub fn encrypt<E: Buffer>(
        &mut self,
        data: &mut E,
        associated_data: &[u8],
    ) -> ProtocolResult<()> {
        // TODO: Make limit configurable.
        if unlikely(self.sequence == u64::MAX) {
            return Err(ProtocolError::error(ErrKind::RoundLimit));
        }
        let next = self.next_nonce();
        let nonce = Nonce::<Aes128Gcm>::from_slice(&next);
        self.cipher
            .encrypt_in_place(nonce, associated_data, data)
            .map_err(|_| ProtocolError::error(ErrKind::Encryption))
    }

    /// Decrypts the message in-place to its original format.
    /// The buffer will be truncated to the length of the original data upon success.
    pub fn decrypt<D: Buffer>(
        &mut self,
        data: &mut D,
        associated_data: &[u8],
    ) -> ProtocolResult<()> {
        // TODO: Resigned the interface without requiring `Buffer` trait.
        let next = self.next_nonce();
        let nonce = Nonce::<Aes128Gcm>::from_slice(&next);
        self.cipher
            .decrypt_in_place(nonce, associated_data, data)
            .map_err(|_| ProtocolError::error(ErrKind::Decryption))
    }
}

/// This module contains the functions used to establish connections.
///
/// ICS uses a specification frame which is 8-bytes in size, and it has two sections:
/// - Protocol signature (header): The first 4 bytes of the frame encode the fixed parameters
///   used to identify the protocol's predefined set of parameters.
/// - Associated data: The last 4 bytes of the frame encode the relative parameters
///   as associated data that can be used to extend the behavior.
///
/// # Protocol Variant
///
/// This implementation works according to the variant `0` with `ICS0` as signature.
/// The `ICS0` signature defines the security mechanisms and the semantics of the associated data.
///
/// - Protocol signature (fixed parameters):
///   - Security level: 128-bit.
///   - Key Exchange: DH-X25519.
///   - KDF: HKDF-SHA256.
///   - Cipher: AES128GCM.
///
/// - Protocol data:
///   - Byte at 4: ABI version.
///   - Byte at 5: Encryption options:
///     - `0x00` = Unencrypted.
///     - `0x01` = Encrypted.
///   - Byte at 6: Identity options (Reserved, not implemented).
///   - Byte at 7: Resumption options (Reserved, not implemented).
///
/// This entire recipe is represented by the protocol header.
///
/// Different protocol header implies different set of parameters and semantics of the associated data.
///
/// # Negotiation.
///
/// The client-side is the side that initiates the negotiation using the `initiate` function.
/// Initiation starts by sending `specification-frame`.
///
/// The server-side waits for the `specification-frame` to arrive and compares the announced
/// specifications with its configurations.
///
/// The server then sends back its response as single byte that represents two states:
///
/// - `0x00` = rejected/abort.
///
/// - `0x01` = accepted.
///
/// If the specifications have the encryption-flag set and server has confirmed, the server should
/// expect a key-exchange session using `accept_key_exchange` function.
///
/// The client should start a key-exchange session using `initiate_key_exchange` function.
///
/// Each side sends its public key which is an ephemeral 32-bytes long X25519 public key.  
///
/// The the shared-secret is then derived using `diffie-hellman` algorithm.
///
/// The derived shared-secret is then passed to HMAC-based key derivation function to construct
/// AEAD-based encryption-states, one for sending and another for receiving.
///
/// Each state has its own `Aes128Gcm` cipher constructed with derived unique key alongside nonce base
/// and internal counter.
///
/// Each state encrypt and decrypt after generating 12-bytes nonce derived from the base and the counter.
///
pub mod negotiation {
    use super::*;

    use x25519_dalek::{EphemeralSecret, PublicKey};

    use crate::error::{ErrKind, ProtocolError, ProtocolResult};
    use crate::transport::traits::{BytesReceiver, BytesSender, BytesTransport};

    pub async fn receive_specs<T>(transport: &mut T) -> ProtocolResult<ConnectionSpecs>
    where
        T: BytesReceiver,
    {
        let mut destination = [0u8; ICS_FRAME_LEN];
        transport.receive(&mut destination).await?;

        if &destination[0..4] != ICS_SIG_0 {
            return Err(ProtocolError::error(ErrKind::InvalidNegotiation));
        }

        let abi = destination[4];
        let flags = destination[5];
        let encrypted = (flags & 0x01) != 0;

        Ok(ConnectionSpecs {
            version: abi,
            encrypted,
        })
    }

    pub async fn send_specs<T>(transport: &mut T, specs: &ConnectionSpecs) -> ProtocolResult<()>
    where
        T: BytesSender,
    {
        let mut source = [0u8; ICS_FRAME_LEN];
        source[0..4].copy_from_slice(ICS_SIG_0);
        source[4] = specs.version;
        source[5] = specs.encrypted as u8;
        source[6..8].copy_from_slice(&0u16.to_le_bytes());
        transport.send(&source).await
    }

    /// Send a confirmation (0x01) to the transport.
    #[inline(always)]
    pub async fn confirm<T>(transport: &mut T) -> ProtocolResult<()>
    where
        T: BytesSender,
    {
        transport.send(&[0x01]).await
    }

    /// Send a rejection (0x00) to the transport.
    #[inline(always)]
    pub async fn reject<T>(transport: &mut T) -> ProtocolResult<()>
    where
        T: BytesSender,
    {
        transport.send(&[0x00]).await
    }

    /// Initiates a capability negotiation.
    pub async fn initiate<T>(transport: &mut T, capability: ConnectionSpecs) -> ProtocolResult<()>
    where
        T: BytesTransport,
    {
        self::send_specs(transport, &capability).await?;

        let mut response = [0u8; 1];
        transport.receive(&mut response).await?;

        match response[0] {
            0x01 => Ok(()),
            0x00 => Err(ProtocolError::error(ErrKind::SpecsMismatch)),
            _ => Err(ProtocolError::error(ErrKind::InvalidNegotiation)),
        }
    }

    /// Initiates an expected cryptographic key-exchange session.
    pub async fn initiate_key_exchange<T>(
        transport: &mut T,
    ) -> ProtocolResult<(SenderState, ReceiverState)>
    where
        T: BytesTransport,
    {
        let client_secret = EphemeralSecret::random_from_rng(OsRng);

        let client_public = PublicKey::from(&client_secret);
        transport.send(client_public.as_bytes()).await?;

        let mut server_pub_bytes = [0u8; 32];
        transport.receive(&mut server_pub_bytes).await?;
        let server_public = PublicKey::from(server_pub_bytes);

        let shared_secret = client_secret.diffie_hellman(&server_public);
        let (recv_key, send_key, nonce_base) = derive_session_keys(shared_secret.as_bytes())?;

        let sender_state = EncryptionProvider::new(send_key, nonce_base);
        let receiver_state = EncryptionProvider::new(recv_key, nonce_base);

        Ok((sender_state, receiver_state))
    }

    /// Accepts an expected cryptographic key-exchange session.
    pub async fn accept_key_exchange<T>(
        transport: &mut T,
    ) -> ProtocolResult<(SenderState, ReceiverState)>
    where
        T: BytesTransport,
    {
        let mut client_pub_bytes = [0u8; 32];
        transport.receive(&mut client_pub_bytes).await?;
        let client_public = PublicKey::from(client_pub_bytes);

        let server_secret = EphemeralSecret::random_from_rng(OsRng);

        let server_public = PublicKey::from(&server_secret);
        transport.send(server_public.as_bytes()).await?;

        let shared_secret = server_secret.diffie_hellman(&client_public);
        let (send_key, recv_key, nonce_base) = derive_session_keys(shared_secret.as_bytes())?;

        let sender_state = EncryptionProvider::new(send_key, nonce_base);
        let receiver_state = EncryptionProvider::new(recv_key, nonce_base);

        Ok((sender_state, receiver_state))
    }

    /// HMAC-based key-derivation function.
    fn derive_session_keys(
        shared_secret: &[u8],
    ) -> ProtocolResult<(SenderKey, ReceiverKey, NonceBase)> {
        let hkdf = Hkdf::<Sha256>::new(Some(b"ics-session"), shared_secret);

        let mut send_key = [0u8; 16];
        let mut recv_key = [0u8; 16];
        let mut nonce_base = [0u8; 4];

        let map_err = |_| ProtocolError::error(ErrKind::KeyDerivation);

        hkdf.expand(b"ics-send", &mut send_key).map_err(map_err)?;

        hkdf.expand(b"ics-receive", &mut recv_key)
            .map_err(map_err)?;

        hkdf.expand(b"ics-nonce-base", &mut nonce_base)
            .map_err(map_err)?;

        Ok((send_key, recv_key, nonce_base))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;

    use tokio::net::{TcpListener, TcpStream};

    use crate::transport::traits::{BytesReceiver, BytesSender};

    #[tokio::test]
    async fn test_negotiation_with_encryption() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut transport, _) = listener.accept().await.expect("accept failed");

            let proposed = negotiation::receive_specs(&mut transport)
                .await
                .expect("server negotiation failed");

            negotiation::confirm(&mut transport)
                .await
                .expect("Failed to send confirmation");

            let (_s_state, mut recv_state) = if proposed.encrypted {
                negotiation::accept_key_exchange(&mut transport)
                    .await
                    .expect("server encryption failed")
            } else {
                panic!("expected encryption");
            };

            // First message.
            let mut bytes = [0u8; 2];
            transport.receive(&mut bytes).await.unwrap();
            let len = u16::from_le_bytes(bytes) as usize;

            let mut buffer = vec![0u8; len];
            transport.receive(&mut buffer).await.unwrap();

            recv_state.decrypt(&mut buffer, b"").unwrap();
            assert_eq!(&buffer, b"first message!");

            // Second message.
            transport.receive(&mut bytes).await.unwrap();
            let len = u16::from_le_bytes(bytes) as usize;

            let mut buffer = vec![0u8; len];
            transport.receive(&mut buffer).await.unwrap();

            recv_state.decrypt(&mut buffer, b"").unwrap();
            assert_eq!(&buffer, b"second message!");
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut transport = TcpStream::connect(&addr).await.unwrap();

        let capability = ConnectionSpecs {
            version: 1,
            encrypted: true,
        };

        negotiation::initiate(&mut transport, capability)
            .await
            .expect("client negotiation failed");

        let (mut send_state, _r_state) = negotiation::initiate_key_exchange(&mut transport)
            .await
            .expect("client encryption failed");

        // First message.
        let mut buffer = b"first message!".to_vec();
        send_state.encrypt(&mut buffer, b"").unwrap();

        let mut bytes = (buffer.len() as u16).to_le_bytes();
        transport.send(&mut bytes).await.unwrap();
        transport.send(&buffer).await.unwrap();

        // Second message.
        let mut buffer = b"second message!".to_vec();
        send_state.encrypt(&mut buffer, b"").unwrap();

        let mut bytes = (buffer.len() as u16).to_le_bytes();
        transport.send(&mut bytes).await.unwrap();
        transport.send(&buffer).await.unwrap();

        server.await.unwrap();
    }
}
