use aead::{AeadInPlace, Buffer, KeyInit, Nonce, OsRng};
use aes_gcm::Aes128Gcm;
use hkdf::Hkdf;
use sha2::Sha256;

use crate::next::error::ProtocolResult;
use crate::next::error::{ErrKind, ProtocolError};
use crate::opt::branch_hints::unlikely;

//           ----------------------------------------------
//           |         IPC SPECIFICATION PROTOCOL         |
//           ----------------------------------------------
//
//                         ----------------
//                         |  DATA FLOW   |
//                         ----------------
//
//              CLIENT                           SERVER
//                |                                 |
//                | Specification Frame (8 bytes)   |
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
// |         SPECIFICATION FRAME DATA           |
// ----------------------------------------------
//
// Specification frame (8 bytes)
// [0..4]   Specification protocol signature (and version)
// [4]      Stream version
// [5]      Flags:
//            0x01 = encryption enabled
//            0x02 = identity required (not implemented, future use)
// [6..8]   Reserved = 0 (2 bytes)
//
// Confirmation byte (1 byte):
// 0x01 = accepted
// 0x00 = rejected/abort
//
// Key exchange (currently):
// [0..32]  Client ephemeral X25519 public key
// [0..32]  Server ephemeral X25519 public key

const SPECS_FRAME_LEN: usize = 8;

/// Protocol flags.
/// `ICS0` = Interconnect Specification Version 0.
const SPECS_PROTO: &[u8; 4] = b"ICS0";

#[derive(Debug, Clone, Copy)]
pub struct ConnectionSpecs {
    /// Announced ABI version.
    pub abi: u8,
    pub encryption: bool,
}

impl ConnectionSpecs {
    #[inline(always)]
    pub const fn new(abi: u8, encryption: bool) -> Self {
        Self { abi, encryption }
    }
}

pub type NonceBase = [u8; 4];
pub type SenderKey = [u8; 16];
pub type ReceiverKey = [u8; 16];
pub type SenderState = EncryptionState;
pub type ReceiverState = EncryptionState;

/// Stores the cipher-state and provides encryption and decryption methods.
pub struct EncryptionState {
    cipher: Aes128Gcm,
    sequence: u64,
    nonce_base: [u8; 4],
}

impl EncryptionState {
    pub fn new(key: &[u8], nonce_base: [u8; 4]) -> ProtocolResult<Self> {
        let cipher = Aes128Gcm::new_from_slice(key)
            .map_err(|_| ProtocolError::error(ErrKind::InvalidEncryptionKey))?;
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

pub mod negotiation {
    use super::*;

    use x25519_dalek::{EphemeralSecret, PublicKey};

    use crate::next::error::{ErrKind, ProtocolError, ProtocolResult};
    use crate::next::transport::traits::BytesTransport;

    pub async fn read_frame<T>(transport: &mut T) -> ProtocolResult<ConnectionSpecs>
    where
        T: BytesTransport,
    {
        let mut destination = [0u8; SPECS_FRAME_LEN];
        transport.receive_bytes(&mut destination).await?;

        if &destination[0..4] != SPECS_PROTO {
            return Err(ProtocolError::error(ErrKind::InvalidNegotiation));
        }

        let abi = destination[4];
        let flags = destination[5];
        let encryption = (flags & 0x01) != 0;

        Ok(ConnectionSpecs { abi, encryption })
    }

    pub async fn write_frame<T>(transport: &mut T, specs: &ConnectionSpecs) -> ProtocolResult<()>
    where
        T: BytesTransport,
    {
        let mut source = [0u8; SPECS_FRAME_LEN];
        source[0..4].copy_from_slice(SPECS_PROTO);
        source[4] = specs.abi;
        source[5] = specs.encryption as u8;
        source[6..8].copy_from_slice(&0u16.to_le_bytes());
        transport.send_bytes(&source).await
    }

    /// Send a confirmation (0x01) to the transport.
    #[inline(always)]
    pub async fn confirm<T>(transport: &mut T) -> ProtocolResult<()>
    where
        T: BytesTransport,
    {
        transport.send_bytes(&[0x01]).await
    }

    /// Send a rejection (0x00) to the transport.
    #[inline(always)]
    pub async fn reject<T>(transport: &mut T) -> ProtocolResult<()>
    where
        T: BytesTransport,
    {
        transport.send_bytes(&[0x00]).await
    }

    /// Initiates a capability negotiation.
    pub async fn initiate<T>(transport: &mut T, capability: ConnectionSpecs) -> ProtocolResult<()>
    where
        T: BytesTransport,
    {
        self::write_frame(transport, &capability).await?;

        let mut confirmation = [0u8; 1];
        transport.receive_bytes(&mut confirmation).await?;

        match confirmation[0] {
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
        transport.send_bytes(client_public.as_bytes()).await?;

        let mut server_pub_bytes = [0u8; 32];
        transport.receive_bytes(&mut server_pub_bytes).await?;
        let server_public = PublicKey::from(server_pub_bytes);

        let shared = client_secret.diffie_hellman(&server_public);
        let (recv_key, send_key, nonce_base) = derive_session_keys(shared.as_bytes())?;

        Ok((
            EncryptionState::new(&send_key, nonce_base)?,
            EncryptionState::new(&recv_key, nonce_base)?,
        ))
    }

    /// Accepts an expected cryptographic key-exchange session.
    pub async fn accept_key_exchange<T>(
        transport: &mut T,
    ) -> ProtocolResult<(SenderState, ReceiverState)>
    where
        T: BytesTransport,
    {
        let mut client_pub_bytes = [0u8; 32];
        transport.receive_bytes(&mut client_pub_bytes).await?;
        let client_public = PublicKey::from(client_pub_bytes);

        let server_secret = EphemeralSecret::random_from_rng(OsRng);
        let server_public = PublicKey::from(&server_secret);
        transport.send_bytes(server_public.as_bytes()).await?;

        let shared = server_secret.diffie_hellman(&client_public);
        let (send_key, recv_key, nonce_base) = derive_session_keys(shared.as_bytes())?;

        Ok((
            EncryptionState::new(&send_key, nonce_base)?,
            EncryptionState::new(&recv_key, nonce_base)?,
        ))
    }

    /// HMAC-based key-derivation function.
    fn derive_session_keys(
        shared_secret: &[u8],
    ) -> ProtocolResult<(SenderKey, ReceiverKey, NonceBase)> {
        let hkdf = Hkdf::<Sha256>::new(Some(b"ics-negotiation"), shared_secret);

        let mut nonce_base = [0u8; 4];
        let mut send_key = [0u8; 16];
        let mut recv_key = [0u8; 16];

        let map_err = |_| ProtocolError::error(ErrKind::KeyDerivation);

        hkdf.expand(b"ics-session-read", &mut recv_key)
            .map_err(map_err)?;
        hkdf.expand(b"ics-session-write", &mut send_key)
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

    use crate::next::transport::traits::{BytesReceiver, BytesSender};

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

            let (_s_state, mut recv_state) = if proposed.encryption {
                negotiation::accept_key_exchange(&mut transport)
                    .await
                    .expect("server encryption failed")
            } else {
                panic!("expected encryption");
            };

            // First message.
            let mut bytes = [0u8; 2];
            transport.receive_bytes(&mut bytes).await.unwrap();
            let len = u16::from_le_bytes(bytes) as usize;

            let mut buffer = vec![0u8; len];
            transport.receive_bytes(&mut buffer).await.unwrap();

            recv_state.decrypt(&mut buffer, b"").unwrap();
            assert_eq!(&buffer, b"first message!");

            // Second message.
            transport.receive_bytes(&mut bytes).await.unwrap();
            let len = u16::from_le_bytes(bytes) as usize;

            let mut buffer = vec![0u8; len];
            transport.receive_bytes(&mut buffer).await.unwrap();

            recv_state.decrypt(&mut buffer, b"").unwrap();
            assert_eq!(&buffer, b"second message!");
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut transport = TcpStream::connect(&addr).await.unwrap();

        let capability = ConnectionSpecs {
            abi: 1,
            encryption: true,
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
        transport.send_bytes(&mut bytes).await.unwrap();
        transport.send_bytes(&buffer).await.unwrap();

        // Second message.
        let mut buffer = b"second message!".to_vec();
        send_state.encrypt(&mut buffer, b"").unwrap();

        let mut bytes = (buffer.len() as u16).to_le_bytes();
        transport.send_bytes(&mut bytes).await.unwrap();
        transport.send_bytes(&buffer).await.unwrap();

        server.await.unwrap();
    }
}
