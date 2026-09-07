use aes_gcm::aead::rand_core::{OsRng, RngCore};
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;

const PREFIX: &str = "DAIKON-ENC1:";

fn key_from_env() -> Result<Option<[u8; 32]>, String> {
    let Some(raw) = std::env::var("KV_AT_REST_KEY")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };

    let decoded = hex::decode(&raw)
        .or_else(|_| STANDARD.decode(&raw))
        .map_err(|_| "KV_AT_REST_KEY must be 32-byte hex or base64".to_string())?;
    let key: [u8; 32] = decoded
        .try_into()
        .map_err(|_| "KV_AT_REST_KEY must decode to exactly 32 bytes".to_string())?;
    Ok(Some(key))
}

pub fn enabled() -> bool {
    key_from_env().map(|key| key.is_some()).unwrap_or(false)
}

pub fn encrypt(data: &[u8]) -> Result<Vec<u8>, String> {
    let Some(key) = key_from_env()? else {
        return Ok(data.to_vec());
    };

    let cipher =
        Aes256Gcm::new_from_slice(&key).map_err(|_| "invalid encryption key".to_string())?;
    let mut nonce_bytes = [0u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher
        .encrypt(nonce, data)
        .map_err(|_| "encryption failed".to_string())?;

    let mut payload = nonce_bytes.to_vec();
    payload.extend_from_slice(&ciphertext);
    Ok(format!("{}{}", PREFIX, STANDARD.encode(payload)).into_bytes())
}

pub fn decrypt(data: &[u8]) -> Result<Vec<u8>, String> {
    if !data.starts_with(PREFIX.as_bytes()) {
        return Ok(data.to_vec());
    }

    let Some(key) = key_from_env()? else {
        return Err("encrypted data requires KV_AT_REST_KEY".to_string());
    };
    let payload = STANDARD
        .decode(&data[PREFIX.len()..])
        .map_err(|_| "invalid encrypted data".to_string())?;
    if payload.len() < 12 {
        return Err("invalid encrypted data".to_string());
    }

    let cipher =
        Aes256Gcm::new_from_slice(&key).map_err(|_| "invalid encryption key".to_string())?;
    cipher
        .decrypt(Nonce::from_slice(&payload[..12]), &payload[12..])
        .map_err(|_| "decryption failed; check KV_AT_REST_KEY".to_string())
}
