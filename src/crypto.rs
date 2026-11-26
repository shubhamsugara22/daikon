use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use std::io::Write;
use std::path::Path;

const PREFIX: &str = "DAIKON-ENC1:";
const NONCE_SIZE: usize = 12;

fn decode_key(raw: &str) -> Result<[u8; 32], String> {
    let decoded = hex::decode(raw)
        .or_else(|_| STANDARD.decode(&raw))
        .map_err(|_| "encryption keys must be 32-byte hex or base64".to_string())?;
    decoded
        .try_into()
        .map_err(|_| "encryption keys must decode to exactly 32 bytes".to_string())
}

fn keys_from_env() -> Result<(Option<[u8; 32]>, Vec<[u8; 32]>), String> {
    let active = std::env::var("KV_AT_REST_KEY")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| decode_key(value.trim()))
        .transpose()?;
    let previous = std::env::var("KV_AT_REST_OLD_KEYS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(decode_key)
        .collect::<Result<Vec<_>, _>>()?;
    Ok((active, previous))
}

pub fn enabled() -> bool {
    keys_from_env()
        .map(|(active, _)| active.is_some())
        .unwrap_or(false)
}

pub fn validate_rotation_config() -> Result<(), String> {
    let (active, _) = keys_from_env()?;
    if active.is_none() {
        return Err("KV_AT_REST_KEY must be configured as the active key".to_string());
    }
    Ok(())
}

pub fn encrypt(data: &[u8]) -> Result<Vec<u8>, String> {
    let (active_key, previous_keys) = keys_from_env()?;
    let Some(key) = active_key else {
        if !previous_keys.is_empty() {
            return Err("KV_AT_REST_KEY must be set when previous keys are configured".to_string());
        }
        return Ok(data.to_vec());
    };

    let cipher =
        Aes256Gcm::new_from_slice(&key).map_err(|_| "invalid encryption key".to_string())?;
    let mut nonce_bytes = [0u8; NONCE_SIZE];
    getrandom::fill(&mut nonce_bytes).map_err(|_| "random nonce generation failed".to_string())?;
    let nonce = Nonce::try_from(&nonce_bytes[..]).map_err(|_| "invalid nonce".to_string())?;
    let ciphertext = cipher
        .encrypt(&nonce, data)
        .map_err(|_| "encryption failed".to_string())?;

    let mut payload = nonce_bytes.to_vec();
    payload.extend_from_slice(&ciphertext);
    Ok(format!("{}{}", PREFIX, STANDARD.encode(payload)).into_bytes())
}

pub fn decrypt(data: &[u8]) -> Result<Vec<u8>, String> {
    if !data.starts_with(PREFIX.as_bytes()) {
        return Ok(data.to_vec());
    }

    let payload = STANDARD
        .decode(&data[PREFIX.len()..])
        .map_err(|_| "invalid encrypted data".to_string())?;
    if payload.len() < NONCE_SIZE {
        return Err("invalid encrypted data".to_string());
    }

    let (active_key, previous_keys) = keys_from_env()?;
    let mut has_key = false;
    for key in active_key.into_iter().chain(previous_keys) {
        has_key = true;
        let cipher =
            Aes256Gcm::new_from_slice(&key).map_err(|_| "invalid encryption key".to_string())?;
        let nonce = Nonce::try_from(&payload[..NONCE_SIZE])
            .map_err(|_| "invalid encrypted data".to_string())?;
        if let Ok(plaintext) = cipher.decrypt(&nonce, &payload[NONCE_SIZE..]) {
            return Ok(plaintext);
        }
    }
    if !has_key {
        return Err("encrypted data requires KV_AT_REST_KEY".to_string());
    }
    Err("decryption failed; check KV_AT_REST_KEY and KV_AT_REST_OLD_KEYS".to_string())
}

pub fn reencrypt(data: &[u8]) -> Result<Vec<u8>, String> {
    validate_rotation_config()?;
    encrypt(&decrypt(data)?)
}

pub(crate) fn replace_file(path: &Path, contents: &[u8]) -> Result<(), String> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut temp_file =
        tempfile::NamedTempFile::new_in(parent).map_err(|error| error.to_string())?;
    if let Ok(metadata) = path.metadata() {
        temp_file
            .as_file()
            .set_permissions(metadata.permissions())
            .map_err(|error| error.to_string())?;
    }
    temp_file
        .write_all(contents)
        .map_err(|error| error.to_string())?;
    temp_file
        .persist(path)
        .map(|_| ())
        .map_err(|error| error.error.to_string())
}

#[cfg(test)]
pub(crate) static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plaintext_roundtrip_without_key() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("KV_AT_REST_KEY");
        let data = b"wal-entry";
        assert_eq!(decrypt(&encrypt(data).unwrap()).unwrap(), data);
    }

    #[test]
    fn encrypted_roundtrip_with_hex_key() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var(
            "KV_AT_REST_KEY",
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        );
        let data = b"snapshot-data";
        let encrypted = encrypt(data).unwrap();
        assert!(encrypted.starts_with(PREFIX.as_bytes()));
        assert_eq!(decrypt(&encrypted).unwrap(), data);
        std::env::remove_var("KV_AT_REST_KEY");
    }

    #[test]
    fn decrypts_previous_key_and_reencrypts_with_active_key() {
        let _guard = ENV_LOCK.lock().unwrap();
        const OLD_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        const NEW_KEY: &str = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";

        std::env::set_var("KV_AT_REST_KEY", OLD_KEY);
        std::env::remove_var("KV_AT_REST_OLD_KEYS");
        let old_ciphertext = encrypt(b"rotation-check").unwrap();

        std::env::set_var("KV_AT_REST_KEY", NEW_KEY);
        std::env::set_var("KV_AT_REST_OLD_KEYS", OLD_KEY);
        assert_eq!(decrypt(&old_ciphertext).unwrap(), b"rotation-check");

        let rotated = reencrypt(&old_ciphertext).unwrap();
        assert_eq!(decrypt(&rotated).unwrap(), b"rotation-check");

        std::env::remove_var("KV_AT_REST_OLD_KEYS");
        assert!(decrypt(&rotated).is_ok());
        assert!(decrypt(&old_ciphertext).is_err());
        std::env::remove_var("KV_AT_REST_KEY");
    }
}
