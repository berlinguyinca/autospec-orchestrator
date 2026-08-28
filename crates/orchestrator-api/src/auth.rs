use axum::http::HeaderMap;
use std::sync::Arc;

/// Pluggable API-token validation boundary. Production currently uses one
/// static bearer secret; external identity providers can implement the same
/// contract without changing route handlers.
pub trait ApiTokenValidator: Send + Sync {
    fn validate(&self, token: &[u8]) -> bool;
}

#[derive(Clone)]
pub struct StaticApiTokenValidator {
    expected: Arc<[u8]>,
}

impl StaticApiTokenValidator {
    pub fn new(token: String) -> Self {
        Self {
            expected: Arc::from(token.into_bytes()),
        }
    }
}

impl ApiTokenValidator for StaticApiTokenValidator {
    fn validate(&self, token: &[u8]) -> bool {
        constant_time_eq(token, &self.expected)
    }
}

pub(crate) fn authorize_bearer(headers: &HeaderMap, validator: &dyn ApiTokenValidator) -> bool {
    let supplied = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::as_bytes)
        .unwrap_or_default();
    validator.validate(supplied)
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max = left.len().max(right.len());
    let mut difference = left.len() ^ right.len();
    for index in 0..max {
        difference |= usize::from(
            left.get(index).copied().unwrap_or(0) ^ right.get(index).copied().unwrap_or(0),
        );
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_token_validator_rejects_prefix_suffix_and_length_mismatch() {
        let validator = StaticApiTokenValidator::new("secret-token".into());
        assert!(validator.validate(b"secret-token"));
        assert!(!validator.validate(b"secret"));
        assert!(!validator.validate(b"secret-token-extra"));
        assert!(!validator.validate(b"xsecret-token"));
    }
}
