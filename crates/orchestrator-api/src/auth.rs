use axum::http::HeaderMap;
use std::sync::Arc;

use crate::{error::ApiError, state::AppState};

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
    let Some(supplied) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty())
        .map(str::as_bytes)
    else {
        return false;
    };
    validator.validate(supplied)
}

pub(crate) fn authorize_api(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    authorize(
        headers,
        state.api_token_validator.as_ref(),
        "missing or invalid API bearer token",
    )
}

pub(crate) fn authorize_worker(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    authorize(
        headers,
        state.worker_token_validator.as_ref(),
        "missing or invalid worker bearer token",
    )
}

fn authorize(
    headers: &HeaderMap,
    validator: &dyn ApiTokenValidator,
    message: &'static str,
) -> Result<(), ApiError> {
    if authorize_bearer(headers, validator) {
        Ok(())
    } else {
        Err(ApiError::unauthorized(message))
    }
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingValidator(AtomicUsize);

    impl ApiTokenValidator for CountingValidator {
        fn validate(&self, _: &[u8]) -> bool {
            self.0.fetch_add(1, Ordering::SeqCst);
            true
        }
    }

    #[test]
    fn static_token_validator_rejects_prefix_suffix_and_length_mismatch() {
        let validator = StaticApiTokenValidator::new("secret-token".into());
        assert!(validator.validate(b"secret-token"));
        assert!(!validator.validate(b"secret"));
        assert!(!validator.validate(b"secret-token-extra"));
        assert!(!validator.validate(b"xsecret-token"));
    }

    #[test]
    fn missing_and_malformed_bearer_headers_fail_before_secret_validation() {
        let validator = CountingValidator(AtomicUsize::new(0));
        assert!(!authorize_bearer(&HeaderMap::new(), &validator));
        for value in ["", "secret", "Basic secret", "Bearer ", "bearer secret"] {
            let mut headers = HeaderMap::new();
            headers.insert(axum::http::header::AUTHORIZATION, value.parse().unwrap());
            assert!(!authorize_bearer(&headers, &validator), "{value:?}");
        }
        assert_eq!(validator.0.load(Ordering::SeqCst), 0);
    }
}
