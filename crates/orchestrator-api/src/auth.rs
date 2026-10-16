use axum::http::HeaderMap;
use orchestrator_core::WorkerId;
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, sync::Arc};

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

/// Who a worker bearer token speaks for (spec section 5 R-ORCH).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerIdentity {
    /// The legacy shared worker token: it speaks for every worker.
    Any,
    /// A per-worker token: it speaks for this worker only.
    Worker(WorkerId),
}

/// Resolves a presented worker bearer token to the identity it was issued to.
pub trait WorkerTokenValidator: Send + Sync {
    fn identify(&self, presented: &[u8]) -> Option<WorkerIdentity>;
}

/// The legacy single shared worker token. It identifies as [`WorkerIdentity::Any`].
#[derive(Clone)]
pub struct SharedWorkerToken(StaticApiTokenValidator);

impl SharedWorkerToken {
    pub fn new(token: String) -> Self {
        Self(StaticApiTokenValidator::new(token))
    }
}

impl WorkerTokenValidator for SharedWorkerToken {
    fn identify(&self, presented: &[u8]) -> Option<WorkerIdentity> {
        self.0.validate(presented).then_some(WorkerIdentity::Any)
    }
}

/// Per-worker token digests: one `<worker_id> <sha256-hex-of-token>` per line.
///
/// Only digests are stored. `identify` hashes the presented token and compares
/// every stored digest in constant time, so neither the token nor which entry
/// matched leaks through timing. Tokens and digests never appear in an error
/// or a `Debug` rendering.
#[derive(Clone)]
pub struct PerWorkerTokens {
    entries: Vec<(WorkerId, [u8; 32])>,
}

impl std::fmt::Debug for PerWorkerTokens {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "PerWorkerTokens({} workers)", self.entries.len())
    }
}

impl PerWorkerTokens {
    /// Parses the digest file. `#` starts a comment and blank lines are
    /// skipped. A malformed line, a repeated worker id or a repeated digest
    /// fails the whole load; the error names the line number only.
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut entries = Vec::new();
        let mut workers = BTreeSet::new();
        let mut digests = BTreeSet::new();
        for (index, raw) in text.lines().enumerate() {
            let number = index + 1;
            let line = raw.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            let mut fields = line.split_whitespace();
            let (Some(worker), Some(digest), None) = (fields.next(), fields.next(), fields.next())
            else {
                return Err(format!(
                    "worker token file line {number}: expected `<worker_id> <sha256-hex>`"
                ));
            };
            let digest = decode_digest(digest).ok_or_else(|| {
                format!("worker token file line {number}: digest is not 64 hexadecimal characters")
            })?;
            if !workers.insert(worker.to_owned()) {
                return Err(format!(
                    "worker token file line {number}: worker id appears more than once"
                ));
            }
            if !digests.insert(digest) {
                return Err(format!(
                    "worker token file line {number}: digest is shared by more than one worker"
                ));
            }
            entries.push((WorkerId::new(worker), digest));
        }
        if entries.is_empty() {
            return Err("worker token file lists no workers".to_owned());
        }
        Ok(Self { entries })
    }
}

impl WorkerTokenValidator for PerWorkerTokens {
    fn identify(&self, presented: &[u8]) -> Option<WorkerIdentity> {
        let digest: [u8; 32] = Sha256::digest(presented).into();
        let mut found = None;
        for (worker, expected) in &self.entries {
            if constant_time_eq(&digest, expected) {
                found = Some(WorkerIdentity::Worker(worker.clone()));
            }
        }
        found
    }
}

fn decode_digest(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (byte, pair) in out.iter_mut().zip(hex.as_bytes().chunks(2)) {
        let high = char::from(pair[0]).to_digit(16)?;
        let low = char::from(pair[1]).to_digit(16)?;
        *byte = u8::try_from(high * 16 + low).ok()?;
    }
    Some(out)
}

fn bearer_token(headers: &HeaderMap) -> Option<&[u8]> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty())
        .map(str::as_bytes)
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

/// Authenticates a worker bearer token and returns the identity it speaks for.
pub(crate) fn authorize_worker(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<WorkerIdentity, ApiError> {
    bearer_token(headers)
        .and_then(|token| state.worker_token_validator.identify(token))
        .ok_or_else(|| ApiError::unauthorized("missing or invalid worker bearer token"))
}

/// Which credential authorized a request that accepts either.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Caller {
    Api,
    Worker(WorkerIdentity),
}

/// Either credential may request post-merge cleanup: the API token (an
/// external client that decided a PR merged) or the worker token. Both
/// validators always run, so the answer's timing does not reveal which
/// credential was supplied.
pub(crate) fn authorize_api_or_worker(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Caller, ApiError> {
    let api = authorize_bearer(headers, state.api_token_validator.as_ref());
    let worker =
        bearer_token(headers).and_then(|token| state.worker_token_validator.identify(token));
    match (api, worker) {
        (true, _) => Ok(Caller::Api),
        (false, Some(identity)) => Ok(Caller::Worker(identity)),
        (false, None) => Err(ApiError::unauthorized(
            "missing or invalid API or worker bearer token",
        )),
    }
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

    fn digest_hex(token: &str) -> String {
        Sha256::digest(token.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn tokens(lines: &[(&str, &str)]) -> PerWorkerTokens {
        let text = lines
            .iter()
            .map(|(worker, token)| format!("{worker} {}", digest_hex(token)))
            .collect::<Vec<_>>()
            .join("\n");
        PerWorkerTokens::parse(&text).expect("valid token file")
    }

    #[test]
    fn a_per_worker_token_identifies_only_its_worker() {
        let tokens = tokens(&[("w1", "token-one"), ("w2", "token-two")]);
        assert_eq!(
            tokens.identify(b"token-one"),
            Some(WorkerIdentity::Worker(WorkerId::new("w1")))
        );
        assert_eq!(
            tokens.identify(b"token-two"),
            Some(WorkerIdentity::Worker(WorkerId::new("w2")))
        );
        assert_eq!(tokens.identify(b"unknown"), None);
        assert_eq!(tokens.identify(b"token-on"), None);
        assert_eq!(tokens.identify(b""), None);
    }

    #[test]
    fn the_shared_token_identifies_as_any() {
        let shared = SharedWorkerToken::new("shared".into());
        assert_eq!(shared.identify(b"shared"), Some(WorkerIdentity::Any));
        assert_eq!(shared.identify(b"other"), None);
    }

    #[test]
    fn parse_skips_comments_and_blank_lines_and_accepts_uppercase_hex() {
        let upper = digest_hex("a").to_uppercase();
        let text = format!("# issued 2026-10\n\nw1 {upper} # trailing comment\n   \n");
        let tokens = PerWorkerTokens::parse(&text).expect("parses");
        assert_eq!(
            tokens.identify(b"a"),
            Some(WorkerIdentity::Worker(WorkerId::new("w1")))
        );
    }

    #[test]
    fn parse_rejects_duplicates_empty_files_and_malformed_lines_without_leaking_digests() {
        let digest = digest_hex("secret-token");
        let other = digest_hex("other-token");
        let twice = PerWorkerTokens::parse(&format!("w1 {digest}\nw1 {other}\n"))
            .expect_err("two lines for w1");
        assert!(twice.contains("line 2"), "{twice}");
        let shared = PerWorkerTokens::parse(&format!("w1 {digest}\nw2 {digest}\n"))
            .expect_err("one digest for two workers");
        assert!(shared.contains("line 2"), "{shared}");
        assert!(PerWorkerTokens::parse("").is_err());
        assert!(PerWorkerTokens::parse("# only a comment\n").is_err());
        for (bad, line) in [
            (format!("w1 {digest}\nw2\n"), "line 2"),
            (format!("w1 {digest} extra\n"), "line 1"),
            (format!("w1 {}\n", &digest[..63]), "line 1"),
            (format!("w1 {}g\n", &digest[..63]), "line 1"),
            (format!("\n\nw1 {}\n", "z".repeat(64)), "line 3"),
        ] {
            let error = PerWorkerTokens::parse(&bad).expect_err("malformed line");
            assert!(error.contains(line), "{error}");
            for secret in [&digest, &other, &digest[..16].to_owned(), &"z".repeat(8)] {
                assert!(
                    !error.contains(secret.as_str()),
                    "error leaked digest: {error}"
                );
            }
        }
        let tokens = tokens(&[("w1", "secret-token")]);
        let rendered = format!("{tokens:?}");
        assert!(!rendered.contains(&digest[..16]), "{rendered}");
    }
}
