use axum::{
    body::Body,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use orchestrator_persistence::StoreError;
use serde_json::json;

pub(crate) struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    pub(crate) fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    pub(crate) fn validation(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "VALIDATION", message)
    }

    pub(crate) fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "UNAUTHORIZED", message)
    }

    pub(crate) fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "CONFLICT", message)
    }

    pub(crate) fn store(error: StoreError) -> Self {
        match error {
            StoreError::NotFound(message) => Self::new(StatusCode::NOT_FOUND, "NOT_FOUND", message),
            StoreError::IllegalTransition { from, to } => Self::new(
                StatusCode::CONFLICT,
                "ILLEGAL_TRANSITION",
                format!("illegal state transition: {from:?} -> {to:?}"),
            ),
            StoreError::IdempotencyConflict(message) => {
                Self::new(StatusCode::CONFLICT, "IDEMPOTENCY_CONFLICT", message)
            }
            StoreError::DuplicateExecutionId(message) => {
                Self::new(StatusCode::CONFLICT, "DUPLICATE_EXECUTION_ID", message)
            }
            StoreError::InvalidArtifactName(message) => Self::validation(message),
            StoreError::Conflict(message) => Self::conflict(message),
            other => Self::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL",
                other.to_string(),
            ),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response<Body> {
        (
            self.status,
            Json(json!({"error": {"code": self.code, "message": self.message}})),
        )
            .into_response()
    }
}
