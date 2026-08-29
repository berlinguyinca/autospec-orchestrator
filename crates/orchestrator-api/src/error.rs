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
        Self::store_with_execution(error, None)
    }

    pub(crate) fn store_for(error: StoreError, execution_id: &str) -> Self {
        Self::store_with_execution(error, Some(execution_id))
    }

    fn store_with_execution(error: StoreError, execution_id: Option<&str>) -> Self {
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
            other => {
                tracing::error!(execution_id, %other, "internal API persistence failure");
                Self::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "INTERNAL",
                    "internal server error",
                )
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn internal_store_failures_never_expose_database_diagnostics() {
        let response = ApiError::store(StoreError::SequenceConflict).into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({
                "error": {"code": "INTERNAL", "message": "internal server error"}
            })
        );
    }
}
