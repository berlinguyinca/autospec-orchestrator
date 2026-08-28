//! Structured, correlation-aware logging (spec sections 37, 72, 99).

use crate::Execution;
use serde_json::{Map, Number, Value};
use std::{collections::BTreeMap, fmt};
use tracing::{
    field::{Field, Visit},
    span::{Attributes, Id, Record},
    Event, Subscriber,
};
use tracing_subscriber::{
    fmt::{
        format::{FormatEvent, FormatFields, Writer},
        FmtContext, MakeWriter,
    },
    layer::{Context, SubscriberExt},
    registry::LookupSpan,
    EnvFilter, Layer, Registry,
};

#[derive(Clone, Default)]
struct CorrelationFields(BTreeMap<String, Value>);

#[derive(Default)]
struct CorrelationLayer;

impl<S> Layer<S> for CorrelationLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, context: Context<'_, S>) {
        let Some(span) = context.span(id) else {
            return;
        };
        let mut fields = span
            .parent()
            .and_then(|parent| parent.extensions().get::<CorrelationFields>().cloned())
            .unwrap_or_default();
        attributes.record(&mut JsonVisitor(&mut fields.0));
        span.extensions_mut().insert(fields);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, context: Context<'_, S>) {
        let Some(span) = context.span(id) else {
            return;
        };
        let mut extensions = span.extensions_mut();
        if let Some(fields) = extensions.get_mut::<CorrelationFields>() {
            values.record(&mut JsonVisitor(&mut fields.0));
        }
    }
}

struct JsonVisitor<'a>(&'a mut BTreeMap<String, Value>);

impl JsonVisitor<'_> {
    fn insert(&mut self, field: &Field, value: Value) {
        if !is_secret_field_name(field.name()) {
            self.0.insert(field.name().to_owned(), value);
        }
    }
}

impl Visit for JsonVisitor<'_> {
    fn record_f64(&mut self, field: &Field, value: f64) {
        if let Some(number) = Number::from_f64(value) {
            self.insert(field, Value::Number(number));
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.insert(field, Value::Number(value.into()));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.insert(field, Value::Number(value.into()));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.insert(field, Value::Bool(value));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.insert(field, Value::String(value.to_owned()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.insert(field, Value::String(format!("{value:?}")));
    }
}

#[derive(Clone)]
struct CorrelatedJson {
    service: String,
}

impl<S, N> FormatEvent<S, N> for CorrelatedJson
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        context: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let mut fields = BTreeMap::new();
        event.record(&mut JsonVisitor(&mut fields));
        if let Some(span) = context.lookup_current() {
            if let Some(correlation) = span.extensions().get::<CorrelationFields>() {
                fields.extend(correlation.0.clone());
            }
        }
        fields.insert("service".to_owned(), Value::String(self.service.clone()));
        fields.insert(
            "level".to_owned(),
            Value::String(event.metadata().level().to_string()),
        );
        fields.insert(
            "target".to_owned(),
            Value::String(event.metadata().target().to_owned()),
        );
        let object: Map<String, Value> = fields.into_iter().collect();
        writeln!(
            writer,
            "{}",
            serde_json::to_string(&object).map_err(|_| fmt::Error)?
        )
    }
}

fn is_secret_field_name(name: &str) -> bool {
    let normalized: String = name
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect();
    ["token", "secret", "password", "apikey"]
        .iter()
        .any(|sensitive| normalized.contains(sensitive))
}

pub(crate) fn telemetry_subscriber<W>(service: &str, writer: W) -> impl Subscriber + Send + Sync
where
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
{
    Registry::default()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(CorrelationLayer)
        .with(
            tracing_subscriber::fmt::layer()
                .event_format(CorrelatedJson {
                    service: service.to_owned(),
                })
                .with_writer(writer),
        )
}

/// Installs the process-wide JSON subscriber used by controllers and workers.
pub fn init_tracing(service: &str) {
    tracing::subscriber::set_global_default(telemetry_subscriber(service, std::io::stdout))
        .expect("global tracing subscriber is already initialized");
}

/// Creates a span with the shared execution correlation vocabulary.
pub fn execution_span(execution: &Execution) -> tracing::Span {
    let task = execution.manifest.task.as_ref();
    let project_id = task.map_or("", |task| task.project_id.as_str());
    let issue_id = task.map_or("", |task| task.issue_id.as_str());
    let task_id = task.and_then(|task| task.task_id.as_deref()).unwrap_or("");
    let attempt_id = execution.attempt_id.as_ref().map_or("", |id| id.as_str());
    let session_id = execution.session_id.as_ref().map_or("", |id| id.as_str());
    let worker_id = execution.worker_id.as_ref().map_or("", |id| id.as_str());
    tracing::info_span!(
        "execution",
        project_id,
        issue_id,
        task_id,
        execution_id = %execution.id,
        attempt_id,
        session_id,
        worker_id,
    )
}

#[cfg(test)]
mod tests {
    use super::{execution_span, telemetry_subscriber};
    use crate::{
        AgentAssignment, Execution, ExecutionId, ExecutionManifest, ExecutionState, HarnessKind,
        ModelPolicy, OwnershipLabels, PersistenceMode, RepositoryReference, Role,
        RuntimeRequirement, TaskReference,
    };
    use chrono::Utc;
    use std::{
        io,
        sync::{Arc, Mutex},
    };
    use tracing::subscriber::with_default;

    #[derive(Clone, Default)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);

    struct BufferWriter(Buffer);

    impl io::Write for BufferWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0
                 .0
                .lock()
                .expect("buffer lock")
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buffer {
        type Writer = BufferWriter;

        fn make_writer(&'a self) -> Self::Writer {
            BufferWriter(self.clone())
        }
    }

    fn execution() -> Execution {
        Execution {
            id: ExecutionId::from("node-417-impl-01"),
            role: Role::Implementation,
            state: ExecutionState::Running,
            manifest: ExecutionManifest {
                api_version: crate::MANIFEST_API_VERSION.to_owned(),
                role: Role::Implementation,
                task: Some(TaskReference {
                    project_id: "inferweave".to_owned(),
                    issue_id: "417".to_owned(),
                    task_id: Some("reservation-cancellation".to_owned()),
                }),
                repository: RepositoryReference {
                    repo: "InferWeave/inferweave-node".to_owned(),
                    base_ref: "main".to_owned(),
                    base_sha: None,
                    branch: None,
                },
                agent: AgentAssignment {
                    harness: HarnessKind::Pi,
                    model_policy: ModelPolicy {
                        provider: "inferweave".to_owned(),
                        preferred: vec!["qwen3.8-27b".to_owned()],
                        alternatives: Vec::new(),
                        fallback_class: None,
                    },
                },
                runtime: RuntimeRequirement::default(),
                services: Vec::new(),
                persistence: PersistenceMode::Ephemeral,
                task_packet: None,
            },
            worker_id: None,
            attempt_id: None,
            session_id: None,
            worktree_path: None,
            labels: OwnershipLabels {
                execution_id: ExecutionId::from("node-417-impl-01"),
                worker_id: crate::WorkerId::from("buildbox-02"),
                repository: "InferWeave/inferweave-node".to_owned(),
                issue: Some("417".to_owned()),
            },
            created_at: Utc::now(),
            updated_at: Utc::now(),
            result: None,
        }
    }

    #[test]
    fn execution_logs_flatten_correlation_fields_and_drop_secret_fields() {
        let output = Buffer::default();
        let subscriber = telemetry_subscriber("test-service", output.clone());

        with_default(subscriber, || {
            execution_span(&execution()).in_scope(|| {
                tracing::info_span!("credentials", github_token = "never-log-this").in_scope(
                    || {
                        tracing::info!(password = "also-secret", action = "started");
                    },
                );
            });
        });

        let bytes = output.0.lock().expect("buffer lock").clone();
        let line: serde_json::Value =
            serde_json::from_slice(&bytes).expect("one JSON log line was emitted");
        assert_eq!(line["service"], "test-service");
        assert_eq!(line["project_id"], "inferweave");
        assert_eq!(line["issue_id"], "417");
        assert_eq!(line["task_id"], "reservation-cancellation");
        assert_eq!(line["execution_id"], "node-417-impl-01");
        assert!(line.get("model_id").is_none());
        assert_eq!(line["action"], "started");
        let rendered = String::from_utf8(bytes).expect("UTF-8 log output");
        assert!(!rendered.contains("github_token"));
        assert!(!rendered.contains("never-log-this"));
        assert!(!rendered.contains("password"));
        assert!(!rendered.contains("also-secret"));
        assert!(!rendered.contains("qwen3.8-27b"));
    }

    #[test]
    fn secret_field_normalization_covers_api_key_spellings() {
        let output = Buffer::default();
        let subscriber = telemetry_subscriber("test-service", output.clone());

        with_default(subscriber, || {
            tracing::info!(
                apiKey = "camel-secret",
                apikey = "compact-secret",
                api_key = "snake-secret",
                "api-key" = "kebab-secret",
                action = "safe"
            );
        });

        let rendered = String::from_utf8(output.0.lock().expect("buffer lock").clone())
            .expect("UTF-8 log output");
        assert!(rendered.contains("safe"));
        for secret in [
            "camel-secret",
            "compact-secret",
            "snake-secret",
            "kebab-secret",
        ] {
            assert!(!rendered.contains(secret));
        }
    }
}
