//! Pi 0.84.3 agent harness (spec sections 22, 38, 39, 41, 79, 95).

mod events;
mod resume;
mod session;

use async_trait::async_trait;
use harness_traits::{AgentHarness, HarnessError, SessionRef};
use orchestrator_core::{ExecutionEvent, ModelPolicy, OwnershipLabels, TaskPacket};
use std::{
    collections::HashMap,
    path::PathBuf,
    process::Child,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Debug, Clone)]
pub struct PiHarnessConfig {
    pub state_root: PathBuf,
    pub worktree: PathBuf,
    pub executable: PathBuf,
    pub labels: OwnershipLabels,
    /// AutoSpec's model policy, carried into Pi without choosing an alternative.
    pub model_policy: Option<ModelPolicy>,
    /// Exact Pi tool allowlist. Empty means Pi receives `--no-tools`.
    pub tools: Vec<String>,
    /// Additional explicit skill paths. Package discovery remains disabled.
    pub skills: Vec<PathBuf>,
    pub stop_timeout: Duration,
}

impl PiHarnessConfig {
    pub fn for_execution(
        state_root: PathBuf,
        worktree: PathBuf,
        executable: PathBuf,
        labels: OwnershipLabels,
    ) -> Self {
        Self {
            state_root,
            worktree,
            executable,
            labels,
            model_policy: None,
            tools: vec![
                "read".to_owned(),
                "bash".to_owned(),
                "edit".to_owned(),
                "write".to_owned(),
            ],
            skills: Vec::new(),
            stop_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Debug)]
struct ProcessRegistry(Mutex<HashMap<String, Child>>);

impl Drop for ProcessRegistry {
    fn drop(&mut self) {
        let Ok(children) = self.0.get_mut() else {
            return;
        };
        for child in children.values_mut() {
            let _ = session::kill_process_group(child.id());
            let _ = child.wait();
        }
    }
}

#[derive(Debug, Clone)]
pub struct PiHarness {
    config: PiHarnessConfig,
    processes: Arc<ProcessRegistry>,
    unknown_events: Arc<AtomicU64>,
}

impl PiHarness {
    pub fn new(config: PiHarnessConfig) -> Self {
        Self {
            config,
            processes: Arc::new(ProcessRegistry(Mutex::new(HashMap::new()))),
            unknown_events: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn config(&self) -> &PiHarnessConfig {
        &self.config
    }

    pub fn unknown_event_count(&self) -> u64 {
        self.unknown_events.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl AgentHarness for PiHarness {
    fn name(&self) -> &'static str {
        "pi"
    }

    async fn start(&self, packet: &TaskPacket) -> Result<SessionRef, HarnessError> {
        session::start(self, packet)
    }

    async fn resume(&self, session: &SessionRef) -> Result<(), HarnessError> {
        resume::resume(self, session)
    }

    async fn fork_conversation(&self, session: &SessionRef) -> Result<SessionRef, HarnessError> {
        resume::fork_conversation(self, session)
    }

    async fn poll_events(&self, session: &SessionRef) -> Result<Vec<ExecutionEvent>, HarnessError> {
        events::poll_events(self, session)
    }

    async fn stop(&self, session: &SessionRef) -> Result<(), HarnessError> {
        session::stop(self, session).await
    }
}
