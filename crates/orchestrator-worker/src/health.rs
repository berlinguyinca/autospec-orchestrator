use orchestrator_core::FailureClass;
use std::time::{Duration, Instant};

const CPU_SATURATION_PERCENT: f64 = 95.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthAssessment {
    Healthy,
    InactiveWarning { seconds: u64 },
    Failed(FailureClass),
}

/// Classifies execution health without deciding whether another attempt occurs
/// (spec sections 40 and 95).
#[derive(Debug, Clone)]
pub struct HealthMonitor {
    started_at: Instant,
    last_event_at: Instant,
    saturated_since: Option<Instant>,
    paused_at: Option<Instant>,
    warned_for_event_window: bool,
    inactivity: Duration,
    wall_clock: Duration,
    cpu_saturation: Duration,
}

impl Default for HealthMonitor {
    fn default() -> Self {
        Self::default_at(Instant::now())
    }
}

impl HealthMonitor {
    pub fn default_at(started_at: Instant) -> Self {
        Self::new_at(
            started_at,
            Duration::from_secs(900),
            Duration::from_secs(14_400),
            Duration::from_secs(3_600),
        )
    }

    pub fn new_at(
        started_at: Instant,
        inactivity: Duration,
        wall_clock: Duration,
        cpu_saturation: Duration,
    ) -> Self {
        Self {
            started_at,
            last_event_at: started_at,
            saturated_since: None,
            paused_at: None,
            warned_for_event_window: false,
            inactivity,
            wall_clock,
            cpu_saturation,
        }
    }

    pub fn record_event(&mut self, at: Instant) {
        self.last_event_at = at;
        self.warned_for_event_window = false;
    }

    pub fn pause(&mut self, at: Instant) {
        if self.paused_at.is_none() {
            self.paused_at = Some(at);
        }
    }

    pub fn resume(&mut self, at: Instant) {
        let Some(paused_at) = self.paused_at.take() else {
            return;
        };
        let suspended = at.saturating_duration_since(paused_at);
        self.started_at += suspended;
        self.last_event_at += suspended;
        if let Some(saturated_since) = &mut self.saturated_since {
            *saturated_since += suspended;
        }
    }

    pub fn assess(&mut self, now: Instant, cpu_percent: f64) -> HealthAssessment {
        if self.paused_at.is_some() {
            return HealthAssessment::Healthy;
        }
        if now.duration_since(self.started_at) > self.wall_clock {
            return HealthAssessment::Failed(FailureClass::Timeout);
        }
        let inactive_for = now.duration_since(self.last_event_at);
        if inactive_for > self.inactivity {
            return HealthAssessment::Failed(FailureClass::Inactivity);
        }
        if cpu_percent > CPU_SATURATION_PERCENT {
            let saturated_since = *self.saturated_since.get_or_insert(now);
            if now.duration_since(saturated_since) >= self.cpu_saturation {
                return HealthAssessment::Failed(FailureClass::ResourceViolation);
            }
        } else {
            self.saturated_since = None;
        }
        if !self.warned_for_event_window && inactive_for >= self.inactivity / 2 {
            self.warned_for_event_window = true;
            return HealthAssessment::InactiveWarning {
                seconds: inactive_for.as_secs(),
            };
        }
        HealthAssessment::Healthy
    }
}
