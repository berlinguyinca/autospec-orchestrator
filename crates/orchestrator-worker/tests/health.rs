use orchestrator_core::FailureClass;
use orchestrator_worker::{HealthAssessment, HealthMonitor};
use std::time::{Duration, Instant};

#[test]
fn warns_once_at_half_inactivity_and_resets_on_event() {
    let start = Instant::now();
    let mut monitor = HealthMonitor::default_at(start);
    assert_eq!(
        monitor.assess(start + Duration::from_secs(449), 0.0),
        HealthAssessment::Healthy
    );
    assert_eq!(
        monitor.assess(start + Duration::from_secs(450), 0.0),
        HealthAssessment::InactiveWarning { seconds: 450 }
    );
    assert_eq!(
        monitor.assess(start + Duration::from_secs(451), 0.0),
        HealthAssessment::Healthy
    );
    monitor.record_event(start + Duration::from_secs(500));
    assert_eq!(
        monitor.assess(start + Duration::from_secs(950), 0.0),
        HealthAssessment::InactiveWarning { seconds: 450 }
    );
}

#[test]
fn classifies_inactivity_and_wall_clock_without_retry_decisions() {
    let start = Instant::now();
    let mut monitor = HealthMonitor::default_at(start);
    assert_eq!(
        monitor.assess(start + Duration::from_secs(901), 0.0),
        HealthAssessment::Failed(FailureClass::Inactivity)
    );
    let mut monitor = HealthMonitor::default_at(start);
    monitor.record_event(start + Duration::from_secs(14_000));
    assert_eq!(
        monitor.assess(start + Duration::from_secs(14_401), 0.0),
        HealthAssessment::Failed(FailureClass::Timeout)
    );
}

#[test]
fn classifies_only_sustained_cpu_saturation() {
    let start = Instant::now();
    let mut monitor = HealthMonitor::new_at(
        start,
        Duration::from_secs(10_000),
        Duration::from_secs(20_000),
        Duration::from_secs(100),
    );
    assert_eq!(monitor.assess(start, 96.0), HealthAssessment::Healthy);
    assert_eq!(
        monitor.assess(start + Duration::from_secs(99), 96.0),
        HealthAssessment::Healthy
    );
    assert_eq!(
        monitor.assess(start + Duration::from_secs(100), 94.9),
        HealthAssessment::Healthy
    );
    assert_eq!(
        monitor.assess(start + Duration::from_secs(200), 100.0),
        HealthAssessment::Healthy
    );
    assert_eq!(
        monitor.assess(start + Duration::from_secs(300), 100.0),
        HealthAssessment::Failed(FailureClass::ResourceViolation)
    );
}
