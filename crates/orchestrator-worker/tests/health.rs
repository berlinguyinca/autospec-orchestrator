use orchestrator_core::FailureClass;
use orchestrator_worker::{HealthAssessment, HealthMonitor, ResourceSample};
use std::time::{Duration, Instant};

fn cpu(cpu_percent: f64) -> ResourceSample {
    ResourceSample {
        cpu_percent,
        ..Default::default()
    }
}

fn io(cpu_percent: f64, io_bytes: Option<u64>) -> ResourceSample {
    ResourceSample {
        cpu_percent,
        io_bytes,
        ..Default::default()
    }
}

#[test]
fn warns_once_at_half_inactivity_and_resets_on_event() {
    let start = Instant::now();
    let mut monitor = HealthMonitor::default_at(start);
    assert_eq!(
        monitor.assess(start + Duration::from_secs(449), &cpu(0.0)),
        HealthAssessment::Healthy
    );
    assert_eq!(
        monitor.assess(start + Duration::from_secs(450), &cpu(0.0)),
        HealthAssessment::InactiveWarning { seconds: 450 }
    );
    assert_eq!(
        monitor.assess(start + Duration::from_secs(451), &cpu(0.0)),
        HealthAssessment::Healthy
    );
    monitor.record_event(start + Duration::from_secs(500));
    assert_eq!(
        monitor.assess(start + Duration::from_secs(950), &cpu(0.0)),
        HealthAssessment::InactiveWarning { seconds: 450 }
    );
}

#[test]
fn classifies_inactivity_and_wall_clock_without_retry_decisions() {
    let start = Instant::now();
    let mut monitor = HealthMonitor::default_at(start);
    assert_eq!(
        monitor.assess(start + Duration::from_secs(901), &cpu(0.0)),
        HealthAssessment::Failed(FailureClass::Inactivity)
    );
    let mut monitor = HealthMonitor::default_at(start);
    monitor.record_event(start + Duration::from_secs(14_000));
    assert_eq!(
        monitor.assess(start + Duration::from_secs(14_401), &cpu(0.0)),
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
    assert_eq!(monitor.assess(start, &cpu(96.0)), HealthAssessment::Healthy);
    assert_eq!(
        monitor.assess(start + Duration::from_secs(99), &cpu(96.0)),
        HealthAssessment::Healthy
    );
    assert_eq!(
        monitor.assess(start + Duration::from_secs(100), &cpu(94.9)),
        HealthAssessment::Healthy
    );
    assert_eq!(
        monitor.assess(start + Duration::from_secs(200), &cpu(100.0)),
        HealthAssessment::Healthy
    );
    assert_eq!(
        monitor.assess(start + Duration::from_secs(300), &cpu(100.0)),
        HealthAssessment::Failed(FailureClass::ResourceViolation)
    );
}

#[test]
fn human_pause_suspends_inactivity_wall_clock_and_cpu_windows() {
    let start = Instant::now();
    let mut monitor = HealthMonitor::new_at(
        start,
        Duration::from_secs(100),
        Duration::from_secs(200),
        Duration::from_secs(50),
    );
    assert_eq!(
        monitor.assess(start + Duration::from_secs(20), &cpu(100.0)),
        HealthAssessment::Healthy
    );
    monitor.pause(start + Duration::from_secs(30));
    assert_eq!(
        monitor.assess(start + Duration::from_secs(10_000), &cpu(100.0)),
        HealthAssessment::Healthy
    );
    monitor.resume(start + Duration::from_secs(10_030));
    assert_eq!(
        monitor.assess(start + Duration::from_secs(10_049), &cpu(100.0)),
        HealthAssessment::Healthy
    );
    assert_eq!(
        monitor.assess(start + Duration::from_secs(10_080), &cpu(0.0)),
        HealthAssessment::InactiveWarning { seconds: 80 }
    );
}

#[test]
fn repeated_human_pauses_are_idempotent_and_accumulate_suspended_time() {
    let start = Instant::now();
    let mut monitor = HealthMonitor::new_at(
        start,
        Duration::from_secs(100),
        Duration::from_secs(200),
        Duration::from_secs(50),
    );
    monitor.pause(start + Duration::from_secs(10));
    monitor.pause(start + Duration::from_secs(20));
    monitor.resume(start + Duration::from_secs(1_010));
    monitor.resume(start + Duration::from_secs(1_020));
    monitor.pause(start + Duration::from_secs(1_020));
    monitor.resume(start + Duration::from_secs(2_020));
    assert_eq!(
        monitor.assess(start + Duration::from_secs(2_099), &cpu(0.0)),
        HealthAssessment::InactiveWarning { seconds: 99 }
    );
    assert_eq!(
        monitor.assess(start + Duration::from_secs(2_101), &cpu(0.0)),
        HealthAssessment::Failed(FailureClass::Inactivity)
    );
}

fn secs(start: Instant, n: u64) -> Instant {
    start + Duration::from_secs(n)
}

#[test]
fn rising_io_with_no_events_is_quiet_healthy_not_inactivity() {
    let start = Instant::now();
    let mut monitor = HealthMonitor::default_at(start);
    monitor.assess(secs(start, 1), &io(0.0, Some(1_000)));
    assert_eq!(
        monitor.assess(secs(start, 901), &io(0.0, Some(2_000))),
        HealthAssessment::QuietHealthy { quiet_seconds: 901 }
    );
}

#[test]
fn flat_io_at_99_percent_cpu_is_inactivity() {
    let start = Instant::now();
    let mut monitor = HealthMonitor::default_at(start);
    monitor.assess(secs(start, 1), &io(99.0, Some(5_000)));
    assert_eq!(
        monitor.assess(secs(start, 901), &io(99.0, Some(5_000))),
        HealthAssessment::Failed(FailureClass::Inactivity)
    );
}

#[test]
fn unknown_io_is_inactivity_not_progress() {
    let start = Instant::now();
    let mut monitor = HealthMonitor::default_at(start);
    monitor.assess(secs(start, 1), &io(0.0, Some(5_000)));
    assert_eq!(
        monitor.assess(secs(start, 901), &io(0.0, None)),
        HealthAssessment::Failed(FailureClass::Inactivity)
    );
}

#[test]
fn a_single_first_sample_is_a_baseline_not_progress() {
    let start = Instant::now();
    let mut monitor = HealthMonitor::default_at(start);
    assert_eq!(
        monitor.assess(secs(start, 901), &io(0.0, Some(5_000))),
        HealthAssessment::Failed(FailureClass::Inactivity)
    );
}

#[test]
fn rising_io_cannot_outlive_the_wall_clock_limit() {
    let start = Instant::now();
    let mut monitor = HealthMonitor::default_at(start);
    monitor.assess(secs(start, 14_000), &io(0.0, Some(1)));
    assert_eq!(
        monitor.assess(secs(start, 14_401), &io(0.0, Some(2))),
        HealthAssessment::Failed(FailureClass::Timeout)
    );
}

#[test]
fn quiet_execution_fails_once_io_goes_flat() {
    let start = Instant::now();
    let mut monitor = HealthMonitor::default_at(start);
    monitor.assess(secs(start, 1), &io(0.0, Some(1_000)));
    monitor.assess(secs(start, 800), &io(0.0, Some(2_000)));
    // Progress at 800 s extends the deadline to 1700 s, not beyond.
    assert_eq!(
        monitor.assess(secs(start, 1_700), &io(0.0, Some(2_000))),
        HealthAssessment::InactiveWarning { seconds: 900 }
    );
    assert_eq!(
        monitor.assess(secs(start, 1_701), &io(0.0, Some(2_000))),
        HealthAssessment::Failed(FailureClass::Inactivity)
    );
}

#[test]
fn counter_reset_is_not_progress() {
    let start = Instant::now();
    let mut monitor = HealthMonitor::default_at(start);
    monitor.assess(secs(start, 1), &io(0.0, Some(9_000)));
    assert_eq!(
        monitor.assess(secs(start, 901), &io(0.0, Some(10))),
        HealthAssessment::Failed(FailureClass::Inactivity)
    );
}

#[test]
fn io_progress_is_suspended_with_the_clock_while_paused() {
    let start = Instant::now();
    let mut monitor = HealthMonitor::new_at(
        start,
        Duration::from_secs(100),
        Duration::from_secs(100_000),
        Duration::from_secs(50_000),
    );
    monitor.assess(secs(start, 1), &io(0.0, Some(10)));
    monitor.assess(secs(start, 90), &io(0.0, Some(20)));
    monitor.pause(secs(start, 95));
    monitor.resume(secs(start, 5_095));
    assert_eq!(
        monitor.assess(secs(start, 5_100), &io(0.0, Some(20))),
        HealthAssessment::Healthy
    );
    assert_eq!(
        monitor.assess(secs(start, 5_196), &io(0.0, Some(20))),
        HealthAssessment::Failed(FailureClass::Inactivity)
    );
}
