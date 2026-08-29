use runtime_apptainer::ApptainerRuntime;
use runtime_docker::DockerRuntime;
use runtime_podman::PodmanRuntime;
use runtime_traits::{inspect_runtime, Runtime, RuntimeAvailability, RUNTIME_CONFORMANCE_VERSION};

#[tokio::test]
async fn frozen_runtime_conformance_reports_every_adapter_and_explicit_unavailability() {
    let docker = DockerRuntime::connect(None).expect("construct Docker adapter");
    let podman = PodmanRuntime::default();
    let apptainer = ApptainerRuntime::default();
    let runtimes: [&dyn Runtime; 3] = [&docker, &podman, &apptainer];

    for runtime in runtimes {
        let report = inspect_runtime(runtime).await;
        assert_eq!(report.contract, RUNTIME_CONFORMANCE_VERSION);
        assert_eq!(report.runtime, runtime.name());
        match report.availability {
            RuntimeAvailability::Available => {
                println!(
                    "PASS {RUNTIME_CONFORMANCE_VERSION}: {} available",
                    runtime.name()
                );
            }
            RuntimeAvailability::Unavailable => {
                println!(
                    "SKIP {RUNTIME_CONFORMANCE_VERSION}: {} unavailable",
                    runtime.name()
                );
            }
        }
    }
}
