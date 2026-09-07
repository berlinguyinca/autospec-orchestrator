use runtime_apptainer::ApptainerRuntime;
use runtime_docker::DockerRuntime;
use runtime_podman::PodmanRuntime;
use runtime_traits::{inspect_runtime, Runtime, RuntimeAvailability, RUNTIME_CONFORMANCE_VERSION};

#[tokio::test]
async fn frozen_runtime_conformance_reports_every_adapter_and_explicit_unavailability() {
    let docker = DockerRuntime::connect(None).expect("construct Docker adapter");
    let podman = PodmanRuntime::default();
    let apptainer = ApptainerRuntime::default();
    let runtimes: [(&dyn Runtime, runtime_traits::RuntimeConformanceMetadata); 3] = [
        (&docker, runtime_docker::CONFORMANCE_METADATA),
        (&podman, runtime_podman::CONFORMANCE_METADATA),
        (&apptainer, runtime_apptainer::CONFORMANCE_METADATA),
    ];

    for (runtime, metadata) in runtimes {
        let report = inspect_runtime(runtime, metadata).await;
        assert_eq!(report.contract, RUNTIME_CONFORMANCE_VERSION);
        assert_eq!(report.runtime, runtime.name());
        match report.availability {
            RuntimeAvailability::Available => {
                assert_eq!(runtime.name(), "docker");
                println!(
                    "ELIGIBLE {RUNTIME_CONFORMANCE_VERSION}: {} lifecycle gate is required",
                    runtime.name()
                );
            }
            RuntimeAvailability::DetectedUnsupported => {
                println!(
                    "UNSUPPORTED {RUNTIME_CONFORMANCE_VERSION}: {} detected without lifecycle conformance",
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

    assert!(
        docker.available().await,
        "Docker is required for conformance"
    );
}
