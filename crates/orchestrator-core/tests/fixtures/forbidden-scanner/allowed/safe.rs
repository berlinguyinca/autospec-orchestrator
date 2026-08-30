/* Docker system prune is forbidden; this comment documents the boundary. */
// Command::new("docker").args(["image", "prune"]);
fn remove_exact_container() {
    std::process::Command::new("docker").args(["rm", "autospec-execution-agent"]);
}

fn validate_forbidden_arguments(arguments: &[&str]) -> bool {
    const GLOBAL_CLEANUP: [&str; 2] = ["system", "prune"];
    arguments == GLOBAL_CLEANUP
}

fn validation_example() -> &'static str {
    r#"Command::new("docker").args(["container", "prune"])"#
}
