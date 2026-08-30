/* Docker system prune is forbidden; this comment documents the boundary. */
fn remove_exact_container() {
    std::process::Command::new("docker").args(["rm", "autospec-execution-agent"]);
}
