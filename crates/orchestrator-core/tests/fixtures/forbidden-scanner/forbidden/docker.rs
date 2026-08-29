fn forbidden_cleanup() {
    std::process::Command::new("docker")
        .args(["container", "prune", "--force"]);
}
