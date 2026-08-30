fn forbidden_cleanup(binary: &str) {
    std::process::Command::new(binary)
        .args(["container", "prune", "--force"]);
}
