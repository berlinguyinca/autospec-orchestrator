use std::env;
use std::process::Command;

/// Construct the only Git process boundary used by this crate.
///
/// Clearing the environment prevents caller-controlled `GIT_*` repository,
/// object, index, namespace, and config routing from escaping ownership checks.
pub(crate) fn git_command() -> Command {
    let path = env::var_os("PATH");
    let mut command = Command::new("git");
    command
        .env_clear()
        .env("GIT_ATTR_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_PAGER", "cat")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("HOME", "/dev/null")
        .env("LC_ALL", "C");
    if let Some(path) = path {
        command.env("PATH", path);
    }
    command
}
