use crate::StorageError;
use std::{
    ffi::OsString,
    fmt,
    path::PathBuf,
    process::{Command, Stdio},
};

#[derive(Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub program: PathBuf,
    pub arguments: Vec<OsString>,
}

impl CommandSpec {
    pub fn new(program: impl Into<PathBuf>, arguments: impl IntoIterator<Item = OsString>) -> Self {
        Self {
            program: program.into(),
            arguments: arguments.into_iter().collect(),
        }
    }
}

impl fmt::Debug for CommandSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandSpec")
            .field("program", &self.program)
            .field("arguments", &self.arguments)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    pub code: i32,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

pub trait CommandRunner: fmt::Debug + Send + Sync {
    fn run(&self, command: &CommandSpec) -> Result<CommandOutput, StorageError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessCommandRunner;

impl CommandRunner for ProcessCommandRunner {
    fn run(&self, command: &CommandSpec) -> Result<CommandOutput, StorageError> {
        if !command.program.is_absolute() {
            return Err(StorageError::Command(format!(
                "command path is not absolute: {}",
                command.program.display()
            )));
        }
        let output = Command::new(&command.program)
            .args(&command.arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(|error| {
                StorageError::Command(format!("execute {}: {error}", command.program.display()))
            })?;
        Ok(CommandOutput {
            code: output.status.code().unwrap_or(-1),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

pub(crate) fn run_checked(
    runner: &dyn CommandRunner,
    command: CommandSpec,
    purpose: &str,
) -> Result<CommandOutput, StorageError> {
    let output = runner.run(&command)?;
    if output.code == 0 {
        Ok(output)
    } else {
        Err(StorageError::Command(format!(
            "{purpose} exited {}: {}",
            output.code,
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

pub(crate) fn args(values: &[&str]) -> Vec<OsString> {
    values.iter().map(OsString::from).collect()
}
