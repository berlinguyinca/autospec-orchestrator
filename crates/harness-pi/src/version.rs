//! Pi version matrix (spec s3 Pi versions, s5 R-ORCH).
//!
//! Only the Pi releases whose CLI flags and session record format were tested
//! may run unattended. Anything else is refused unless the operator opts in.

use harness_traits::HarnessError;

/// Pi releases exercised by this harness's tests. 0.85 and 0.86 are untested.
pub const TESTED_PI_VERSIONS: [&str; 3] = ["0.84.3", "0.87.0", "0.87.1"];

/// Extracts the first `MAJOR.MINOR.PATCH` token from `pi --version` output.
///
/// A token is whitespace-delimited, may carry a leading `v`, and must be exactly
/// three dot-separated runs of ASCII digits. Prerelease or build suffixes
/// (`0.84.3-beta`) and anything else are not versions, so garbage yields `None`.
pub fn parse_pi_version(stdout: &str) -> Option<String> {
    stdout.split_whitespace().find_map(|token| {
        let token = token.strip_prefix('v').unwrap_or(token);
        let mut parts = token.split('.');
        let numeric = |part: Option<&str>| {
            part.filter(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
                .is_some()
        };
        let valid = numeric(parts.next())
            && numeric(parts.next())
            && numeric(parts.next())
            && parts.next().is_none();
        valid.then(|| token.to_owned())
    })
}

/// Accepts a tested version, or any parseable version when `allow_untested` is
/// set (with a warning). An unparseable version is always an error.
pub fn check_pi_version(found: Option<&str>, allow_untested: bool) -> Result<(), HarnessError> {
    let Some(version) = found else {
        return Err(HarnessError::Start(
            "could not determine the Pi version from `pi --version`".to_owned(),
        ));
    };
    if TESTED_PI_VERSIONS.contains(&version) {
        return Ok(());
    }
    if allow_untested {
        tracing::warn!(
            pi_version = version,
            tested = ?TESTED_PI_VERSIONS,
            "running an untested Pi version because allow_untested_pi_version is set"
        );
        return Ok(());
    }
    Err(HarnessError::Start(format!(
        "Pi version {version} is outside the tested matrix {TESTED_PI_VERSIONS:?}; \
         set allow_untested_pi_version to run it anyway"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tested_versions_pass_without_the_opt_in() {
        for version in TESTED_PI_VERSIONS {
            assert!(check_pi_version(Some(version), false).is_ok(), "{version}");
        }
    }

    #[test]
    fn an_untested_version_is_refused_by_name_unless_allowed() {
        for version in ["0.86.0", "0.85.1", "0.87.2", "1.0.0"] {
            match check_pi_version(Some(version), false) {
                Err(HarnessError::Start(message)) => {
                    assert!(message.contains(version), "{message}")
                }
                other => panic!("{version}: expected Start error, got {other:?}"),
            }
            assert!(check_pi_version(Some(version), true).is_ok(), "{version}");
        }
    }

    #[test]
    fn an_unparseable_version_is_an_error_even_when_untested_versions_are_allowed() {
        assert!(matches!(
            check_pi_version(None, true),
            Err(HarnessError::Start(_))
        ));
        assert!(matches!(
            check_pi_version(None, false),
            Err(HarnessError::Start(_))
        ));
    }

    #[test]
    fn parse_finds_the_first_version_token() {
        assert_eq!(parse_pi_version("pi 0.84.3\n").as_deref(), Some("0.84.3"));
        assert_eq!(parse_pi_version("0.87.1").as_deref(), Some("0.87.1"));
        assert_eq!(
            parse_pi_version("pi v0.87.0 (abc)").as_deref(),
            Some("0.87.0")
        );
        assert_eq!(parse_pi_version("1.2.3 4.5.6").as_deref(), Some("1.2.3"));
    }

    #[test]
    fn parse_rejects_garbage() {
        for text in [
            "",
            "\n",
            "pi",
            "0.84",
            "0.84.3.1",
            "0.84.3-beta",
            "a.b.c",
            "0..3",
            ".84.3",
            "0.84.3x",
            "error: unknown flag --version",
        ] {
            assert_eq!(parse_pi_version(text), None, "{text:?}");
        }
    }
}
