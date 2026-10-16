//! Disk byte and inode admission budget for the worker state root.
//!
//! Spec §5 R-BUILD: warn at 80 %, stop at 90 %, for bytes and inodes. A `Stop`
//! verdict is advertised as a health error, which makes the controller mark the
//! worker `Offline`, so no new execution is claimed. Running executions are not
//! touched. Every heartbeat re-samples, so admission resumes when space returns.

use std::{io, path::Path, sync::Arc};

/// Health code advertised when bytes are exhausted or below the reserve.
pub const DISK_PRESSURE_BYTES: &str = "disk-pressure-bytes";
/// Health code advertised when inodes are exhausted.
pub const DISK_PRESSURE_INODES: &str = "disk-pressure-inodes";

const GIB: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskSample {
    pub total_bytes: u64,
    pub free_bytes: u64,
    pub total_inodes: u64,
    pub free_inodes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskVerdict {
    Ok,
    Warn,
    Stop(&'static str),
}

/// True when `used / total >= percent / 100`. A zero total (filesystems that
/// do not report inodes) is never under pressure.
fn at_least_percent(total: u64, free: u64, percent: u8) -> bool {
    if total == 0 {
        return false;
    }
    let used = u128::from(total.saturating_sub(free));
    used * 100 >= u128::from(total) * u128::from(percent)
}

pub fn judge(s: &DiskSample, warn: u8, stop: u8, reserve_bytes: u64) -> DiskVerdict {
    if at_least_percent(s.total_bytes, s.free_bytes, stop)
        || (reserve_bytes > 0 && s.free_bytes < reserve_bytes)
    {
        return DiskVerdict::Stop(DISK_PRESSURE_BYTES);
    }
    if at_least_percent(s.total_inodes, s.free_inodes, stop) {
        return DiskVerdict::Stop(DISK_PRESSURE_INODES);
    }
    if at_least_percent(s.total_bytes, s.free_bytes, warn)
        || at_least_percent(s.total_inodes, s.free_inodes, warn)
    {
        return DiskVerdict::Warn;
    }
    DiskVerdict::Ok
}

/// Samples the filesystem holding `path` (space available to unprivileged users).
pub fn sample(path: &Path) -> io::Result<DiskSample> {
    let stat = rustix::fs::statvfs(path)?;
    let block = if stat.f_frsize > 0 {
        stat.f_frsize
    } else {
        stat.f_bsize
    };
    Ok(DiskSample {
        total_bytes: stat.f_blocks.saturating_mul(block),
        free_bytes: stat.f_bavail.saturating_mul(block),
        total_inodes: stat.f_files,
        free_inodes: stat.f_favail,
    })
}

/// Injectable sampler so the heartbeat path can be driven with a full disk.
pub type Sampler = Arc<dyn Fn(&Path) -> io::Result<DiskSample> + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskPolicy {
    pub warn_percent: u8,
    pub stop_percent: u8,
    pub reserve_bytes: u64,
}

impl DiskPolicy {
    pub fn new(warn_percent: u8, stop_percent: u8, reserve_gib: u64) -> anyhow::Result<Self> {
        anyhow::ensure!(
            (1..=100).contains(&stop_percent) && warn_percent <= stop_percent,
            "disk thresholds must satisfy 0 <= warn <= stop <= 100 (stop >= 1)"
        );
        let reserve_bytes = reserve_gib
            .checked_mul(GIB)
            .ok_or_else(|| anyhow::anyhow!("--disk-reserve-gib is too large"))?;
        Ok(Self {
            warn_percent,
            stop_percent,
            reserve_bytes,
        })
    }

    /// Samples `root` once and judges it. A failed sample is logged and is no
    /// pressure signal (`Ok`).
    pub fn assess(&self, root: &Path, sampler: &Sampler) -> DiskVerdict {
        match sampler(root) {
            Ok(sample) => {
                let verdict = judge(
                    &sample,
                    self.warn_percent,
                    self.stop_percent,
                    self.reserve_bytes,
                );
                match verdict {
                    DiskVerdict::Ok => {}
                    DiskVerdict::Warn => {
                        tracing::warn!(root = %root.display(), "state root disk usage above warn threshold");
                    }
                    DiskVerdict::Stop(reason) => {
                        tracing::warn!(root = %root.display(), reason, "state root under disk pressure; refusing new executions");
                    }
                }
                verdict
            }
            Err(error) => {
                tracing::warn!(root = %root.display(), %error, "disk sample failed");
                DiskVerdict::Ok
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_with(used_bytes_pct: u64, used_inodes_pct: u64) -> DiskSample {
        DiskSample {
            total_bytes: 1000,
            free_bytes: 1000 - used_bytes_pct * 10,
            total_inodes: 1000,
            free_inodes: 1000 - used_inodes_pct * 10,
        }
    }

    #[test]
    fn stops_at_ninety_percent_bytes() {
        assert_eq!(
            judge(&sample_with(90, 10), 80, 90, 0),
            DiskVerdict::Stop("disk-pressure-bytes")
        );
        assert_eq!(judge(&sample_with(89, 10), 80, 90, 0), DiskVerdict::Warn);
    }

    #[test]
    fn stops_on_inodes_with_free_bytes() {
        assert_eq!(
            judge(&sample_with(10, 95), 80, 90, 0),
            DiskVerdict::Stop("disk-pressure-inodes")
        );
    }

    #[test]
    fn warns_at_eighty_five_and_is_ok_at_fifty() {
        assert_eq!(judge(&sample_with(85, 10), 80, 90, 0), DiskVerdict::Warn);
        assert_eq!(judge(&sample_with(50, 50), 80, 90, 0), DiskVerdict::Ok);
    }

    #[test]
    fn reserve_stops_even_when_percent_is_low() {
        let s = DiskSample {
            total_bytes: 1000 * GIB,
            free_bytes: 300 * GIB,
            total_inodes: 10,
            free_inodes: 10,
        };
        assert_eq!(judge(&s, 80, 90, 0), DiskVerdict::Ok);
        assert_eq!(judge(&s, 80, 90, 300 * GIB), DiskVerdict::Ok);
        assert_eq!(
            judge(&s, 80, 90, 301 * GIB),
            DiskVerdict::Stop("disk-pressure-bytes")
        );
    }

    #[test]
    fn filesystems_without_inode_counts_are_not_inode_pressured() {
        let s = DiskSample {
            total_bytes: 1000,
            free_bytes: 900,
            total_inodes: 0,
            free_inodes: 0,
        };
        assert_eq!(judge(&s, 80, 90, 0), DiskVerdict::Ok);
    }

    #[test]
    fn real_statvfs_sample_of_a_tempdir_is_populated_and_consistent() {
        let dir = tempfile::tempdir().unwrap();
        let s = sample(dir.path()).unwrap();
        assert!(s.total_bytes > 0);
        assert!(s.free_bytes <= s.total_bytes);
        assert!(s.free_inodes <= s.total_inodes);
        assert!(sample(&dir.path().join("missing")).is_err());
        // Reserve larger than the whole filesystem must stop on the real sample.
        assert_eq!(
            judge(&s, 80, 90, s.total_bytes + 1),
            DiskVerdict::Stop(DISK_PRESSURE_BYTES)
        );
    }

    #[test]
    fn policy_rejects_inverted_or_out_of_range_thresholds() {
        assert!(DiskPolicy::new(80, 90, 0).is_ok());
        assert!(DiskPolicy::new(91, 90, 0).is_err());
        assert!(DiskPolicy::new(0, 0, 0).is_err());
        assert!(DiskPolicy::new(80, 101, 0).is_err());
        assert_eq!(DiskPolicy::new(80, 90, 2).unwrap().reserve_bytes, 2 * GIB);
    }

    #[test]
    fn policy_health_error_follows_the_injected_sampler() {
        let policy = DiskPolicy::new(80, 90, 0).unwrap();
        let full: Sampler = Arc::new(|_| Ok(sample_with(95, 10)));
        let free: Sampler = Arc::new(|_| Ok(sample_with(10, 10)));
        let broken: Sampler = Arc::new(|_| Err(io::Error::other("boom")));
        let root = Path::new("/x");
        assert_eq!(
            policy.assess(root, &full),
            DiskVerdict::Stop(DISK_PRESSURE_BYTES)
        );
        assert_eq!(policy.assess(root, &free), DiskVerdict::Ok);
        assert_eq!(policy.assess(root, &broken), DiskVerdict::Ok);
    }
}
