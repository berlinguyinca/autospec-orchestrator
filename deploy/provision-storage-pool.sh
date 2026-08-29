#!/bin/sh
set -eu

usage() {
  echo "usage: $0 <plan|probe|apply> <apfs|lvm> <pool> <absolute-state-root>" >&2
  exit 64
}

[ "$#" -eq 4 ] || usage
action=$1
kind=$2
pool=$3
state_root=$4

case "$action" in plan|probe|apply) ;; *) usage ;; esac
case "$kind" in apfs|lvm) ;; *) usage ;; esac
case "$pool" in ""|*[!A-Za-z0-9_./:-]*) echo "pool contains unsupported characters" >&2; exit 64 ;; esac
case "$state_root" in /*) ;; *) echo "state root must be absolute" >&2; exit 64 ;; esac
case "$state_root" in *".."*) echo "state root contains an unsafe component" >&2; exit 64 ;; esac

confirm="provision:${kind}:${pool}:${state_root}"

plan() {
  echo "State root: ${state_root}"
  echo "Create the owner-only state root on the already provisioned execution-storage pool."
  if [ "$kind" = apfs ]; then
    echo "AUTOSPEC_STORAGE_KIND=apfs"
    echo "AUTOSPEC_APFS_PROBE_PATH=${pool}"
    echo "AUTOSPEC_STORAGE_POOL=${pool}"
    echo "Probe: diskutil info ${pool}"
  else
    echo "AUTOSPEC_STORAGE_KIND=lvm"
    echo "AUTOSPEC_LVM_VOLUME_GROUP=${pool}"
    echo "AUTOSPEC_STORAGE_POOL=${pool}"
    echo "Probe: /usr/sbin/lvm vgs ${pool}"
  fi
  echo "Apply requires: AUTOSPEC_STORAGE_CONFIRM=${confirm} $0 apply ${kind} ${pool} ${state_root}"
}

probe() {
  if [ "$kind" = apfs ]; then
    command -v diskutil >/dev/null 2>&1 || { echo "diskutil is unavailable" >&2; exit 69; }
    diskutil info "$pool" >/dev/null
  else
    [ -x /usr/sbin/lvm ] || { echo "/usr/sbin/lvm is unavailable" >&2; exit 69; }
    /usr/sbin/lvm vgs --noheadings --options vg_uuid "$pool" >/dev/null
  fi
}

case "$action" in
  plan)
    plan
    ;;
  probe)
    probe
    [ -d "$state_root" ] || { echo "state root does not exist: ${state_root}" >&2; exit 69; }
    [ ! -L "$state_root" ] || { echo "state root must not be a symlink" >&2; exit 69; }
    echo "execution storage capability is present"
    ;;
  apply)
    if [ "${AUTOSPEC_STORAGE_CONFIRM:-}" != "$confirm" ]; then
      echo "refusing apply; set AUTOSPEC_STORAGE_CONFIRM=${confirm}" >&2
      exit 65
    fi
    probe
    install -d -m 0700 "$state_root"
    [ ! -L "$state_root" ] || { echo "state root must not be a symlink" >&2; exit 69; }
    echo "execution storage state root provisioned: ${state_root}"
    ;;
esac
