#!/bin/bash
#
# vm-backup.sh
#
# Backup & Archive Policy implementation for Proxmox + ZFS
#
# Features:
#  - Local ZFS snapshots with rotation
#  - ZFS bookmarks for incremental transfer
#  - Incremental send/receive over SSH
#  - Separate backup and archive targets
#  - Interactive progress (pv + mbuffer)
#  - cron-friendly dry-run and debug modes
#

# set -euo pipefail

# ============================================================
# Default configuration (policy-level parameters)
# ============================================================

DRY_RUN=0
DEBUG=0

# Local dataset to back up
LOCAL_DS="tank/test"

# Backup server (nearline backups)
BACKUP_USER="root"
BACKUP_HOST="nfs8"
BACKUP_PATH="tank/backup/${HOSTNAME}"

# Archive server (offsite, long-term storage)
ARCHIVE_USER="root"
ARCHIVE_HOST="nfs9"
ARCHIVE_PATH="tank/archive/${HOSTNAME}"

# Local retention policy
KEEP_LOCAL=5   # number of latest snapshots to keep locally

# Transfer tuning
MBUFFER_MEM="2G"
MBUFFER_SPEED=""

# Interactive mode:
#   1 = show progress (manual run)
#   0 = silent (cron)
INTERACTIVE=1

# Time-related variables
DATE=$(date +%Y-%m-%d-%H%M%S)
DAY=$(date +%d)
MONTH=$(date +%b)
YEAR=$(date +%Y)

# ============================================================
# Logging helpers
# ============================================================

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
}

# ============================================================
# Usage / help
# ============================================================

usage() {
cat <<EOF
Usage: vm-backup.sh [options]

Options:
  --help                    Show this help and exit
  --dry-run                 Show commands without executing them
  --debug                   Enable verbose debug output

  --dataset DATASET         Local ZFS dataset to backup
                             (default: tank/test)

  --backup-host HOST        Backup server hostname
                             (default: nfs8)

  --backup-dataset DATASET  ZFS dataset for backups on backup server
                             (default: tank/backup)

  --archive-host HOST       Archive server hostname (offsite)
                             (default: nfs9)

  --archive-dataset DATASET ZFS dataset for archives on archive server
                             (default: tank/archive)

Examples:
  vm-backup.sh --dry-run
  vm-backup.sh --dataset tank/vm
  vm-backup.sh --debug --dry-run
EOF
}

# ============================================================
# Argument parsing (getopt)
# ============================================================

OPTIONS=$(getopt -o h \
    --long help,dry-run,debug,dataset:,backup-host:,backup-dataset:,archive-host:,archive-dataset: \
    -n 'vm-backup.sh' -- "$@")

if [ $? != 0 ]; then
    echo "Incorrect options provided"
    exit 1
fi

eval set -- "${OPTIONS}"

while true; do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        --dry-run)
            DRY_RUN=1
            shift
            ;;
        --debug)
            DEBUG=1
            shift
            ;;
        --dataset)
            LOCAL_DS="$2"
            shift 2
            ;;
        --backup-host)
            BACKUP_HOST="$2"
            shift 2
            ;;
        --backup-dataset)
            BACKUP_PATH="$2"
            shift 2
            ;;
        --archive-host)
            ARCHIVE_HOST="$2"
            shift 2
            ;;
        --archive-dataset)
            ARCHIVE_PATH="$2"
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *)
            usage
            exit 1
            ;;
    esac
done

# ============================================================
# Command execution wrapper (dry-run / debug aware)
# ============================================================

run_cmd() {
    local CMD="$*"

    if [ "${DRY_RUN}" -eq 1 ]; then
        log "[DRY-RUN] ${CMD}"
    elif [ "${DEBUG}" -eq 1 ]; then
        log "[DEBUG] ${CMD}"
        eval "${CMD}"
    else
        eval "${CMD}"
    fi
}

# ============================================================
# 1) Local snapshot creation + rotation + bookmark
#
# Policy:
#  - Always create a snapshot
#  - Immediately create a bookmark with the same name
#  - Keep only the latest KEEP_LOCAL snapshots locally
# ============================================================

snapshot_local() {
    local SNAP_NAME="$1"
    local FULL_SNAP_NAME="${LOCAL_DS}@${SNAP_NAME}"
    local FULL_BOOKMARK_NAME="${LOCAL_DS}#${SNAP_NAME}"

    log "Creating snapshot ${FULL_SNAP_NAME}"
    run_cmd zfs snapshot "${FULL_SNAP_NAME}"

    log "Creating bookmark ${FULL_BOOKMARK_NAME}"
    run_cmd zfs bookmark "${FULL_SNAP_NAME}" "${FULL_BOOKMARK_NAME}"

    # Rotate local snapshots (keep only the newest KEEP_LOCAL)
    ALL_SNAPS=($(zfs list -H -t snapshot -o name -s creation -r "${LOCAL_DS}"))
    NUM=${#ALL_SNAPS[@]}

    if [ "${NUM}" -gt "${KEEP_LOCAL}" ]; then
        TO_REMOVE=("${ALL_SNAPS[@]:0:NUM-KEEP_LOCAL}")
        for OLD_SNAP in "${TO_REMOVE[@]}"; do
            log "Destroying old snapshot ${OLD_SNAP}"
            run_cmd zfs destroy "${OLD_SNAP}"
        done
    fi

    log "Local snapshot ${FULL_SNAP_NAME} created"
}

# ============================================================
# 2) Find the most recent local bookmark matching
#    any remote snapshot GUID
#
# Purpose:
#  - Resume incremental replication after outages
#  - Do NOT rely on snapshot names or timestamps
# ============================================================

get_recent_bookmark() {
    local REMOTE_USER="$2"
    local REMOTE_HOST="$3"
    local REMOTE_DS="$4"

    LAST_RECENT_BOOKMARK=""
    REMOTE_SNAP_TO_DELETE=""

    # --- Declare arrays ---
    declare -A NAME_BY_GUID          # Remote snapshots: GUID → name
    declare -A BM_BY_GUID            # Local bookmarks: GUID → space-separated names
    # Associative arrays in Bash are unordered: ${!BM_BY_GUID[@]} returns keys in arbitrary order (depends on the internal hash table).
    declare -a REMOTE_GUIDS_ORDER    # Order of remote snapshots newest → oldest
    declare -a LOCAL_GUIDS_ORDER     # Order of local bookmarks newest → oldest

    # --- Remote snapshots ---
    while read -r R_NAME R_GUID; do
        # Trim hidden chars
        R_NAME="${R_NAME//[$'\r\n']}"
        R_GUID="${R_GUID//[$'\r\n']}"
        NAME_BY_GUID["$R_GUID"]="$R_NAME"
        REMOTE_GUIDS_ORDER+=("$R_GUID")
    done < <(ssh "${REMOTE_USER}@${REMOTE_HOST}" \
        zfs list -H -t snapshot -o name,guid -S creation -r "${REMOTE_DS}" 2>/dev/null)

    [ ${#REMOTE_GUIDS_ORDER[@]} -eq 0 ] && return 0

    log "REMOTE_GUIDS_ORDER=${REMOTE_GUIDS_ORDER[*]}"

    # --- Local bookmarks ---
    while read -r NAME GUID; do
        NAME="${NAME//[$'\r\n']}"
        GUID="${GUID//[$'\r\n']}"
        # Append to existing list if duplicate GUID
        if [ -n "${BM_BY_GUID[$GUID]}" ]; then
            BM_BY_GUID["$GUID"]="${BM_BY_GUID[$GUID]} $NAME"
        else
            BM_BY_GUID["$GUID"]="$NAME"
            LOCAL_GUIDS_ORDER+=("$GUID")  # Maintain insertion order
        fi
        log "BM_BY_GUID['$GUID']=${BM_BY_GUID[$GUID]}"
    done < <(zfs list -H -t bookmark -o name,guid -S creation -r "${LOCAL_DS}")

    log "LOCAL_GUIDS_ORDER=${LOCAL_GUIDS_ORDER[*]}"

    # --- Walk remote snapshots newest → oldest ---
    for R_GUID in "${REMOTE_GUIDS_ORDER[@]}"; do
        if [ -n "${BM_BY_GUID[$R_GUID]}" ]; then
            # Pick the newest local bookmark for this GUID (first in list)
            LAST_RECENT_BOOKMARK="${BM_BY_GUID[$R_GUID]%% *}"
            log "Found recent bookmark: $LAST_RECENT_BOOKMARK"
            return 0
        else
            REMOTE_SNAP_TO_DELETE="$REMOTE_SNAP_TO_DELETE ${NAME_BY_GUID[$R_GUID]}"
        fi
    done
}

# ============================================================
# 3) Incremental (or full) send to remote host
# ============================================================

send_increment() {
    local SNAP="$2"
    local REMOTE_USER="$3"
    local REMOTE_HOST="$4"
    local REMOTE_DS="$5"

    get_recent_bookmark "${LOCAL_DS}" "${REMOTE_USER}" "${REMOTE_HOST}" "${REMOTE_DS}"

    # Cleanup incompatible remote snapshots
    if [ -n "${REMOTE_SNAP_TO_DELETE}" ]; then
        for R_SNAP in ${REMOTE_SNAP_TO_DELETE}; do
            log "Removing remote snapshot ${R_SNAP}"
            run_cmd "ssh ${REMOTE_USER}@${REMOTE_HOST} zfs destroy -r ${R_SNAP}" || true
        done
    fi

    if [ -n "${LAST_RECENT_BOOKMARK}" ]; then
        log "Incremental send from ${LAST_RECENT_BOOKMARK} to ${LOCAL_DS}@${SNAP}"
        SEND_CMD="zfs send -c -i ${LAST_RECENT_BOOKMARK} ${LOCAL_DS}@${SNAP}"
    else
        log "Full send of ${LOCAL_DS}@${SNAP}"
        SEND_CMD="zfs send -c ${LOCAL_DS}@${SNAP}"
        run_cmd "ssh ${REMOTE_USER}@${REMOTE_HOST} mkdir -p ${REMOTE_DS}"
    fi

    if [ "${INTERACTIVE}" -eq 1 ]; then
        STREAM_SIZE=$(${SEND_CMD} -Pn | tail -1 | awk '{print $2}')
        CMD="${SEND_CMD} | pv -s ${STREAM_SIZE} | mbuffer -s 1M -m ${MBUFFER_MEM} -L ${MBUFFER_SPEED} | ssh ${REMOTE_USER}@${REMOTE_HOST} zfs recv -Fu ${REMOTE_DS}"
    else
        CMD="${SEND_CMD} | mbuffer -s 1M -m ${MBUFFER_MEM} -L ${MBUFFER_SPEED} | ssh ${REMOTE_USER}@${REMOTE_HOST} zfs recv -Fu ${REMOTE_DS}"
    fi

    run_cmd "${CMD}"

    # Optional rename (used for monthly archives)
    if [[ -n ${6+x} && -n $6 ]]; then
        NEW_SNAP_NAME="$6"
        run_cmd "ssh ${REMOTE_USER}@${REMOTE_HOST} zfs rename ${REMOTE_DS}@${SNAP} ${REMOTE_DS}@${NEW_SNAP_NAME}"
    fi
}

# ============================================================
# Copy Proxmox VM configuration (manifests)
# ============================================================

copy_manifests() {
    run_cmd "rsync -av --copy-links --delete /etc/pve/qemu-server /${LOCAL_DS}/"
}

# ============================================================
# Main workflow
# ============================================================

run_cmd "zfs list ${LOCAL_DS}" || exit 1

copy_manifests
snapshot_local "${DATE}"

# Nearline backup (every run)
send_increment "X" "${DATE}" "${BACKUP_USER}" "${BACKUP_HOST}" "${BACKUP_PATH}"

# Offsite archive (monthly)
if [ "${DAY}" = "01" ]; then
    send_increment "X" "${DATE}" "${ARCHIVE_USER}" "${ARCHIVE_HOST}" "${ARCHIVE_PATH}" "${YEAR}-${MONTH}"
fi

log "Backup & archive workflow completed."
