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

set -euo pipefail

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
KEEP_LOCAL=15   # number of latest snapshots to keep locally

# Transfer tuning
MBUFFER_MEM="2G"
MBUFFER_SPEED=""

# Interactive mode:
#   1 = show progress (manual run)
#   0 = silent (cron)
if  [[ -t 0 ]]; then 
    INTERACTIVE=1
else
    INTERACTIVE=0
fi
    

# Time-related variables
DATE=$(date +%Y-%m-%d-%H%M%S)
DAY=$(date +%d)
MONTH=$(date +%b)
YEAR=$(date +%Y)

PIDFILE=/var/run/zfs-send-recv.pid

# Prevent double start
if [[ -f $PIDFILE ]]; then
    oldpid=$(<"$PIDFILE")
    if kill -0 "$oldpid" 2>/dev/null; then
        log "zfs send already running (pid $oldpid), exiting"
        exit 0
    else
        log "Stale pidfile found, removing"
        rm -f "$PIDFILE"
    fi
fi

echo $$ >"$PIDFILE"
# PID file is always deleted, even if the script crashes or is interrupted.
trap 'rm -f "$PIDFILE"' EXIT

# ============================================================
# Logging helpers
# ============================================================

log() {
    if [ $INTERACTIVE -eq 1 ]; then 
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
    fi
    logger -t backup "$*"
}

debug() {
    local LEVEL=${1:-1}
    if [ ${LEVEL} -le ${DEBUG} ]; then
        shift # Remove $1 from $*
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
    fi
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
  --archive                 Force send datastore to archive
  --debug LEVEL             Enable verbose debug output LEVELS 1, 2, 3

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
    --long help,dry-run,archive,debug:,dataset:,backup-host:,backup-dataset:,archive-host:,archive-dataset: \
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
        --archive)
            FORCE_ARCHIVE=1
            shift
            ;;
        --debug)
            DEBUG="$2"
            shift 2
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

    if [ ${DRY_RUN} -eq 1 ]; then
        log "[DRY-RUN] ${CMD}"
    elif [ ${DEBUG} -eq 1 ]; then
        log "[DEBUG] ${CMD}"
        eval "${CMD}"
    else
        eval "${CMD}"
    fi
}
# Ensures the PID file is removed even if the script is killed
trap 'rm -f "$PIDFILE"' EXIT

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
# Sets a global variables LAST_RECENT_BOOKMARK, REMOTE_SNAP_TO_DELETE
# If matched bookmark does not exists or has descendants on remote 
# side they must to be deleted before send datastream
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

    debug 2 "REMOTE_GUIDS_ORDER=${REMOTE_GUIDS_ORDER[*]}"

    # --- Local bookmarks ---
    while read -r NAME GUID; do
        NAME="${NAME//[$'\r\n']}"
        GUID="${GUID//[$'\r\n']}"
        # Append to existing list if duplicate GUID
        if [ -n "${BM_BY_GUID[$GUID]:-}" ]; then
            BM_BY_GUID["$GUID"]="${BM_BY_GUID[$GUID]} $NAME"
        else
            BM_BY_GUID["$GUID"]="$NAME"
            LOCAL_GUIDS_ORDER+=("$GUID")  # Maintain insertion order
        fi
        debug 2 "BM_BY_GUID['$GUID']=${BM_BY_GUID[$GUID]}"
    done < <(zfs list -H -t bookmark -o name,guid -S creation -r "${LOCAL_DS}")

    debug 2 "LOCAL_GUIDS_ORDER=${LOCAL_GUIDS_ORDER[*]}"

    # --- Walk remote snapshots newest → oldest ---
    for R_GUID in "${REMOTE_GUIDS_ORDER[@]}"; do
        if [ -n "${BM_BY_GUID[$R_GUID]}" ]; then
            # Pick the newest local bookmark for this GUID (first in list)
            LAST_RECENT_BOOKMARK="${BM_BY_GUID[$R_GUID]%% *}"
            return 0
        else
            REMOTE_SNAP_TO_DELETE="$REMOTE_SNAP_TO_DELETE ${NAME_BY_GUID[$R_GUID]}"
        fi
    done
}

# ============================================================
# 3)Determine it is a first bookmark for today
# ============================================================

is_first_bookmark_today() {
    local bm="$1"
    local today_start min_bm="" min_ts=""

    today_start=$(date -d 'today 00:00' +%s)

    while read -r name ts; do
        (( ts < today_start )) && continue
        if [[ -z $min_ts || ts -lt min_ts ]]; then
            min_ts=$ts
            min_bm=$name
        fi
    done < <(zfs list -H -t bookmark -o name,creation -p rpool/test)

    [[ -n $min_bm && $bm == "$min_bm" ]]
}

# ============================================================
# 4) Incremental (or full) send to remote host
# ============================================================

send_increment() {
    local SNAP="$2"
    local REMOTE_USER="$3"
    local REMOTE_HOST="$4"
    local REMOTE_DS="$5"
    local NEW_SNAP_NAME="${6:-}"

    get_recent_bookmark "${LOCAL_DS}" "${REMOTE_USER}" "${REMOTE_HOST}" "${REMOTE_DS}"
    log "Found recent bookmark: ${LAST_RECENT_BOOKMARK}"

    # Cleanup incompatible remote snapshots
    if [ -n "${REMOTE_SNAP_TO_DELETE}" ]; then
    log "Remote snapsot to delete: ${REMOTE_SNAP_TO_DELETE}"
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
        CMD="${SEND_CMD} | pv -s ${STREAM_SIZE} | mbuffer -q -s 1M -m ${MBUFFER_MEM} -L ${MBUFFER_SPEED} | ssh ${REMOTE_USER}@${REMOTE_HOST} zfs recv -Fu ${REMOTE_DS}"
    else
        CMD="${SEND_CMD} | mbuffer -q -s 1M -m ${MBUFFER_MEM} -L ${MBUFFER_SPEED} | ssh ${REMOTE_USER}@${REMOTE_HOST} zfs recv -Fu ${REMOTE_DS}"
    fi

    run_cmd "${CMD}"

    # Optional rename (used for monthly archives)
    if [[ -n "$NEW_SNAP_NAME" ]]; then
        run_cmd "ssh ${REMOTE_USER}@${REMOTE_HOST} zfs rename ${REMOTE_DS}@${SNAP} ${REMOTE_DS}@${NEW_SNAP_NAME}"
    fi
}


# ============================================================
# 5) Copy Proxmox VM configuration (manifests)
# ============================================================

copy_manifests() {
    run_cmd "rsync -a --copy-links --delete /etc/pve/qemu-server /${LOCAL_DS}/"
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
if [ -n "$FORCE_ARCHIVE" -o "${DAY}" = "01" ]; then
    send_increment "X" "${DATE}" "${ARCHIVE_USER}" "${ARCHIVE_HOST}" "${ARCHIVE_PATH}" "${YEAR}-${MONTH}-${DAY}"
fi

log "Backup & archive workflow completed."

