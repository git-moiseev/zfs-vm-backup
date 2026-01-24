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

set -eou pipefail

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
FORCE_ARCHIVE=

# Local retention policy
KEEP_LOCAL=50   # number of latest snapshots to keep locally
KEEP_UNTIL=     # YYMMDD format date 
SET_NAME=       # Set snapshot name

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
# Logs messages to syslog and optionally stdout.
# Output to stdout only happens in interactive mode.
# ============================================================

log() {
    if [ $INTERACTIVE -eq 1 ]; then 
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
    fi
    logger -t backup "$*"
}

# ============================================================
# Emits debug output based on DEBUG verbosity level.
# Arguments:
#   $1  Debug level (default: 1)
#   $*  Debug message
# ============================================================

debug() {
    local LEVEL=${1:-1}
    if [ ${LEVEL} -le ${DEBUG} ]; then
        shift # Remove $1 from $*
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
        logger -t backup "[DEBUG $LEVEL] $*"
    fi
}

# ============================================================
# Ensures required userland tools are installed.
# Currently supports Debian-based systems only.
# ============================================================

check_dependicies() {
for PROG in mbuffer pv numfmt; do
     if [ ! -x /usr/bin/$PROG ]; then
          apt install -y $PROG
     fi
done
}

# ============================================================
# Prints CLI usage information and exits.
# ============================================================

usage() {
cat <<EOF
Usage: vm-backup.sh [OPTIONS]

ZFS snapshot, backup and archive replication for Proxmox.

Options:
  -h, --help
        Show this help and exit.

  --dry-run
        Print commands without executing them.

  --debug LEVEL
        Enable debug output.
        LEVEL:
          1  basic debug messages
          2  include ZFS GUID and bookmark logic
          3  very verbose (command tracing)

  --dataset DATASET
        Local ZFS dataset to back up.
        Default: ${LOCAL_DS}

  --backup-host HOST
        Backup server hostname.
        Default: ${BACKUP_HOST}

  --backup-dataset DATASET
        ZFS dataset on backup server.
        Default: ${BACKUP_PATH}

  --archive
        Force offsite archive replication.
        (Archive logic must be enabled in script.)

  --archive-host HOST
        Archive server hostname.
        Default: ${ARCHIVE_HOST}

  --archive-dataset DATASET
        ZFS dataset on archive server.
        Default: ${ARCHIVE_PATH}

  --keep COUNT
        Number of local snapshots to retain.
        Default: ${KEEP_LOCAL}

  --keep-until YYYYMMDD
        Expiration date for created snapshots.
        Sets ZFS property: custom:keep-until

  --rename SNAPSHOT_NAME
        Override automatic snapshot name.
        Default: timestamp (YYYY-MM-DD-HHMMSS)

Examples:
  vm-backup.sh
        Run backup with defaults.

  vm-backup.sh --dry-run --debug 2
        Show all commands and bookmark matching logic.

  vm-backup.sh --dataset tank/vm --keep 30
        Back up a different dataset with shorter retention.

  vm-backup.sh --rename manual-rollback
        Create a snapshot with a fixed name.

EOF
}


# ============================================================
# Argument parsing (getopt)
# ============================================================

OPTIONS=$(getopt -o h \
    --long help,dry-run,archive,debug:,dataset:,backup-host:,backup-dataset:,archive-host:,archive-dataset:,keep:,keep-until:,rename: \
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
        --keep)
            KEEP_LOCAL="$2"
            shift 2
            ;;
        --keep-until)
            KEEP_UNTIL="$2"
            shift 2
            ;;
        --rename)
            SET_NAME="$2"
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
    elif [ ${DEBUG} -gt 1 ]; then
        log "[DEBUG] ${CMD}"
        eval "${CMD}"
    else
        eval "${CMD}"
    fi
}

# ============================================================
# Sets a custom ZFS property indicating snapshot expiration.
# Arguments:
#   $1  Dataset or snapshot
#   $2  Expiration date (YYYYMMDD)
# ============================================================

zfs_set_keep_until() {
    local dataset="$1"
    local date="$2"
    local prop="custom:keep-until"

    # basic YYYYMMDD validation
    if [[ "$date" =~ ^[0-9]{8}$ ]]; then
       run_cmd zfs set "$prop=$date" "$dataset"
    else
       log "Invalid date format (expected YYYYMMDD)"
    fi
}

# ============================================================
# 1) Local snapshot creation + rotation + bookmark
#
# Policy:
#  - Always create a snapshot
#  - Immediately create a bookmark with the same name
# ============================================================

snapshot_local() {
    local SNAP_NAME="$1"
    local FULL_SNAP_NAME="${LOCAL_DS}@${SNAP_NAME}"
    local FULL_BOOKMARK_NAME="${LOCAL_DS}#${SNAP_NAME}"

    log "Creating snapshot ${FULL_SNAP_NAME}"
    run_cmd zfs snapshot "${FULL_SNAP_NAME}"
    if [ -n "$KEEP_UNTIL" ]; then 
        zfs_set_keep_until "${LOCAL_DS}" "$KEEP_UNTIL"
    fi

    log "Creating bookmark ${FULL_BOOKMARK_NAME}"
    run_cmd zfs bookmark "${FULL_SNAP_NAME}" "${FULL_BOOKMARK_NAME}"

    log "Local snapshot ${FULL_SNAP_NAME} created"
}

zfs_cleanup_if_expired() {
    local obj="$1"
    local prop="custom:keep-until"
    local today
    today=$(date +%Y%m%d)

        keep_until=$(zfs get -H -o value "$prop" "$obj" 2>/dev/null)

        if [[ "$keep_until" == "-" || -z "$keep_until" || "$today" -gt "$keep_until" ]]; then
             run_cmd zfs destroy "$obj"
        fi
}

snapshot_rotate() {
    # Rotate local snapshots (keep only the newest KEEP_LOCAL)
    ALL_SNAPS=($(zfs list -H -t snapshot -o name -s creation -r "${LOCAL_DS}"))
    NUM=${#ALL_SNAPS[@]}

    if [ "${NUM}" -gt "${KEEP_LOCAL}" ]; then
        TO_REMOVE=("${ALL_SNAPS[@]:0:NUM-KEEP_LOCAL}")
        for OLD_SNAP in "${TO_REMOVE[@]}"; do
            zfs_cleanup_if_expired "${OLD_SNAP}"
        done
    fi
}

# ============================================================
# Find the most recent local bookmark matching
# any remote snapshot GUID
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

    # --- Local bookmarks and snaphots ---
    while read -r NAME GUID; do
        NAME="${NAME//[$'\r\n']}"
        GUID="${GUID//[$'\r\n']}"
        debug 2 "NAME=$NAME, GUID=$GUID"
        # Append to existing list if duplicate GUID
        if [ -n "${BM_BY_GUID[$GUID]:-}" ]; then
            BM_BY_GUID["$GUID"]="${BM_BY_GUID[$GUID]} $NAME"
        else
            BM_BY_GUID["$GUID"]="$NAME"
            LOCAL_GUIDS_ORDER+=("$GUID")  # Maintain insertion order
        fi
        debug 2 "BM_BY_GUID['$GUID']=${BM_BY_GUID[$GUID]}"
    done < <(zfs list -H -t snapshot,bookmark -o name,guid -S creation -r "${LOCAL_DS}")

    debug 2 "LOCAL_GUIDS_ORDER=${LOCAL_GUIDS_ORDER[*]}"

    # --- Walk remote snapshots newest → oldest ---
    for R_GUID in "${REMOTE_GUIDS_ORDER[@]}"; do
        if [ -n "${BM_BY_GUID[$R_GUID]:-}" ]; then
            # Pick the newest local bookmark for this GUID (first in list)
            LAST_RECENT_BOOKMARK="${BM_BY_GUID[$R_GUID]%% *}"
            return 0
        else
            REMOTE_SNAP_TO_DELETE="$REMOTE_SNAP_TO_DELETE ${NAME_BY_GUID[$R_GUID]}"
        fi
    done
}

# ============================================================
# Incremental (or full) send to remote host
# Sends a ZFS snapshot to a remote host using incremental replication.
# Falls back to full send if no common bookmark is found.
#
# Arguments:
#   $1  Local dataset
#   $2  Snapshot name
#   $3  Remote user
#   $4  Remote host
#   $5  Remote dataset
# ============================================================

send_increment() {
    local DATASTORE="$1"
    local SNAP="$2"
    local REMOTE_USER="$3"
    local REMOTE_HOST="$4"
    local REMOTE_DS="$5"

    # get_recent_bookmark sets variables 
    # LAST_RECENT_BOOKMARK
    # REMOTE_SNAP_TO_DELETE
    get_recent_bookmark "${DATASTORE}" "${REMOTE_USER}" "${REMOTE_HOST}" "${REMOTE_DS}"
    log "Found recent bookmark: ${LAST_RECENT_BOOKMARK} against "${REMOTE_HOST}" "${REMOTE_DS}""

    # Cleanup incompatible remote snapshots
    if [ -n "${REMOTE_SNAP_TO_DELETE}" ]; then
    log "Remote snapsot to delete: ${REMOTE_SNAP_TO_DELETE}"
        for R_SNAP in ${REMOTE_SNAP_TO_DELETE}; do
            log "Removing remote snapshot ${R_SNAP}"
            run_cmd "ssh ${REMOTE_USER}@${REMOTE_HOST} zfs destroy -r ${R_SNAP}" || true
        done
    fi

    zfs_snap_remove_if_exists "${REMOTE_HOST}" "${REMOTE_DS}" "${SNAP}"

    if [ -n "${LAST_RECENT_BOOKMARK}" ]; then
        log "Incremental send from ${LAST_RECENT_BOOKMARK} to ${DATASTORE}@${SNAP} to ${REMOTE_HOST} ${REMOTE_DS}"
        SEND_CMD="zfs send -c -i ${LAST_RECENT_BOOKMARK} ${DATASTORE}@${SNAP}"
    else
        log "Full send of ${DATASTORE}@${SNAP} to ${REMOTE_HOST} ${REMOTE_DS}"
        SEND_CMD="zfs send -c ${DATASTORE}@${SNAP}"
        run_cmd "ssh -q ${REMOTE_USER}@${REMOTE_HOST} mkdir -p ${REMOTE_DS}"
    fi

    if [ "${INTERACTIVE}" -eq 1 ]; then
        if [ ${DRY_RUN} -eq 1 ]; then 
            log "If DRY_RUN cannot caclulate actualy STREAM_SIZE without ${DATASTORE}@${SNAP} exists"
            STREAM_SIZE=1000000000
        else 
            STREAM_SIZE=$(${SEND_CMD} -Pn | tail -1 | awk '{print $2}')
        fi
        log $(echo ${STREAM_SIZE} | numfmt --to=iec --format "Tolal %f will be sent") "($STREAM_SIZE bytes)"
        CMD="${SEND_CMD} | pv -s ${STREAM_SIZE} | mbuffer -q -s 1M -m ${MBUFFER_MEM} -L ${MBUFFER_SPEED} | ssh ${REMOTE_USER}@${REMOTE_HOST} zfs recv -Fu ${REMOTE_DS}"
    else
        CMD="${SEND_CMD} | mbuffer -q -s 1M -m ${MBUFFER_MEM} -L ${MBUFFER_SPEED} | ssh ${REMOTE_USER}@${REMOTE_HOST} zfs recv -Fu ${REMOTE_DS}"
    fi

    run_cmd "${CMD}"

}


# ============================================================
# 5) Copy Proxmox VM configuration (manifests)
# ============================================================

copy_manifests() {
    if [ -d /etc/pve/qemu-server ]; then 
        run_cmd "rsync -a --copy-links --delete /etc/pve/qemu-server /${LOCAL_DS}/"
    fi
}

# zfs_snapshot_rename () {
#     local OLD_NAME=${1}
#     local NEW_NAME=${2}
#     local SRC=$(zfs list -H -t snapshot -o name ${LOCAL_DS}@${OLD_NAME} 2>/dev/null)
#     if [ -n "${SRC}" ]; then
#         local DST=$(zfs list -H -t snapshot -o name ${LOCAL_DS}@${NEW_NAME} 2>/dev/null)
#         if [ -n "${DST}" ]; then
#             log "Snapsot ${DST} already exists. Remove it"
#             run_cmd zfs destroy -r ${DST}
#         fi
#         run_cmd zfs rename ${LOCAL_DS}@${1} ${LOCAL_DS}@${2}
#     else
#         log "Snapsot ${SRC} does not exists."
#     fi
# }

zfs_snap_remove_if_exists() {
    local HOST=${1}
    local DATASTORE=${2}
    local SNAP=${3}
    local PRE_CMD=
    if [ ! "$HOST" = "localhost" ];then 
        PRE_CMD="ssh -q $HOST"
    fi

    local SNAP_EXISTS=$($PRE_CMD zfs list -H -t snapshot -o name ${DATASTORE}@${SNAP} 2>/dev/null)
    if [ -n "${SNAP_EXISTS}" ]; then
        log "Snapsot ${DATASTORE}@${SNAP} already exists on $HOST. Remove it"
        run_cmd "$PRE_CMD zfs destroy ${DATASTORE}@${SNAP}"
    fi
    # If destroy snapdhot, also destroy bookmark
    local MARK_EXISTS=$($PRE_CMD zfs list -H -t bookmark -o name ${DATASTORE}#${SNAP} 2>/dev/null)
    if [ -n "${MARK_EXISTS}" ]; then
        log "Bookmark ${DATASTORE}#${SNAP} already exists on $HOST.. Remove it"
        run_cmd "$PRE_CMD zfs destroy ${DATASTORE}#${SNAP}"
    fi
}

# ============================================================
# Main workflow
# ============================================================

check_dependicies

run_cmd "zfs list -H -o name ${LOCAL_DS}" || exit 1

copy_manifests

if [ -n "${SET_NAME}" ]; then
    SNAP_NAME=${SET_NAME}
else
    SNAP_NAME=${DATE}
fi 

zfs_snap_remove_if_exists localhost "${LOCAL_DS}" "${SNAP_NAME}"

snapshot_local "${SNAP_NAME}"

# Rename snaphot (morning or first day of month run)
#if [ -n "${SET_NAME}" ]; then
#    snapshot_rename "${DATE}" "${SET_NAME}"
#fi

# Nearline backup (every run)
send_increment "${LOCAL_DS}" "${SNAP_NAME}" "${BACKUP_USER}" "${BACKUP_HOST}" "${BACKUP_PATH}"

snapshot_rotate "${LOCAL_DS}"

# Offsite archive (monthly)
if [ -n "$FORCE_ARCHIVE" -o "${DAY}" = "01" ]; then
    send_increment "${LOCAL_DS}" "${SNAP_NAME}" "${ARCHIVE_USER}" "${ARCHIVE_HOST}" "${ARCHIVE_PATH}" 
fi

log "Backup & archive workflow completed."

