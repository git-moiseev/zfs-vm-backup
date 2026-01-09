# Proxmox ZFS Backup & Archive Policy

This repository contains a **policy-driven backup & archive workflow** for Proxmox VE hosts using **ZFS native snapshots, bookmarks and replication**.

The goal is to achieve:

* Fast local rollback for developers
* Reliable nearline backups
* Long‑term offsite archives
* Resume-safe incremental replication after outages
* Minimal metadata and no external state

Everything is implemented as **policy-as-code** in a single Bash script.

---

## Architecture Overview

### Infrastructure

* **Proxmox hosts**: `pve0 … pve9`

  * Local ZFS storage
  * 5–20 VMs per host

* **Backup server**: `nfs8`

  * Same datacenter
  * 10G network
  * Nearline backups with rotation

* **Archive server**: `nfs9`

  * Remote datacenter
  * 1G network
  * Long-term archives (kept indefinitely)

---

## Backup & Archive Policy

### 1. Local snapshots (every run)

* Create a ZFS snapshot of the VM dataset
* Immediately create a **ZFS bookmark** with the same name
* Keep only the **last N snapshots** locally (`KEEP_LOCAL`)
* Older snapshots are destroyed, bookmarks remain

**Why bookmarks?**

* Zero space usage
* Stable GUID
* Ideal anchors for incremental replication

---

### 2. Nearline backups (nfs8)

* Every run sends the newest snapshot to `nfs8`
* Incremental send is used whenever possible
* Increment base is determined **by GUID**, not by name or date
* If the remote side diverged, incompatible snapshots are removed automatically

Retention:

* Snapshots are rotated on the backup server
* Older snapshots are deleted according to local policy

---

### 3. Offsite archives (nfs9)

* Executed **only on the 1st day of each month**
* The monthly snapshot is:

  * Sent incrementally if possible
  * Renamed to `YYYY-Mon` (e.g. `2026-Jan`)
* Archives are kept indefinitely

**Important:**

* Bookmarks ensure archives can be recreated even after years
* No reliance on flags, state files or timestamps

---

## GUID-Based Resume Logic

When connectivity is lost between datacenters:

1. The archive server is queried for snapshot GUIDs
2. GUIDs are sorted by creation time (newest first)
3. The script searches for the **newest matching local bookmark**
4. Incremental replication resumes from that point

This guarantees:

* No unnecessary full sends
* Safe resume after weeks or months of downtime

---

## Workflow Diagram

```mermaid
flowchart TD

    A[Proxmox Host] -->|Create snapshot| B[Local ZFS Snapshot]
    B -->|Create bookmark| C[Local ZFS Bookmark]

    B -->|Rotate snapshots| D[Keep last N snapshots]

    C -->|Incremental send (GUID-based)| E[nfs8 Backup Server]

    E -->|Rotation| F[Nearline Backup Retention]

    C -->|Monthly only| G[nfs9 Archive Server]
    G -->|Rename YYYY-Mon| H[Permanent Archive]

    subgraph Failure Recovery
        E -->|List GUIDs| I[Find newest matching bookmark]
        I -->|Resume incremental send| G
    end
```

---

## Script Features

* Pure Bash (no pyzfs dependency)
* Uses native ZFS tools only
* Interactive progress via `pv + mbuffer`
* cron-safe non-interactive mode
* `--dry-run` and `--debug` modes

---

## Usage

```bash
vm-backup.sh [options]
```

### Common examples

```bash
# Dry run
./vm-backup.sh --dry-run

# Backup a specific dataset
./vm-backup.sh --dataset tank/vm

# Debug execution
./vm-backup.sh --debug
```

---

## Design Principles

* **ZFS is the source of truth**
* **GUIDs over names**
* **Bookmarks instead of flags**
* **No external metadata**
* **Everything must be resumable**

---

## Intended Audience

This project is designed for:

* Proxmox administrators
* ZFS power users
* Infrastructure engineers
* Anyone who values deterministic, inspectable backup systems

---

## License

MIT (or your preferred license)

---

## Final Note

This is not just a script.

It is a **backup policy encoded directly into ZFS semantics**.
