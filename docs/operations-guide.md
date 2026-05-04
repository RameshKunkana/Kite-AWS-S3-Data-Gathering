# DriFy Ingestor — Operations Guide

---

## Prerequisites

- PEM file: `C:\Users\mp2bc\Downloads\drify-ingestor-data-gathering-key-pair.pem`
- EC2 Public IP: check AWS Console → EC2 → Instances → `drify-ingestor` → Public IPv4
- PowerShell on your local Windows machine

---

## 1. Connect to EC2

Open PowerShell on your local machine and run:

```powershell
ssh -i "C:\Users\mp2bc\Downloads\drify-ingestor-data-gathering-key-pair.pem" ubuntu@<EC2_PUBLIC_IP>
```

Replace `<EC2_PUBLIC_IP>` with your actual EC2 public IP (e.g. `13.235.xx.xx`).

You are connected when you see:
```
ubuntu@ip-172-31-47-247:~$
```

---

## 2. Daily Morning Routine

Every morning before 09:00 IST, do the following after connecting to EC2.

### Step 1 — Navigate to the repo

```bash
cd ~/apps/drify/repo
```

### Step 2 — Pull latest code from GitHub

Run this every morning (or whenever code has been updated):

```bash
git pull
```

This fetches the latest code from GitHub to EC2.
If you see `Already up to date`, no new code was pushed — that is fine.

### Step 3 — Update the Kite access token

The Kite access token expires every day and must be updated before the market opens.

```bash
nano .env
```

Find the line:
```
KITE_ACCESS_TOKEN=xxxxxxxxxxxxxxxx
```

Replace the value with today's fresh token. Save: `Ctrl+O` → Enter → `Ctrl+X`

### Step 4 — Restart the service

```bash
sudo systemctl restart drify-ingestor
```

### Step 5 — Verify it is running

```bash
sudo systemctl status drify-ingestor
```

You should see `Active: active (running)`.

To watch live logs for a few seconds to confirm ticks are flowing:

```bash
sudo journalctl -u drify-ingestor -f
```

Press `Ctrl+C` to stop watching logs.

### Step 6 — Disconnect

You can now close PowerShell. The app runs on EC2 independently.

---

## 3. Start the Service

If the service is stopped and you want to start it:

```bash
sudo systemctl start drify-ingestor
```

---

## 4. Stop the Service

To stop data collection at any time:

```bash
sudo systemctl stop drify-ingestor
```

The service will not restart until you start it again manually.

---

## 5. Check Service Status

```bash
sudo systemctl status drify-ingestor
```

| Status | Meaning |
|---|---|
| `active (running)` | App is running normally |
| `activating (auto-restart)` | App crashed, systemd is restarting it |
| `inactive (dead)` | App is stopped |
| `failed` | App failed and is not retrying |

---

## 6. View Logs

**Last 50 lines:**
```bash
sudo journalctl -u drify-ingestor -n 50 --no-pager
```

**Live log stream:**
```bash
sudo journalctl -u drify-ingestor -f
```

**Logs since a specific time:**
```bash
sudo journalctl -u drify-ingestor --since "2026-04-24 09:00:00"
```

Press `Ctrl+C` to stop watching.

---

## 7. Deploy a Code Update

When new code has been pushed to GitHub, do the following on EC2:

```bash
cd ~/apps/drify/repo
git pull
sudo systemctl restart drify-ingestor
sudo journalctl -u drify-ingestor -f
```

If the Glue compaction script (`infra/glue_daily_compaction.py`) was also updated,
run this extra command after `git pull`:

```bash
aws s3 cp infra/glue_daily_compaction.py \
  s3://drify-market-data/scripts/glue_daily_compaction.py \
  --region ap-south-1
```

---

## 8. Enable / Disable Auto-Start on EC2 Reboot

**Disable** (service will not start automatically if EC2 reboots):
```bash
sudo systemctl disable drify-ingestor
```

**Re-enable:**
```bash
sudo systemctl enable drify-ingestor
```

---

## 9. Update Market Holidays

At the start of each year, update the `MARKET_HOLIDAYS` line in `.env` with the
new NSE holiday list:

```bash
cd ~/apps/drify/repo
nano .env
```

Find the line:
```
MARKET_HOLIDAYS=2026-01-15,2026-01-26,...
```

Replace with the new year's dates in `YYYY-MM-DD` format, comma-separated.

---

## Quick Reference

| Tasks                          | Command                                       |
|------------------------------- |-------------------------------------------    |
| Connect to EC2                 | `ssh -i "path/to/key.pem" ubuntu@<IP>`        |
| Go to repo                     | `cd ~/apps/drify/repo`                        |
| Update token                   | `nano .env` → update `KITE_ACCESS_TOKEN`      |
| **Start data collection**      | **`sudo systemctl restart drify-ingestor`**   |
| Pull latest code (if updated)  | `git pull`                                    |
| Stop service                   | `sudo systemctl stop drify-ingestor`          |
| Restart service                | `sudo systemctl restart drify-ingestor`       |
| Check status                   | `sudo systemctl status drify-ingestor`        |
| View live logs                 | `sudo journalctl -u drify-ingestor -f`        |
| Upload Glue script             | `aws s3 cp infra/glue_daily_compaction.py s3://drify-market-data/scripts/glue_daily_compaction.py --region ap-south-1` |