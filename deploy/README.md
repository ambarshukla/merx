# deploy/

How merx runs on a small Linux host (2 CPUs, 4 GB of memory is plenty), behind an
existing reverse proxy. Decision D-015.

```
internet ──443──▶ reverse proxy (Caddy, TLS) ──▶ merx-web     :8000   /client /sources /ops
                                             └─▶ merx-dagster :3000   Dagster UI, password
daily timer (systemd, 00:30 UTC) ──▶ docker compose run --rm refresh   (merx refresh)

all three share one data directory on the host:  raw zone · DuckDB · Dagster history
```

| File | What it is |
|---|---|
| `../Dockerfile` | One image for every process |
| `docker-compose.yml` | `web` and `dagster` (long-running), `refresh` (one-off, profile `jobs`) |
| `merx-refresh.service`, `merx-refresh.timer` | The daily refresh, as a systemd timer |
| `.env.example` | What the host's `deploy/.env` holds: the EIA API key, the proxy's Docker network, the data directory |

## Why it is shaped like this

- **No always-on scheduler.** Dagster's daemon would run the schedules itself, at the cost
  of another long-running process. Instead a system timer runs `merx refresh` once a day,
  after the latest release time in the contracts; it works out which jobs are due that
  day (in each schedule's time zone) and runs them through Dagster, so the runs and their
  checks still appear in Dagster's UI. Timers survive reboots; `Persistent=true` catches
  up a run missed while the host was down.
- **The web app only reads.** It opens the database read-only per request, so the daily
  write and the reads do not need to coordinate beyond DuckDB's brief retries.
- **Dagster's UI has no login**, so the proxy puts it behind basic authentication.
  The consoles are public and read-only.
- **Restart policies** (`unless-stopped`) bring both services back after the host's
  automatic security reboots.

## First install

On the host, with Docker and a reverse proxy already running:

```bash
sudo mkdir -p /opt/merx/src /opt/merx/data && sudo chown -R "$USER" /opt/merx
# from a checkout of the repository, on your machine:
git archive HEAD | ssh HOST 'tar -x -C /opt/merx/src'
# optional: seed the data directory (raw zone and history) from a machine that has it
tar -C data --exclude=dagster_home -cf - . | ssh HOST 'tar -x -C /opt/merx/data'

# on the host
cd /opt/merx/src/deploy
cp .env.example .env && chmod 600 .env    # fill in the key and the proxy network
docker compose build
docker compose up -d web dagster
sudo cp merx-refresh.service merx-refresh.timer /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now merx-refresh.timer
```

Proxy configuration, one block per hostname (Caddy shown; certificates are automatic):

```
merx.example.com {
	encode gzip
	reverse_proxy merx-web:8000
}

merx-internal.example.com {
	basic_auth {
		merx <bcrypt hash from: caddy hash-password>
	}
	reverse_proxy merx-dagster:3000
}
```

If the proxy's Caddyfile is bind-mounted as a single file, **append** to it (`>>`)
rather than replacing it: tools that write a new file (such as `sed -i`) change the inode,
and the container keeps reading the old one. Then
`docker exec <proxy> caddy validate ...` and `caddy reload ...`.

## Updating

```bash
git archive HEAD | ssh HOST 'tar -x -C /opt/merx/src'
ssh HOST 'cd /opt/merx/src/deploy && docker compose build && docker compose up -d web dagster'
```

## Operating it

```bash
cd /opt/merx/src/deploy
docker compose ps
docker compose logs -f web
docker compose run --rm refresh merx refresh --dry-run   # what tonight's run will do
docker compose run --rm refresh merx exceptions
sudo systemctl start merx-refresh.service                # run today's refresh now
systemctl list-timers merx-refresh.timer
journalctl -u merx-refresh.service -n 50
```
