# Local Netdata

Netdata runs natively on Apple Silicon through Homebrew. It does not use Colima or modify the
`podman-machine-applehv` machine that runs McCain Capital.

Start Netdata:

```bash
./monitoring/netdata/start.sh
```

Open <http://127.0.0.1:19999/>. The dashboard binds only to localhost and does not require a
username or password.

The `httpcheck` collector checks McCain Capital's `/healthz` endpoint every 10 seconds and records
availability, response time, response length, and HTTP status.

Stop Netdata without affecting the app:

```bash
./monitoring/netdata/stop.sh
```
