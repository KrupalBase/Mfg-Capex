# Manufacturing MES Engineer — Reference

## SSH Diagnostic Commands (safe, read-only)

Use these to gather information before proposing changes.

| Goal | Example commands |
|------|------------------|
| Check if a service is running | `systemctl status <service>` or `sudo systemctl status <service>` |
| View recent logs | `journalctl -u <service> -n 100 --no-pager` or `tail -n 200 /path/to/log` |
| Find process by name | `pgrep -a <name>` or `ps aux \| grep <name>` |
| Check listening ports | `ss -tlnp` or `netstat -tlnp` (Linux) |
| Check config file | `cat /path/to/config` or `cat /etc/<app>/settings.json` |
| Check disk space | `df -h` |
| Check env / paths | `printenv` or `which <binary>` |

## Common MES / Controls Failure Modes

- **Connectivity**: Wrong host/port in config, firewall, VPN, or service not listening.
- **Credentials**: Expired or wrong keys/passwords; check keyring or vault usage in code.
- **Config drift**: Host-specific path or URL not set (e.g. different on dev vs prod).
- **State/DB**: Corrupt or stale state; need restart or DB repair (always suggest backup first).
- **Simulation vs real**: Code path for "simulation mode" vs real hardware; confirm which mode the environment uses.

## Where to Look in an MES Codebase

- **Config**: `settings.json`, `.env`, app-specific config dirs (paths, device addresses, feature flags).
- **Services**: Long-running processes that talk to PLCs, DB, or other services (start/stop scripts, systemd units).
- **HAL / device layer**: Abstractions for hardware or sim; adapters for OPC, Modbus, PyVisa, etc.
- **Logging**: Central logging module or utilities; log level and log file paths in config.
- **Recipes / work orders**: How lots and jobs are defined and executed; often in a dedicated domain or service.

## Safe Fix Sequence

1. Run read-only diagnostics (see table above).
2. Propose a single, minimal change (one config key, one line, one restart).
3. Show exact command or diff; ask user to confirm before running destructive or wide-ranging commands.
4. Suggest verification step (re-run job, check log line, check UI).
