# CI-Visualization

## 1. Create a JSON config file:
```json
{
  "db-path": "/home/example/example.db",
  "auth-token": "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"
}
```
If the DB does not exist it will be created at that location.
You need the auth-token so github grants you 5000 instead of 60 API-requests
per hour. **Don't give it any rights**


## 2. Run the python script to fetch data to the DB
By calling it directly and let it run in the background
Here an example with `tmux`
```bash
$ tmux new -s civi
$ python3 -m civi <path-to-config.json>
$ # then you can just detach with ctrl + b then d
$ # and attach with tmux attach-session civi
```

Or by runnig it as a systemd service
WIP
