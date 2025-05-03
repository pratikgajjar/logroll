## Logroll

It reads postgres cdc logs and roll them into compressed parquet files and push them to s3.

## Usage

```bash
logroll --config config.yaml
```

## Configuration

```yaml
logroll:
  postgres:
    uri: postgres://user:password@host:port/database
    slot: logroll
    tick: 10s
  s3:
    endpoint: s3.amazonaws.com
    bucket: logroll
    prefix: logroll
      