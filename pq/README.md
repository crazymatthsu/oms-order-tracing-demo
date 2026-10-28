# Persistent query definitions

One JSON file per `oms-*` PQ: the single source of truth for its type, dispatcher, script, heap,
schedule, restart policy, dependencies and environment variables (docs/design/07 section 2).
`${VAR}` values resolve from `deploy/deploy.env` when the definitions are applied; secrets are never
in these files (credentials reach a PQ as `KAFKA_PROP_*` variables or key files from the host's
secret store).

```bash
bash scripts/deploy_pqs.sh --dry-run    # the table to type into the Query Monitor the first time
bash scripts/deploy_pqs.sh              # create or update through the Core+ controller client
bash scripts/deploy_pqs.sh --export     # dhconfig pq export of every PQ into pq/export/ (keep versioned)
```

The AMPS ingesters (`oms-ingest-amps-<x>`) are defined disabled; enable one and disable its Kafka
twin when that system publishes to AMPS (docs/design/11). See docs/deployment.md.
