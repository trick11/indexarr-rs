# Local category filtering

This custom branch is based on v0.3.2 plus the upstream migration from the
retired private Cargo registry to public, locked GitHub dependencies.

Set `INDEXARR_LOCAL_IMPORT_CATEGORIES=movie,tv_show` in the Portainer stack
and run the `dht_crawler` and `resolver` workers alongside `http_server,sync`.
An unset or empty variable preserves upstream behavior (keep every category).
This is independent of `INDEXARR_SYNC_IMPORT_CATEGORIES` and the Sync UI.
Changes take effect when the container restarts.

The `custom/local-category-filter` branch in `trick11/indexarr-rs` publishes
`ghcr.io/trick11/indexarr-custom` through the Custom Indexarr container workflow.
Production should pin the `sha-<commit>` tag; `latest` is provided for manual
updates. Automatic image replacement is deliberately disabled in Portainer.

After metadata is fetched and classified, disallowed entries are atomically
removed, including files, tags and other dependent data. Only the hash and
category remain in `local_category_rejections`, outside the searchable and
exported index. DHT ingestion checks this cache and serializes with rejection
using a per-hash transaction lock, preventing repeated resolution and
rediscovery races. Rejected records are not merged with trackers or recorded
as retryable failures. On startup, entries whose categories are now allowed
are removed from the rejection cache and can be discovered again.

Classification uses upstream rules and can misclassify ambiguous names.
The filter does not retrospectively remove already accepted content.

Database integration test (disposable database only):
`INDEXARR_TEST_DB_URL=postgres://indexarr:indexarr@postgres/indexarr_test cargo test -p indexarr-dht local_category_filter_persists_only_allowed_metadata -- --ignored`
