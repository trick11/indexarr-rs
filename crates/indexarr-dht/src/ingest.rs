use std::sync::Arc;
use std::time::Instant;

use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::{DhtSharedState, DiscoveredHash};

const BATCH_SIZE: usize = 500;
const FLUSH_INTERVAL_SECS: u64 = 5;
const STATS_INTERVAL_SECS: u64 = 60;

/// Background worker that drains the hash queue and inserts into the database.
pub async fn run_hash_ingest(pool: PgPool, shared: Arc<DhtSharedState>, cancel: CancellationToken) {
    tracing::info!("hash ingest worker started");

    let mut pending: Vec<DiscoveredHash> = Vec::with_capacity(BATCH_SIZE);
    let mut last_flush = Instant::now();
    let mut last_stats = Instant::now();
    let mut total_inserted = 0u64;
    let mut total_updated = 0u64;
    let mut consecutive_errors = 0u32;

    loop {
        if cancel.is_cancelled() {
            // Final flush
            if !pending.is_empty() {
                let (ins, upd) = flush_batch(&pool, &pending).await;
                total_inserted += ins;
                total_updated += upd;
            }
            break;
        }

        // Drain queue
        let drained = shared.drain_hashes(BATCH_SIZE);
        pending.extend(drained);

        // Flush when batch is full or interval elapsed
        let should_flush = pending.len() >= BATCH_SIZE
            || (last_flush.elapsed().as_secs() >= FLUSH_INTERVAL_SECS && !pending.is_empty());

        if should_flush {
            let (ins, upd) = flush_batch(&pool, &pending).await;
            if ins + upd > 0 {
                total_inserted += ins;
                total_updated += upd;
                consecutive_errors = 0;
            }
            pending.clear();
            last_flush = Instant::now();
        }

        // Periodic stats
        if last_stats.elapsed().as_secs() >= STATS_INTERVAL_SECS {
            tracing::info!(
                inserted = total_inserted,
                updated = total_updated,
                queue = shared.hash_queue.lock().len(),
                cache = shared.peer_cache.len(),
                "ingest stats"
            );
            last_stats = Instant::now();
        }

        // Circuit breaker on DB errors
        if consecutive_errors > 20 {
            tracing::warn!("ingest circuit breaker — sleeping 60s");
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            consecutive_errors = 0;
            continue;
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    tracing::info!(total_inserted, total_updated, "hash ingest worker stopped");
}

/// Flush a batch of discovered hashes to the database.
async fn flush_batch(pool: &PgPool, hashes: &[DiscoveredHash]) -> (u64, u64) {
    let mut inserted = 0u64;
    let mut updated = 0u64;

    for hash in hashes {
        if hash.info_hash.len() != 40 {
            continue;
        }

        match upsert_hash(pool, hash).await {
            Ok(Some(is_new)) => {
                if is_new {
                    inserted += 1;
                } else {
                    updated += 1;
                }
            }
            Ok(None) => {}
            Err(e) => {
                tracing::debug!(error = %e, hash = %hash.info_hash, "failed to upsert hash");
            }
        }
    }

    (inserted, updated)
}

/// Insert or update a discovered hash. Returns true if new.
pub(crate) async fn upsert_hash(
    pool: &PgPool,
    hash: &DiscoveredHash,
) -> Result<Option<bool>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(&hash.info_hash)
        .execute(&mut *tx)
        .await?;
    // Try insert first
    let result = sqlx::query(
        "INSERT INTO torrents (info_hash, source, discovered_at, observations) \
         SELECT $1, $2, NOW(), 1 WHERE NOT EXISTS \
           (SELECT 1 FROM local_category_rejections WHERE info_hash = $1) \
         ON CONFLICT (info_hash) DO UPDATE SET \
           observations = torrents.observations + 1, \
           source = CASE \
             WHEN $2 = 'announce' THEN 'announce' \
             ELSE torrents.source \
           END \
         RETURNING (xmax = 0) AS is_new",
    )
    .bind(&hash.info_hash)
    .bind(&hash.source)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;

    use sqlx::Row;
    Ok(result.map(|row| row.get::<bool, _>("is_new")))
}
