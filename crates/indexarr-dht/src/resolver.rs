use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bencode::{ByteBufOwned, from_bytes};
use indexarr_resolver_v2::{
    DEFAULT_MAX_CONCURRENT_PEERS, FetchConfig, FetchPeersError, MAX_METADATA_SIZE,
    PeerFailureSummary, fetch_from_peers,
};
use librtbit_core::hash_id::Id20;
use librtbit_core::torrent_metainfo::TorrentMetaV1Info;
use sqlx::{PgPool, Row};
use tokio_util::sync::CancellationToken;

use crate::DhtSharedState;
use crate::engine::DhtEngine;
use crate::tracker_announce::SharedTrackerDiscovery;

/// Metadata resolver — fetches torrent metadata via BEP 9 and runs content pipeline.
pub struct MetadataResolver {
    pool: PgPool,
    shared: Arc<DhtSharedState>,
    engine: Arc<DhtEngine>,
    workers: usize,
    timeout_secs: u64,
    save_files_threshold: u32,
    local_import_categories: Vec<String>,
    cancel: CancellationToken,
    /// Stable peer_id shared with `TrackerDiscovery` so trackers and peers see
    /// the same identity from this resolver instance.
    peer_id: Id20,
    /// Tracker-driven peer discovery (HTTP + UDP popular public trackers).
    tracker_discovery: SharedTrackerDiscovery,
}

#[derive(Debug, Clone)]
struct ResolveJob {
    info_hash: String,
    source: String,
    attempt: i32,
}

const NEW_BEP51_NUMERATOR: usize = 1;
const NEW_BEP51_DENOMINATOR: usize = 2;
const PRIORITY_UPLOAD_NUMERATOR: usize = 1;
const PRIORITY_UPLOAD_DENOMINATOR: usize = 4;

impl MetadataResolver {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pool: PgPool,
        shared: Arc<DhtSharedState>,
        engine: Arc<DhtEngine>,
        workers: usize,
        timeout_secs: u64,
        save_files_threshold: u32,
        local_import_categories: Vec<String>,
        cancel: CancellationToken,
        peer_id: Id20,
        tracker_discovery: SharedTrackerDiscovery,
    ) -> Self {
        Self {
            pool,
            shared,
            engine,
            workers,
            timeout_secs,
            save_files_threshold,
            local_import_categories,
            cancel,
            peer_id,
            tracker_discovery,
        }
    }

    /// Main resolver loop.
    pub async fn run(&self) {
        tracing::info!(workers = self.workers, "metadata resolver started");

        // Wait for DHT routing tables to populate
        loop {
            if self.cancel.is_cancelled() {
                return;
            }
            let stats = self.engine.stats();
            if stats.total_routing_nodes >= 20 {
                tracing::info!(
                    nodes = stats.total_routing_nodes,
                    "routing table ready, starting resolver"
                );
                break;
            }
            tracing::debug!(
                nodes = stats.total_routing_nodes,
                "waiting for routing table..."
            );
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }

        let mut last_stats = Instant::now();
        let _resolved_count = 0u64;
        let _failed_count = 0u64;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.workers));

        loop {
            if self.cancel.is_cancelled() {
                break;
            }

            // Get unresolved hashes to work on
            let batch = self.claim_unresolved_batch(self.workers * 2).await;

            if batch.is_empty() {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                continue;
            }

            // Discover peers for the batch via DHT
            let hash_strings: Vec<String> = batch.iter().map(|job| job.info_hash.clone()).collect();
            let discovered = self.engine.discover_peers(&hash_strings).await;

            // Also check cached peers
            for hash in &hash_strings {
                if !discovered.contains_key(hash) {
                    let cached = self.shared.get_peers(hash);
                    if !cached.is_empty() {
                        let addrs: Vec<SocketAddr> = cached
                            .iter()
                            .filter_map(|(ip, port)| format!("{ip}:{port}").parse().ok())
                            .collect();
                        if !addrs.is_empty() {
                            discovered.insert(hash.clone(), addrs);
                        }
                    }
                }
            }

            // Resolve EVERY hash in the batch — even those with no DHT/cache
            // peers. The spawn task announces against the tracker list and
            // can rescue hashes that DHT couldn't find peers for.
            for job in batch {
                if self.cancel.is_cancelled() {
                    break;
                }
                let hash = job.info_hash.clone();
                let peers: Vec<SocketAddr> = discovered
                    .get(&hash)
                    .map(|e| e.value().clone())
                    .unwrap_or_default();

                let permit = match semaphore.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break,
                };

                let pool = self.pool.clone();
                let timeout = self.timeout_secs;
                let threshold = self.save_files_threshold;
                let local_categories = self.local_import_categories.clone();
                let cancel = self.cancel.clone();
                let peer_id = self.peer_id;
                let tracker_discovery = self.tracker_discovery.clone();
                let shared = self.shared.clone();

                tokio::spawn(async move {
                    let _permit = permit;
                    if cancel.is_cancelled() {
                        return;
                    }

                    let started = Instant::now();

                    // Augment the DHT/cache-discovered peer set with addresses
                    // harvested from announces against popular public trackers.
                    // Run in parallel with the (already-completed) DHT discovery
                    // — bounded to 5s so a slow tracker doesn't stall the fetch.
                    let mut peers = peers;
                    if let Ok(id20) = Id20::from_str(&hash) {
                        let tracker_peers = tracker_discovery
                            .discover_peers(id20, std::time::Duration::from_secs(5))
                            .await;
                        if !tracker_peers.is_empty() {
                            tracing::debug!(
                                hash = %hash,
                                dht = peers.len(),
                                trackers = tracker_peers.len(),
                                "tracker announce complete"
                            );
                            // Dedupe via HashSet, then collect.
                            let mut seen: std::collections::HashSet<SocketAddr> =
                                peers.iter().copied().collect();
                            for addr in tracker_peers {
                                if seen.insert(addr) {
                                    peers.push(addr);
                                }
                            }
                        }
                    }

                    match fetch_metadata(&hash, &peers, timeout, peer_id).await {
                        Ok((meta, harvested_peers, harvested_trackers, prior_failures)) => {
                            // Free peers from the winning peer's ut_pex messages —
                            // fold them into shared.peer_cache for the next batch.
                            let cached = if !harvested_peers.is_empty() {
                                shared.cache_peers(&hash, harvested_peers.iter().copied())
                            } else {
                                0
                            };
                            tracing::info!(
                                hash = %hash,
                                source = %job.source,
                                attempt = job.attempt,
                                peers = peers.len(),
                                size = meta.size,
                                files = meta.files.len(),
                                pex = harvested_peers.len(),
                                pex_cached = cached,
                                lt_tex = harvested_trackers.len(),
                                "BEP 9 fetch ok"
                            );
                            let outcome =
                                process_resolved(&pool, &hash, &meta, threshold, &local_categories)
                                    .await;
                            if matches!(outcome, Ok(ProcessOutcome::Rejected)) {
                                shared.peer_cache.remove(&hash);
                                return;
                            }
                            if let Err(error) = outcome {
                                let retry_at = schedule_retry(&pool, &job).await;
                                record_attempt(
                                    &pool,
                                    &job,
                                    peers.len(),
                                    false,
                                    PeerFailureSummary {
                                        total: 1,
                                        other: 1,
                                        ..Default::default()
                                    },
                                    started.elapsed(),
                                    Some(&error.to_string()),
                                )
                                .await;
                                tracing::warn!(
                                    hash = %hash,
                                    source = %job.source,
                                    attempt = job.attempt,
                                    retry_at = ?retry_at,
                                    error = %error,
                                    "failed to store metadata"
                                );
                                return;
                            }
                            record_attempt(
                                &pool,
                                &job,
                                peers.len(),
                                true,
                                prior_failures,
                                started.elapsed(),
                                None,
                            )
                            .await;
                            if !harvested_trackers.is_empty() {
                                let _ = merge_trackers(&pool, &hash, &harvested_trackers)
                                    .await
                                    .map_err(|e| tracing::warn!(hash = %hash, error = %e, "failed to merge lt_tex trackers"));
                            }
                        }
                        Err(e) => {
                            let retry_at = schedule_retry(&pool, &job).await;
                            record_attempt(
                                &pool,
                                &job,
                                peers.len(),
                                false,
                                e.summary,
                                started.elapsed(),
                                Some(&e.last.to_string()),
                            )
                            .await;
                            tracing::info!(
                                hash = %hash,
                                source = %job.source,
                                attempt = job.attempt,
                                peers = peers.len(),
                                connect_fail = e.summary.connect,
                                no_bep10 = e.summary.no_bep10,
                                timeout_fail = e.summary.timeout,
                                other_fail = e.summary.other,
                                retry_at = ?retry_at,
                                error = %e,
                                "BEP 9 fetch failed"
                            );
                        }
                    }
                });
            }

            // Stats
            if last_stats.elapsed().as_secs() >= 30 {
                let _ = sqlx::query(
                    "DELETE FROM resolver_attempt_events \
                     WHERE created_at < NOW() - INTERVAL '7 days'",
                )
                .execute(&self.pool)
                .await;
                let total: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM torrents WHERE resolved_at IS NOT NULL",
                )
                .fetch_one(&self.pool)
                .await
                .unwrap_or(0);
                tracing::info!(total_resolved = total, "resolver stats");
                last_stats = Instant::now();
            }

            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }

        tracing::info!("metadata resolver stopped");
    }

    /// Atomically claim a fair batch of resolver work.
    ///
    /// Half of each batch is reserved for never-attempted live BEP 51
    /// discoveries. Priority uploads are retained but capped at one quarter,
    /// preventing a stale manual backlog from starving the crawler. The
    /// remaining lane serves retries and other discovery sources. Unused
    /// capacity in any lane flows to general non-upload work.
    async fn claim_unresolved_batch(&self, limit: usize) -> Vec<ResolveJob> {
        let (bep51_limit, upload_limit, general_limit) = fair_lane_limits(limit);
        sqlx::query(
            "WITH \
             new_bep51 AS ( \
               SELECT info_hash FROM torrents \
               WHERE resolved_at IS NULL AND source = 'bep51' \
                 AND resolve_attempts = 0 \
                 AND (retry_after IS NULL OR retry_after <= NOW()) \
               ORDER BY observations DESC, discovered_at DESC \
               FOR UPDATE SKIP LOCKED LIMIT $1 \
             ), \
             priority_upload AS ( \
               SELECT info_hash FROM torrents \
               WHERE resolved_at IS NULL AND source = 'upload' AND priority IS TRUE \
                 AND (retry_after IS NULL OR retry_after <= NOW()) \
               ORDER BY resolve_attempts ASC, observations DESC, discovered_at DESC \
               FOR UPDATE SKIP LOCKED LIMIT $2 \
             ), \
             general AS ( \
               SELECT info_hash FROM torrents \
               WHERE resolved_at IS NULL AND source != 'upload' \
                 AND (retry_after IS NULL OR retry_after <= NOW()) \
                 AND info_hash NOT IN (SELECT info_hash FROM new_bep51) \
               ORDER BY CASE WHEN source = 'announce' THEN 0 ELSE 1 END, \
                        resolve_attempts ASC, observations DESC, discovered_at DESC \
               FOR UPDATE SKIP LOCKED LIMIT $3 \
             ), \
             overflow AS ( \
               SELECT info_hash FROM torrents \
               WHERE resolved_at IS NULL AND source != 'upload' \
                 AND (retry_after IS NULL OR retry_after <= NOW()) \
                 AND info_hash NOT IN (SELECT info_hash FROM new_bep51) \
                 AND info_hash NOT IN (SELECT info_hash FROM general) \
               ORDER BY resolve_attempts ASC, observations DESC, discovered_at DESC \
               FOR UPDATE SKIP LOCKED \
               LIMIT GREATEST(0, $4 - (SELECT COUNT(*) FROM new_bep51) \
                                      - (SELECT COUNT(*) FROM priority_upload) \
                                      - (SELECT COUNT(*) FROM general)) \
             ), \
             selected AS ( \
               SELECT info_hash, 0 AS lane FROM new_bep51 \
               UNION ALL SELECT info_hash, 1 AS lane FROM priority_upload \
               UNION ALL SELECT info_hash, 2 AS lane FROM general \
               UNION ALL SELECT info_hash, 3 AS lane FROM overflow \
             ), \
             ranked AS ( \
               SELECT info_hash, lane, row_number() OVER (PARTITION BY lane ORDER BY info_hash) AS lane_row \
               FROM selected \
             ), \
             claimed AS ( \
               UPDATE torrents t \
               SET resolve_attempts = t.resolve_attempts + 1, \
                   retry_after = NOW() + INTERVAL '10 minutes' \
               FROM ranked r WHERE t.info_hash = r.info_hash \
               RETURNING t.info_hash, t.source, t.resolve_attempts, r.lane, r.lane_row \
             ) \
             SELECT info_hash, source, resolve_attempts \
             FROM claimed ORDER BY lane_row, lane LIMIT $4",
        )
        .bind(bep51_limit as i64)
        .bind(upload_limit as i64)
        .bind(general_limit as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|row| ResolveJob {
            info_hash: row.get("info_hash"),
            source: row.get("source"),
            attempt: row.get("resolve_attempts"),
        })
        .collect()
    }
}

fn fair_lane_limits(limit: usize) -> (usize, usize, usize) {
    if limit == 0 {
        return (0, 0, 0);
    }
    let bep51 = (limit * NEW_BEP51_NUMERATOR / NEW_BEP51_DENOMINATOR).max(1);
    let uploads = (limit * PRIORITY_UPLOAD_NUMERATOR / PRIORITY_UPLOAD_DENOMINATOR).max(1);
    let general = limit.saturating_sub(bep51 + uploads);
    (bep51, uploads, general)
}

async fn record_attempt(
    pool: &PgPool,
    job: &ResolveJob,
    candidate_peers: usize,
    success: bool,
    failures: PeerFailureSummary,
    duration: Duration,
    last_error: Option<&str>,
) {
    let _ = sqlx::query(
        "INSERT INTO resolver_attempt_events \
         (info_hash, source, attempt, success, candidate_peers, connect_fail, \
          no_bep10, timeout_fail, other_fail, duration_ms, last_error) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
    )
    .bind(&job.info_hash)
    .bind(&job.source)
    .bind(job.attempt)
    .bind(success)
    .bind(candidate_peers as i32)
    .bind(failures.connect as i32)
    .bind(failures.no_bep10 as i32)
    .bind(failures.timeout as i32)
    .bind(failures.other as i32)
    .bind(duration.as_millis() as i64)
    .bind(last_error)
    .execute(pool)
    .await;
}

/// Replace the claim lease with the actual exponential retry time after an
/// attempt finishes. Until this runs, the 10-minute lease prevents another
/// resolver loop (or process) from claiming the same in-flight hash.
async fn schedule_retry(pool: &PgPool, job: &ResolveJob) -> Option<chrono::DateTime<chrono::Utc>> {
    sqlx::query_scalar(
        "UPDATE torrents \
         SET retry_after = NOW() + ($2 * INTERVAL '1 second') \
         WHERE info_hash = $1 AND resolved_at IS NULL \
         RETURNING retry_after",
    )
    .bind(&job.info_hash)
    .bind(retry_delay_secs(job.attempt))
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
}

fn retry_delay_secs(attempt: i32) -> i32 {
    let exponent = (attempt - 1).clamp(0, 12) as u32;
    30_i32.saturating_mul(2_i32.pow(exponent)).min(86_400)
}

/// Resolved metadata from a peer.
#[derive(Debug)]
struct ResolvedMeta {
    name: String,
    size: i64,
    files: Vec<FileEntry>,
    is_private: bool,
    piece_length: Option<i32>,
    piece_count: Option<i32>,
    seed_count: i32,
    peer_count: i32,
}

#[derive(Debug)]
struct FileEntry {
    path: String,
    size: i64,
}

/// Fetch metadata for an info_hash from a list of candidate peers via BEP 9.
///
/// Returns `(meta, harvested_peers, harvested_trackers)`.
async fn fetch_metadata(
    info_hash: &str,
    peers: &[SocketAddr],
    timeout_secs: u64,
    peer_id: Id20,
) -> Result<
    (
        ResolvedMeta,
        Vec<SocketAddr>,
        Vec<String>,
        PeerFailureSummary,
    ),
    FetchPeersError,
> {
    if peers.is_empty() {
        return Err(fetch_error(
            std::io::ErrorKind::AddrNotAvailable,
            format!("no peers available for {info_hash}"),
        ));
    }

    let id = Id20::from_str(info_hash).map_err(|e| {
        fetch_error(
            std::io::ErrorKind::InvalidInput,
            format!("invalid info_hash: {e}"),
        )
    })?;
    let cfg = FetchConfig {
        timeout: Duration::from_secs(timeout_secs.max(1)),
        max_metadata_size: MAX_METADATA_SIZE,
    };

    let fetched = fetch_from_peers(id, peers, peer_id, cfg, DEFAULT_MAX_CONCURRENT_PEERS).await?;

    let meta = parse_info_dict(&fetched.bytes).map_err(|e| FetchPeersError {
        last: indexarr_resolver_v2::ResolverError::Connect(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("info dict parse failed after BEP 9 fetch: {e}"),
        )),
        summary: PeerFailureSummary {
            total: 1,
            other: 1,
            ..Default::default()
        },
    })?;
    Ok((
        meta,
        fetched.harvested_peers,
        fetched.harvested_trackers,
        fetched.prior_peer_failures,
    ))
}

fn fetch_error(kind: std::io::ErrorKind, message: String) -> FetchPeersError {
    FetchPeersError {
        last: indexarr_resolver_v2::ResolverError::Connect(std::io::Error::new(kind, message)),
        summary: PeerFailureSummary::default(),
    }
}

/// Merge tracker URLs harvested via BEP 28 (lt_tex) into the DB for
/// `info_hash`. De-duplication is handled in SQL — we append only URLs not
/// already present in the existing JSONB array.
async fn merge_trackers(
    pool: &PgPool,
    info_hash: &str,
    trackers: &[String],
) -> Result<(), sqlx::Error> {
    let json = serde_json::to_value(trackers).unwrap_or(serde_json::Value::Array(vec![]));
    sqlx::query(
        "UPDATE torrents \
         SET trackers = ( \
           SELECT jsonb_agg(DISTINCT t) \
           FROM jsonb_array_elements( \
             COALESCE(trackers, '[]'::jsonb) || $2::jsonb \
           ) AS t \
         ) \
         WHERE info_hash = $1",
    )
    .bind(info_hash)
    .bind(&json)
    .execute(pool)
    .await?;
    Ok(())
}

/// Parse a bencoded `info` dict into the resolver's `ResolvedMeta` shape.
fn parse_info_dict(bytes: &[u8]) -> Result<ResolvedMeta, String> {
    let info: TorrentMetaV1Info<ByteBufOwned> =
        from_bytes(bytes).map_err(|e| format!("bencode decode: {e}"))?;

    let name = info
        .name
        .as_ref()
        .map(|b| String::from_utf8_lossy(b.as_ref()).into_owned())
        .unwrap_or_default();

    let mut files: Vec<FileEntry> = Vec::new();
    let mut total_size: i64 = 0;

    if let Some(file_list) = &info.files {
        // Multi-file torrent.
        for f in file_list {
            let path = f
                .path
                .iter()
                .map(|seg| String::from_utf8_lossy(seg.as_ref()).into_owned())
                .collect::<Vec<_>>()
                .join("/");
            let size = f.length as i64;
            total_size = total_size.saturating_add(size);
            files.push(FileEntry { path, size });
        }
    } else if let Some(length) = info.length {
        // Single-file torrent.
        total_size = length as i64;
        files.push(FileEntry {
            path: name.clone(),
            size: total_size,
        });
    }

    let piece_count = info.pieces.as_ref().map(|p| (p.as_ref().len() / 20) as i32);
    let piece_length = i32::try_from(info.piece_length).ok();

    Ok(ResolvedMeta {
        name,
        size: total_size,
        files,
        is_private: info.private,
        piece_length,
        piece_count,
        // BEP 9 doesn't carry seed/peer counts — left at 0 here. Caller's
        // SQL update uses GREATEST(seed_count, $5) so existing values from the
        // tracker scraper are preserved.
        seed_count: 0,
        peer_count: 0,
    })
}

/// Process resolved metadata — store in DB and run content pipeline.
#[derive(Debug, PartialEq, Eq)]
enum ProcessOutcome {
    Stored,
    Rejected,
}

async fn process_resolved(
    pool: &PgPool,
    info_hash: &str,
    meta: &ResolvedMeta,
    save_files_threshold: u32,
    local_categories: &[String],
) -> Result<ProcessOutcome, sqlx::Error> {
    // Parse and classify
    let parsed = indexarr_parser::parse(&meta.name);
    let file_infos: Vec<indexarr_classifier::FileInfo> = meta
        .files
        .iter()
        .map(|f| indexarr_classifier::FileInfo {
            path: f.path.clone(),
            size: f.size,
            extension: f.path.rsplit('.').next().map(|s| s.to_string()),
        })
        .collect();

    let classification = indexarr_classifier::classify(&parsed, &file_infos, &meta.name);
    let quality_score = indexarr_classifier::compute_quality_score(&parsed);
    let mut tx = pool.begin().await?;

    if !local_categories.is_empty() && !local_categories.contains(&classification.content_type) {
        // Serialize against hash ingestion so an observation already in flight
        // cannot resurrect the row after its metadata has been discarded.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(info_hash)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT INTO local_category_rejections (info_hash, content_type) \
             VALUES ($1, $2) ON CONFLICT (info_hash) DO UPDATE \
             SET content_type = EXCLUDED.content_type",
        )
        .bind(info_hash)
        .bind(&classification.content_type)
        .execute(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM torrents WHERE info_hash = $1")
            .bind(info_hash)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        tracing::info!(hash = %info_hash, content_type = %classification.content_type,
            "discarded local metadata outside category allowlist");
        return Ok(ProcessOutcome::Rejected);
    }

    // Update torrent record
    sqlx::query(
        "UPDATE torrents SET \
           name = $2, size = $3, resolved_at = NOW(), private = $4, \
           seed_count = GREATEST(seed_count, $5), peer_count = GREATEST(peer_count, $6), \
           no_peers = FALSE, priority = FALSE, \
           piece_length = $7, piece_count = $8 \
         WHERE info_hash = $1",
    )
    .bind(info_hash)
    .bind(&meta.name)
    .bind(meta.size)
    .bind(meta.is_private)
    .bind(meta.seed_count)
    .bind(meta.peer_count)
    .bind(meta.piece_length)
    .bind(meta.piece_count)
    .execute(&mut *tx)
    .await?;

    // Store files (if under threshold)
    if meta.files.len() <= save_files_threshold as usize {
        for file in &meta.files {
            let ext = file_extension(&file.path);
            sqlx::query(
                "INSERT INTO torrent_files (info_hash, path, size, extension) VALUES ($1, $2, $3, $4)"
            )
            .bind(info_hash)
            .bind(&file.path)
            .bind(file.size)
            .bind(&ext)
            .execute(&mut *tx)
            .await?;
        }
    }

    // Store content classification
    sqlx::query(
        "INSERT INTO torrent_content (info_hash, content_type, title, year, season, episode, \
         \"group\", language, resolution, codec, video_source, modifier, is_3d, hdr, \
         audio_codec, audio_channels, edition, bit_depth, network, quality_score, \
         is_dubbed, is_complete, is_remastered, is_scene, is_proper, is_repack, \
         platform, has_subtitles, is_anime, music_format, classified_at, classifier_version) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, \
                 $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, NOW(), '0.1.0') \
         ON CONFLICT (info_hash) DO UPDATE SET \
           content_type = EXCLUDED.content_type, title = EXCLUDED.title, year = EXCLUDED.year, \
           quality_score = EXCLUDED.quality_score, classified_at = NOW()"
    )
    .bind(info_hash)
    .bind(&classification.content_type)
    .bind(&parsed.title)
    .bind(parsed.year)
    .bind(parsed.season)
    .bind(parsed.episode)
    .bind(&parsed.group)
    .bind(parsed.languages.first().map(|s| s.as_str()))
    .bind(&parsed.resolution)
    .bind(&parsed.codec)
    .bind(&parsed.video_source)
    .bind(&parsed.modifier)
    .bind(parsed.is_3d)
    .bind(parsed.hdr.first().map(|s| s.as_str()))
    .bind(parsed.audio_codecs.first().map(|s| s.as_str()))
    .bind(&parsed.audio_channels)
    .bind(&parsed.edition)
    .bind(&parsed.bit_depth)
    .bind(&parsed.network)
    .bind(quality_score)
    .bind(parsed.is_dubbed)
    .bind(parsed.is_complete)
    .bind(parsed.is_remastered)
    .bind(parsed.is_scene)
    .bind(parsed.is_proper)
    .bind(parsed.is_repack)
    .bind(classification.platform.or(parsed.platform.clone()))
    .bind(classification.has_subtitles || parsed.has_subtitles)
    .bind(classification.is_anime)
    .bind(&classification.music_format)
    .execute(&mut *tx)
    .await?;

    // Store tags
    for tag in &classification.tags {
        sqlx::query(
            "INSERT INTO torrent_tags (info_hash, tag, source) VALUES ($1, $2, 'classifier') \
             ON CONFLICT (info_hash, tag) DO NOTHING",
        )
        .bind(info_hash)
        .bind(tag)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;

    tracing::debug!(
        hash = %info_hash,
        name = %meta.name,
        content_type = %classification.content_type,
        quality = quality_score,
        "resolved torrent"
    );

    Ok(ProcessOutcome::Stored)
}

fn file_extension(path: &str) -> Option<String> {
    let file_name = path.rsplit(['/', '\\']).next()?;
    let (_, extension) = file_name.rsplit_once('.')?;
    (!extension.is_empty()).then(|| extension.to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::{
        FileEntry, ProcessOutcome, ResolvedMeta, fair_lane_limits, file_extension,
        process_resolved, retry_delay_secs,
    };

    /// Run against an isolated PostgreSQL database, never a production database.
    #[tokio::test]
    #[ignore = "requires INDEXARR_TEST_DB_URL pointing at a disposable database"]
    async fn local_category_filter_persists_only_allowed_metadata() {
        let mut settings = indexarr_core::config::Settings::from_env();
        settings.db_url = std::env::var("INDEXARR_TEST_DB_URL").expect("test database URL");
        settings.local_import_categories = vec!["movie".into(), "tv_show".into()];
        let pool = indexarr_core::db::init_db(&settings).await.unwrap();
        for (n, name, path, expected) in [
            (
                1,
                "Example.Movie.2024.1080p.BluRay.x264",
                "movie.mkv",
                "movie",
            ),
            (
                2,
                "Example.Show.S01E01.1080p.WEB-DL",
                "episode.mkv",
                "tv_show",
            ),
            (3, "Example.Album.FLAC", "track.flac", "music"),
            (4, "Example.XXX.1080p", "video.mp4", "xxx"),
            (5, "Unclassified", "README", "unknown"),
        ] {
            let hash = format!("{n:040x}");
            let observation = crate::DiscoveredHash {
                info_hash: hash.clone(),
                source: "bep51".into(),
                peer_ip: None,
                peer_port: None,
            };
            assert_eq!(
                crate::ingest::upsert_hash(&pool, &observation)
                    .await
                    .unwrap(),
                Some(true)
            );
            let meta = ResolvedMeta {
                name: name.into(),
                size: 1000,
                files: vec![FileEntry {
                    path: path.into(),
                    size: 1000,
                }],
                is_private: false,
                piece_length: None,
                piece_count: None,
                seed_count: 3,
                peer_count: 4,
            };
            let outcome =
                process_resolved(&pool, &hash, &meta, 200, &settings.local_import_categories)
                    .await
                    .unwrap();
            let allowed = expected == "movie" || expected == "tv_show";
            assert_eq!(
                outcome,
                if allowed {
                    ProcessOutcome::Stored
                } else {
                    ProcessOutcome::Rejected
                }
            );
            let stored: Option<String> =
                sqlx::query_scalar("SELECT content_type FROM torrent_content WHERE info_hash=$1")
                    .bind(&hash)
                    .fetch_optional(&pool)
                    .await
                    .unwrap();
            let rejected: Option<String> = sqlx::query_scalar(
                "SELECT content_type FROM local_category_rejections WHERE info_hash=$1",
            )
            .bind(&hash)
            .fetch_optional(&pool)
            .await
            .unwrap();
            assert_eq!(
                stored.as_deref(),
                if allowed { Some(expected) } else { None }
            );
            assert_eq!(
                rejected.as_deref(),
                if allowed { None } else { Some(expected) }
            );
            if !allowed {
                // Neither rediscovery nor a second simultaneous rejection can
                // leave a torrent row eligible for repeated resolution.
                let (rediscovered, repeated) = tokio::join!(
                    crate::ingest::upsert_hash(&pool, &observation),
                    process_resolved(&pool, &hash, &meta, 200, &settings.local_import_categories),
                );
                assert_eq!(rediscovered.unwrap(), None);
                assert_eq!(repeated.unwrap(), ProcessOutcome::Rejected);
                for query in [
                    "SELECT count(*) FROM torrents WHERE info_hash=$1",
                    "SELECT count(*) FROM torrent_content WHERE info_hash=$1",
                    "SELECT count(*) FROM torrent_files WHERE info_hash=$1",
                    "SELECT count(*) FROM torrent_tags WHERE info_hash=$1",
                ] {
                    let count: i64 = sqlx::query_scalar(query)
                        .bind(&hash)
                        .fetch_one(&pool)
                        .await
                        .unwrap();
                    assert_eq!(count, 0, "rejected metadata leaked: {query}");
                }
            }
        }
        settings.local_import_categories.push("music".into());
        let reopened = indexarr_core::db::init_db(&settings).await.unwrap();
        let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM local_category_rejections")
            .fetch_one(&reopened)
            .await
            .unwrap();
        assert_eq!(
            remaining, 2,
            "widening allowlist must release only matching rejections"
        );
        settings.local_import_categories.clear();
        let unrestricted = indexarr_core::db::init_db(&settings).await.unwrap();
        let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM local_category_rejections")
            .fetch_one(&unrestricted)
            .await
            .unwrap();
        assert_eq!(
            remaining, 0,
            "disabling filter must release all rejected hashes"
        );
        let hash = format!("{:040x}", 4);
        let observation = crate::DiscoveredHash {
            info_hash: hash.clone(),
            source: "bep51".into(),
            peer_ip: None,
            peer_port: None,
        };
        assert_eq!(
            crate::ingest::upsert_hash(&pool, &observation)
                .await
                .unwrap(),
            Some(true)
        );
        let meta = ResolvedMeta {
            name: "Example.XXX.1080p".into(),
            size: 1000,
            files: vec![FileEntry {
                path: "video.mp4".into(),
                size: 1000,
            }],
            is_private: false,
            piece_length: None,
            piece_count: None,
            seed_count: 0,
            peer_count: 0,
        };
        assert_eq!(
            process_resolved(&pool, &hash, &meta, 200, &[])
                .await
                .unwrap(),
            ProcessOutcome::Stored
        );
    }

    #[test]
    fn fair_scheduler_reserves_live_and_upload_lanes() {
        assert_eq!(fair_lane_limits(40), (20, 10, 10));
    }

    #[test]
    fn fair_scheduler_handles_small_and_empty_batches() {
        assert_eq!(fair_lane_limits(0), (0, 0, 0));
        assert_eq!(fair_lane_limits(1), (1, 1, 0));
        assert_eq!(fair_lane_limits(2), (1, 1, 0));
    }

    #[test]
    fn retry_backoff_starts_after_completion_and_caps_at_one_day() {
        assert_eq!(retry_delay_secs(1), 30);
        assert_eq!(retry_delay_secs(7), 1_920);
        assert_eq!(retry_delay_secs(13), 86_400);
        assert_eq!(retry_delay_secs(100), 86_400);
    }

    #[test]
    fn file_extensions_require_a_real_suffix() {
        assert_eq!(file_extension("folder/video.MKV").as_deref(), Some("mkv"));
        assert_eq!(
            file_extension(r"folder\\archive.tar.gz").as_deref(),
            Some("gz")
        );
        assert_eq!(file_extension("README"), None);
        assert_eq!(file_extension("folder.with.dots/README"), None);
        assert_eq!(file_extension("trailing."), None);
    }
}
