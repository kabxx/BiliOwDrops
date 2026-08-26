use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail};
use tokio_util::sync::CancellationToken;

use crate::domain::{BiliApi, CheckpointState, TaskCheckpoint, TaskProgress};

pub const MINIMUM_CHECKPOINT_STABLE_POLLS: u32 = 5;
pub const CONFIRMATION_POLLS: u32 = 2;
pub const CLAIM_RETRY_COOLDOWN: Duration = Duration::from_secs(120);
pub const CLAIM_SPACING: Duration = Duration::from_millis(1200);

#[derive(Debug, Default)]
pub struct RewardTracker {
    expected_tasks: HashSet<String>,
    known_checkpoints: HashMap<String, HashSet<String>>,
    stable_polls: HashMap<String, u32>,
    last_attempt: HashMap<String, Instant>,
    claim_submitted: HashSet<String>,
    consecutive_confirmed: u32,
    last_claim_at: Option<Instant>,
}

impl RewardTracker {
    pub fn new(task_ids: impl IntoIterator<Item = String>) -> Self {
        Self {
            expected_tasks: task_ids
                .into_iter()
                .map(|id| id.trim().to_string())
                .filter(|id| !id.is_empty())
                .collect(),
            ..Self::default()
        }
    }

    #[cfg(test)]
    pub fn claim_was_submitted(&self, checkpoint_id: &str) -> bool {
        self.claim_submitted.contains(checkpoint_id)
    }

    #[cfg(test)]
    pub fn consecutive_confirmed(&self) -> u32 {
        self.consecutive_confirmed
    }

    pub async fn process<A: BiliApi + ?Sized>(
        &mut self,
        cancel: &CancellationToken,
        api: &A,
        progress: &[TaskProgress],
    ) -> anyhow::Result<bool> {
        if progress.is_empty() {
            self.consecutive_confirmed = 0;
            return Ok(false);
        }
        let mut returned = HashSet::new();
        let mut all_confirmed = true;
        for task in progress {
            returned.insert(task.id.clone());
            if task.checkpoints.is_empty() {
                let point = TaskCheckpoint {
                    id: task.id.clone(),
                    key: task.task_key.clone(),
                    name: task.name.clone(),
                    current: task.current,
                    limit: task.limit,
                    state: state_from_task(task),
                    raw_status: task.raw_status,
                };
                if !is_claimed(point.state) {
                    all_confirmed = false;
                    if is_claimable(point.state) {
                        self.claim_if_due(cancel, api, &point).await?;
                    }
                }
                continue;
            }
            let mut points = task.checkpoints.clone();
            points.sort_by(|left, right| {
                left.limit
                    .partial_cmp(&right.limit)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| left.id.cmp(&right.id))
            });
            let current_ids = points
                .iter()
                .map(|point| point.id.clone())
                .collect::<HashSet<_>>();
            let known = self.known_checkpoints.entry(task.id.clone()).or_default();
            let mut changed = false;
            for id in &current_ids {
                if known.insert(id.clone()) {
                    changed = true;
                }
            }
            for id in known.iter() {
                if !current_ids.contains(id) {
                    changed = true;
                    all_confirmed = false;
                }
            }
            let stable = self.stable_polls.entry(task.id.clone()).or_default();
            if changed {
                *stable = 1;
                all_confirmed = false;
            } else {
                *stable = stable.saturating_add(1);
            }
            if *stable < MINIMUM_CHECKPOINT_STABLE_POLLS {
                all_confirmed = false;
            }
            if !checkpoints_cover_task_limit(task, &points) {
                all_confirmed = false;
            }
            for point in &points {
                if is_claimed(point.state) || self.claim_submitted.contains(&point.id) {
                    continue;
                }
                all_confirmed = false;
                if is_claimable(point.state) {
                    self.claim_if_due(cancel, api, point).await?;
                }
            }
        }
        if self.expected_tasks.iter().any(|id| !returned.contains(id)) {
            all_confirmed = false;
        }
        if all_confirmed {
            self.consecutive_confirmed = self.consecutive_confirmed.saturating_add(1);
        } else {
            self.consecutive_confirmed = 0;
        }
        Ok(self.consecutive_confirmed >= CONFIRMATION_POLLS)
    }

    async fn claim_if_due<A: BiliApi + ?Sized>(
        &mut self,
        cancel: &CancellationToken,
        api: &A,
        point: &TaskCheckpoint,
    ) -> anyhow::Result<()> {
        if self.claim_submitted.contains(&point.id) {
            return Ok(());
        }
        if let Some(attempted) = self.last_attempt.get(&point.id) {
            if attempted.elapsed() < CLAIM_RETRY_COOLDOWN {
                return Ok(());
            }
        }
        if let Some(last_claim) = self.last_claim_at {
            wait_cancel(cancel, CLAIM_SPACING.saturating_sub(last_claim.elapsed())).await?;
        }
        self.last_attempt.insert(point.id.clone(), Instant::now());
        api.claim_reward(cancel, &point.id).await.map_err(|error| {
            if error.to_string() == "操作已取消" {
                error
            } else {
                anyhow!("领取奖励 {} 失败：{error}", point.id)
            }
        })?;
        self.claim_submitted.insert(point.id.clone());
        self.last_claim_at = Some(Instant::now());
        Ok(())
    }
}

fn state_from_task(task: &TaskProgress) -> CheckpointState {
    if matches!(task.raw_status, 3 | 6) {
        CheckpointState::Claimed
    } else if task.raw_status == 2 {
        CheckpointState::Claimable
    } else {
        CheckpointState::Pending
    }
}

fn is_claimed(state: CheckpointState) -> bool {
    state == CheckpointState::Claimed
}

fn is_claimable(state: CheckpointState) -> bool {
    state == CheckpointState::Claimable
}

pub fn checkpoints_cover_task_limit(task: &TaskProgress, points: &[TaskCheckpoint]) -> bool {
    task.limit <= 0.0 || points.iter().any(|point| point.limit >= task.limit)
}

async fn wait_cancel(cancel: &CancellationToken, duration: Duration) -> anyhow::Result<()> {
    if duration.is_zero() {
        if cancel.is_cancelled() {
            bail!("操作已取消");
        }
        return Ok(());
    }
    tokio::select! {
        _ = tokio::time::sleep(duration) => Ok(()),
        _ = cancel.cancelled() => Err(anyhow!("操作已取消")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{LiveRoomInfo, TraceState};
    use async_trait::async_trait;
    use serde_json::{json, Value};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    struct ClaimApi {
        calls: AtomicUsize,
    }
    struct FailingClaimApi {
        calls: AtomicUsize,
    }
    #[async_trait]
    impl BiliApi for ClaimApi {
        async fn nav(&self, _: &CancellationToken) -> anyhow::Result<Value> {
            Ok(json!({}))
        }
        async fn task_progress(
            &self,
            _: &CancellationToken,
            _: &[String],
        ) -> anyhow::Result<Vec<TaskProgress>> {
            Ok(vec![])
        }
        async fn resolve_room(
            &self,
            _: &CancellationToken,
            _: u64,
        ) -> anyhow::Result<LiveRoomInfo> {
            unreachable!()
        }
        async fn enter_room(&self, _: &CancellationToken, _: &LiveRoomInfo) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn trace_enter(
            &self,
            _: &CancellationToken,
            _: &LiveRoomInfo,
            _: &str,
        ) -> anyhow::Result<TraceState> {
            unreachable!()
        }
        async fn trace_heartbeat(
            &self,
            _: &CancellationToken,
            _: &TraceState,
        ) -> anyhow::Result<TraceState> {
            unreachable!()
        }
        async fn claim_reward(&self, _: &CancellationToken, _: &str) -> anyhow::Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    #[async_trait]
    impl BiliApi for FailingClaimApi {
        async fn nav(&self, _: &CancellationToken) -> anyhow::Result<Value> {
            Ok(json!({}))
        }
        async fn task_progress(
            &self,
            _: &CancellationToken,
            _: &[String],
        ) -> anyhow::Result<Vec<TaskProgress>> {
            Ok(vec![])
        }
        async fn resolve_room(
            &self,
            _: &CancellationToken,
            _: u64,
        ) -> anyhow::Result<LiveRoomInfo> {
            unreachable!()
        }
        async fn enter_room(&self, _: &CancellationToken, _: &LiveRoomInfo) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn trace_enter(
            &self,
            _: &CancellationToken,
            _: &LiveRoomInfo,
            _: &str,
        ) -> anyhow::Result<TraceState> {
            unreachable!()
        }
        async fn trace_heartbeat(
            &self,
            _: &CancellationToken,
            _: &TraceState,
        ) -> anyhow::Result<TraceState> {
            unreachable!()
        }
        async fn claim_reward(&self, _: &CancellationToken, _: &str) -> anyhow::Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Err(anyhow!("temporary claim failure"))
        }
    }
    fn task(current: f64, limit: f64, points: Vec<TaskCheckpoint>) -> TaskProgress {
        TaskProgress {
            id: "parent".into(),
            task_key: "parent".into(),
            name: "watch".into(),
            current,
            limit,
            raw_status: 0,
            sampled_at: String::new(),
            checkpoints: points,
        }
    }
    fn point(id: &str, limit: f64, status: i32) -> TaskCheckpoint {
        TaskCheckpoint {
            id: id.into(),
            key: id.into(),
            name: id.into(),
            current: limit,
            limit,
            state: if status == 2 {
                CheckpointState::Claimable
            } else if status == 3 {
                CheckpointState::Claimed
            } else {
                CheckpointState::Pending
            },
            raw_status: status,
        }
    }
    #[tokio::test]
    async fn stable_full_set_requires_five_then_two_polls() {
        let api = Arc::new(ClaimApi {
            calls: AtomicUsize::new(0),
        });
        let cancel = CancellationToken::new();
        let mut tracker = RewardTracker::new(["parent".into()]);
        let full = task(10.0, 10.0, vec![point("a", 10.0, 3)]);
        for _ in 0..5 {
            assert!(!tracker
                .process(&cancel, api.as_ref(), std::slice::from_ref(&full))
                .await
                .unwrap());
        }
        assert!(tracker
            .process(&cancel, api.as_ref(), &[full])
            .await
            .unwrap());
    }
    #[tokio::test]
    async fn successful_claim_is_never_reposted() {
        let api = Arc::new(ClaimApi {
            calls: AtomicUsize::new(0),
        });
        let cancel = CancellationToken::new();
        let mut tracker = RewardTracker::new(["parent".into()]);
        let claimable = task(1.0, 10.0, vec![point("a", 10.0, 2)]);
        tracker
            .process(&cancel, api.as_ref(), std::slice::from_ref(&claimable))
            .await
            .unwrap();
        tracker
            .process(&cancel, api.as_ref(), &[claimable])
            .await
            .unwrap();
        assert_eq!(api.calls.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn failed_claim_respects_two_minute_cooldown() {
        let api = Arc::new(FailingClaimApi {
            calls: AtomicUsize::new(0),
        });
        let cancel = CancellationToken::new();
        let mut tracker = RewardTracker::new(["parent".into()]);
        let claimable = task(1.0, 10.0, vec![point("a", 10.0, 2)]);
        assert!(tracker
            .process(&cancel, api.as_ref(), std::slice::from_ref(&claimable))
            .await
            .is_err());
        assert!(!tracker
            .process(&cancel, api.as_ref(), &[claimable])
            .await
            .unwrap());
        assert_eq!(api.calls.load(Ordering::SeqCst), 1);
        assert!(!tracker.claim_was_submitted("a"));
    }
    #[test]
    fn parent_limit_requires_terminal_checkpoint() {
        let task = task(1.0, 240.0, vec![point("a", 180.0, 3)]);
        assert!(!checkpoints_cover_task_limit(&task, &task.checkpoints));
    }

    #[tokio::test]
    async fn missing_known_checkpoint_resets_completion_confirmation() {
        let api = Arc::new(ClaimApi {
            calls: AtomicUsize::new(0),
        });
        let cancel = CancellationToken::new();
        let mut tracker = RewardTracker::new(["parent".into()]);
        let full = task(10.0, 10.0, vec![point("a", 5.0, 3), point("b", 10.0, 3)]);
        for _ in 0..5 {
            let _ = tracker
                .process(&cancel, api.as_ref(), std::slice::from_ref(&full))
                .await
                .unwrap();
        }
        let subset = task(10.0, 10.0, vec![point("b", 10.0, 3)]);
        assert!(!tracker
            .process(&cancel, api.as_ref(), &[subset])
            .await
            .unwrap());
        assert_eq!(tracker.consecutive_confirmed(), 0);
    }
}
