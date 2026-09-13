use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

pub const DEFAULT_ROOM_ID: &str = "23612045";
pub const MIN_SESSIONS: u16 = 10;
pub const DEFAULT_SESSIONS: u16 = 500;
pub const MAX_SESSIONS: u16 = 10000;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RunConfiguration {
    pub room_id: String,
    pub sessions: u16,
}

impl RunConfiguration {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (MIN_SESSIONS..=MAX_SESSIONS).contains(&self.sessions),
            "并发数必须在 {MIN_SESSIONS}..={MAX_SESSIONS} 之间"
        );
        anyhow::ensure!(
            !self.room_id.trim().is_empty()
                && self
                    .room_id
                    .trim()
                    .chars()
                    .all(|character| character.is_ascii_digit()),
            "直播间号必须是纯数字"
        );
        anyhow::ensure!(
            self.room_id
                .trim()
                .parse::<u64>()
                .is_ok_and(|room_id| room_id > 0),
            "直播间号必须大于 0"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum AccountChoice {
    #[serde(rename = "cached")]
    Cached { uid: String },
    #[serde(rename = "refresh")]
    Refresh { replace_uid: Option<String> },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CachedAccountSummary {
    pub uid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub room_id: u64,
    pub last_used_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Identity {
    pub uid: String,
    pub room_id: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointState {
    Pending,
    Claimable,
    Claimed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskCheckpoint {
    #[serde(skip_serializing)]
    pub id: String,
    pub key: String,
    pub name: String,
    pub current: f64,
    pub limit: f64,
    pub state: CheckpointState,
    #[serde(skip_serializing)]
    pub raw_status: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskProgress {
    #[serde(skip_serializing)]
    pub id: String,
    pub task_key: String,
    pub name: String,
    pub current: f64,
    pub limit: f64,
    #[serde(skip_serializing)]
    pub raw_status: i32,
    pub sampled_at: String,
    pub checkpoints: Vec<TaskCheckpoint>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SessionStats {
    pub target: u16,
    pub registered: u16,
    pub established: u16,
    pub healthy: u16,
    pub heartbeats: u64,
    pub reconnects: u64,
    pub rate_limits: u64,
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AppPhase {
    Idle,
    ReadingIdentity,
    ValidatingAccount,
    ResolvingRoom,
    ReadingDrops,
    IdleDrops,
    ConnectingSessions,
    Running,
    Stopping,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AppSnapshot {
    pub revision: u64,
    pub view: String,
    pub phase: AppPhase,
    pub phase_message: String,
    pub started_at: Option<String>,
    pub accounts: Vec<CachedAccountSummary>,
    pub identity: Option<Identity>,
    pub progress: Vec<TaskProgress>,
    pub sessions: SessionStats,
    pub diagnostics: Diagnostics,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Diagnostics {
    pub rate: Option<f64>,
    pub updated_at: Option<String>,
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Cookie {
    pub name: String,
    pub value: String,
    pub domain: String,
    pub path: String,
    pub secure: bool,
    pub http_only: bool,
    pub same_site: String,
    pub expiration_date: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryResult {
    pub cookies: Vec<Cookie>,
    pub user_agent: String,
    pub page_url: String,
    pub room_id: u64,
    pub task_ids: Vec<String>,
    pub observed_paths: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CachedAccountRecord {
    pub uid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub last_used_at: String,
    pub discovery: DiscoveryResult,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CacheFile {
    pub version: u32,
    pub accounts: Vec<CachedAccountRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveRoomInfo {
    pub room_id: u64,
    pub ruid: u64,
    pub parent_area_id: u64,
    pub area_id: u64,
    pub live_status: i32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TraceState {
    pub room: LiveRoomInfo,
    pub page_uuid: String,
    pub seq: i64,
    pub timestamp: i64,
    pub heartbeat_interval: Duration,
    pub secret_key: String,
    pub secret_rule: Vec<i32>,
}

#[async_trait]
pub trait BiliApi: Send + Sync {
    async fn nav(&self, cancel: &CancellationToken) -> anyhow::Result<Value>;
    async fn task_progress(
        &self,
        cancel: &CancellationToken,
        task_ids: &[String],
    ) -> anyhow::Result<Vec<TaskProgress>>;
    async fn resolve_room(
        &self,
        cancel: &CancellationToken,
        requested_room_id: u64,
    ) -> anyhow::Result<LiveRoomInfo>;
    async fn enter_room(
        &self,
        cancel: &CancellationToken,
        room: &LiveRoomInfo,
    ) -> anyhow::Result<()>;
    async fn trace_enter(
        &self,
        cancel: &CancellationToken,
        room: &LiveRoomInfo,
        page_uuid: &str,
    ) -> anyhow::Result<TraceState>;
    async fn trace_heartbeat(
        &self,
        cancel: &CancellationToken,
        state: &TraceState,
    ) -> anyhow::Result<TraceState>;
    async fn claim_reward(
        &self,
        cancel: &CancellationToken,
        checkpoint_id: &str,
    ) -> anyhow::Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configuration_rejects_out_of_range_sessions() {
        for sessions in [0, 9, 10001] {
            assert!(RunConfiguration {
                room_id: DEFAULT_ROOM_ID.into(),
                sessions,
            }
            .validate()
            .is_err());
        }
    }

    #[test]
    fn configuration_accepts_hard_limit() {
        assert!(RunConfiguration {
            room_id: DEFAULT_ROOM_ID.into(),
            sessions: MAX_SESSIONS,
        }
        .validate()
        .is_ok());
    }
}
