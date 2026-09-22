use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{anyhow, bail, Context};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use tauri::{AppHandle, Emitter};
use tokio::{sync::oneshot, task::JoinHandle, time::sleep};
use tokio_util::sync::CancellationToken;

use crate::{
    bilibili::{display_name_from_nav, is_authentication_error, uid_from_nav, BiliClient},
    cache::{cache_path_for_current_executable, discovery_is_expired, unix_now, AccountCache},
    domain::{
        AccountChoice, AppPhase, AppSnapshot, CachedAccountRecord, Diagnostics, DiscoveryResult,
        Identity, RunConfiguration, SessionStats, TaskProgress, DEFAULT_ROOM_ID, DEFAULT_SESSIONS,
    },
    login::capture_login,
    rewards::RewardTracker,
    watch_manager::{WatchManager, WatchManagerOptions},
};

const SNAPSHOT_EVENT: &str = "app-snapshot";
const PROGRESS_INTERVAL: Duration = Duration::from_secs(3);
const DISCOVER_INTERVAL: Duration = Duration::from_secs(30);

async fn load_drop_progress(
    client: &BiliClient,
    cancel: &CancellationToken,
    room_id: u64,
    task_ids: &mut Vec<String>,
) -> anyhow::Result<(Vec<String>, Vec<TaskProgress>)> {
    if !task_ids.is_empty() {
        match crate::domain::BiliApi::task_progress(client, cancel, task_ids).await {
            Ok(progress) if !progress.is_empty() => {
                return Ok((task_ids.clone(), progress));
            }
            Ok(_) | Err(_) => {}
        }
    }
    *task_ids = client.discover_task_ids(cancel, room_id).await?;
    if task_ids.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    let progress = crate::domain::BiliApi::task_progress(client, cancel, task_ids).await?;
    Ok((task_ids.clone(), progress))
}

struct ActiveRun {
    cancel: CancellationToken,
    task: JoinHandle<anyhow::Result<()>>,
}

#[derive(Debug, thiserror::Error)]
#[error("用户取消登录操作")]
struct UserCanceled;

pub struct AppController {
    app: AppHandle,
    snapshot: Mutex<AppSnapshot>,
    configuration: Mutex<RunConfiguration>,
    cache: Mutex<AccountCache>,
    active: tokio::sync::Mutex<Option<ActiveRun>>,
    stop_gate: tokio::sync::Mutex<()>,
    window_close_started: AtomicBool,
}

impl AppController {
    pub fn new(app: AppHandle) -> anyhow::Result<Arc<Self>> {
        let cache = AccountCache::load(cache_path_for_current_executable()?)?;
        let accounts = cache.summaries();
        Ok(Arc::new(Self {
            app,
            snapshot: Mutex::new(AppSnapshot {
                revision: 1,
                view: "setup".into(),
                phase: AppPhase::Idle,
                phase_message: "等待开始".into(),
                started_at: None,
                accounts,
                identity: None,
                progress: vec![],
                sessions: SessionStats::default(),
                diagnostics: Diagnostics::default(),
            }),
            configuration: Mutex::new(RunConfiguration {
                room_id: DEFAULT_ROOM_ID.into(),
                sessions: DEFAULT_SESSIONS,
            }),
            cache: Mutex::new(cache),
            active: tokio::sync::Mutex::new(None),
            stop_gate: tokio::sync::Mutex::new(()),
            window_close_started: AtomicBool::new(false),
        }))
    }

    pub fn begin_window_close(&self) -> bool {
        !self.window_close_started.swap(true, Ordering::AcqRel)
    }

    pub fn bootstrap(&self) -> crate::BootstrapPayload {
        crate::BootstrapPayload {
            snapshot: self.snapshot.lock().clone(),
            configuration: self.configuration.lock().clone(),
            selected_account: None,
        }
    }

    pub async fn hydrate_account_names(self: Arc<Self>) {
        let records = self.cache.lock().records();
        for record in records {
            if record.display_name.is_some() {
                continue;
            }
            let Ok(client) = BiliClient::from_discovery(&record.discovery) else {
                continue;
            };
            let cancel = CancellationToken::new();
            let Ok(payload) = crate::domain::BiliApi::nav(&client, &cancel).await else {
                continue;
            };
            let Some(display_name) = display_name_from_nav(&payload) else {
                continue;
            };
            let changed = self
                .cache
                .lock()
                .set_display_name(&record.uid, &display_name)
                .unwrap_or(false);
            if changed {
                let accounts = self.cache.lock().summaries();
                self.update(|snapshot| snapshot.accounts = accounts);
            }
        }
    }

    pub async fn start(
        self: &Arc<Self>,
        configuration: RunConfiguration,
        choice: AccountChoice,
    ) -> anyhow::Result<()> {
        configuration.validate()?;
        anyhow::ensure!(
            matches!(choice, AccountChoice::Cached { .. }),
            "请先选择账号"
        );
        let mut active = self.active.lock().await;
        anyhow::ensure!(active.is_none(), "已有运行任务，请先停止");
        *self.configuration.lock() = configuration.clone();
        self.update(|snapshot| {
            snapshot.view = "run".into();
            snapshot.phase = AppPhase::ValidatingAccount;
            snapshot.phase_message = "检查中".into();
            snapshot.started_at = Some(Utc::now().to_rfc3339());
            snapshot.identity = None;
            snapshot.progress.clear();
            snapshot.sessions = SessionStats {
                target: configuration.sessions,
                ..Default::default()
            };
            snapshot.diagnostics = Diagnostics::default();
        });
        let cancel = CancellationToken::new();
        let (start_sender, start_receiver) = oneshot::channel();
        let controller = Arc::clone(self);
        let task_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            let _ = start_receiver.await;
            let result = controller
                .run_task(task_cancel.clone(), configuration, choice)
                .await;
            controller.active.lock().await.take();
            controller.finish_run(task_cancel.is_cancelled(), &result);
            result
        });
        *active = Some(ActiveRun { cancel, task });
        let _ = start_sender.send(());
        Ok(())
    }

    pub async fn refresh_account(
        self: &Arc<Self>,
        configuration: RunConfiguration,
        replace_uid: Option<String>,
    ) -> anyhow::Result<Option<String>> {
        configuration.validate()?;

        let mut active = self.active.lock().await;
        anyhow::ensure!(active.is_none(), "已有运行任务，请先停止");
        *self.configuration.lock() = configuration.clone();
        self.update(|snapshot| {
            snapshot.view = "setup".into();
            snapshot.phase = AppPhase::ValidatingAccount;
            snapshot.phase_message = "正在读取账号".into();
            snapshot.started_at = Some(Utc::now().to_rfc3339());
            snapshot.identity = None;
            snapshot.progress.clear();
            snapshot.sessions = SessionStats::default();
            snapshot.diagnostics = Diagnostics::default();
        });

        let cancel = CancellationToken::new();
        let (result_sender, result_receiver) = oneshot::channel();
        let controller = Arc::clone(self);
        let task_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            let result = controller
                .refresh_task(task_cancel.clone(), configuration, replace_uid)
                .await;
            let response = result
                .as_ref()
                .map(String::clone)
                .map_err(ToString::to_string);
            match &result {
                Ok(_) => {
                    controller.update(|snapshot| reset_to_setup(snapshot, "账号已添加"));
                }
                Err(_) => {
                    controller.update(|snapshot| reset_to_setup(snapshot, "账号读取失败"));
                }
            }
            controller.active.lock().await.take();
            let _ = result_sender.send(response);
            Ok(())
        });
        *active = Some(ActiveRun { cancel, task });
        drop(active);

        let response = result_receiver.await.context("等待账号读取结果失败")?;
        match response {
            Ok(uid) => Ok(Some(uid)),
            Err(error) if error == "用户取消登录操作" => Ok(None),
            Err(error) => Err(anyhow!(error)),
        }
    }

    pub async fn stop(&self) -> anyhow::Result<()> {
        let _stop = self.stop_gate.lock().await;
        let active = self.active.lock().await.take();
        let result = if let Some(active) = active {
            self.update(|snapshot| {
                snapshot.phase = AppPhase::Stopping;
                snapshot.phase_message = "正在清理观看会话".into();
            });
            active.cancel.cancel();
            active.task.await.context("等待运行任务结束失败")?
        } else {
            Ok(())
        };
        match result {
            Ok(()) => {
                self.update(|snapshot| reset_to_setup(snapshot, "已停止"));
                Ok(())
            }
            Err(error) if is_user_cancel(&error) || is_only_operation_canceled(&error) => {
                self.update(|snapshot| reset_to_setup(snapshot, "已停止"));
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    pub async fn delete_account(&self, uid: &str) -> anyhow::Result<()> {
        anyhow::ensure!(self.active.lock().await.is_none(), "运行期间不能删除账号");
        self.remove_cached_account(uid)?;
        Ok(())
    }

    fn remove_cached_account(&self, uid: &str) -> anyhow::Result<()> {
        let accounts = {
            let mut cache = self.cache.lock();
            cache.remove(uid)?;
            cache.summaries()
        };
        self.update(|snapshot| snapshot.accounts = accounts);
        Ok(())
    }

    async fn run_task(
        &self,
        cancel: CancellationToken,
        configuration: RunConfiguration,
        choice: AccountChoice,
    ) -> anyhow::Result<()> {
        let mut discovery = match choice.clone() {
            AccountChoice::Cached { uid } => {
                self.update(|snapshot| {
                    snapshot.phase = AppPhase::ValidatingAccount;
                    snapshot.phase_message = format!("正在验证账号 {uid}");
                });
                let record = self
                    .cache
                    .lock()
                    .get(&uid)
                    .ok_or_else(|| anyhow!("选择的账号缓存不存在"))?;
                if discovery_is_expired(&record.discovery, unix_now()) {
                    self.remove_cached_account(&uid)?;
                    bail!("账号登录信息已过期，已移除，请重新获取");
                }
                record.discovery
            }
            AccountChoice::Refresh { .. } => unreachable!("账号读取必须通过 refresh_account"),
        };
        self.update(|snapshot| {
            snapshot.phase = AppPhase::ValidatingAccount;
            snapshot.view = "run".into();
            snapshot.phase_message = "检查中".into();
        });
        let client = Arc::new(BiliClient::from_discovery(&discovery)?);
        let nav = match crate::domain::BiliApi::nav(client.as_ref(), &cancel).await {
            Ok(nav) => nav,
            Err(error)
                if matches!(&choice, AccountChoice::Cached { .. })
                    && is_authentication_error(&error) =>
            {
                if let AccountChoice::Cached { uid } = &choice {
                    self.remove_cached_account(uid)?;
                }
                bail!("账号登录信息已失效，已移除，请重新获取");
            }
            Err(error) => return Err(error),
        };
        let uid = uid_from_nav(&nav)?;
        let display_name = display_name_from_nav(&nav);
        if let AccountChoice::Cached { uid: expected } = &choice {
            anyhow::ensure!(
                &uid == expected,
                "缓存账号 {expected} 与当前登录账号 {uid} 不一致"
            );
        }
        let room_id = configuration.room_id.trim().parse::<u64>()?;
        let room = crate::domain::BiliApi::resolve_room(client.as_ref(), &cancel, room_id).await?;
        discovery.room_id = room_id;
        discovery.page_url = live_room_url(&configuration.room_id)?;
        discovery.task_ids.clear();
        let accounts = {
            let mut cache = self.cache.lock();
            cache.touch_with_display_name(
                &uid,
                Utc::now().to_rfc3339(),
                display_name.as_deref(),
            )?;
            cache.summaries()
        };
        self.update(|snapshot| {
            snapshot.accounts = accounts;
            snapshot.identity = Some(Identity {
                uid: uid.clone(),
                room_id,
            });
        });
        self.run_watch(cancel, configuration.sessions, client, uid, discovery, room)
            .await
    }

    async fn refresh_task(
        &self,
        cancel: CancellationToken,
        _configuration: RunConfiguration,
        replace_uid: Option<String>,
    ) -> anyhow::Result<String> {
        self.update(|snapshot| {
            snapshot.phase = AppPhase::ReadingIdentity;
            snapshot.phase_message = "正在打开 B 站登录页".into();
        });

        let discovery = match capture_login(&self.app, &cancel).await? {
            Some(discovery) => discovery,
            None => return Err(UserCanceled.into()),
        };
        self.update(|snapshot| {
            snapshot.phase = AppPhase::ValidatingAccount;
            snapshot.phase_message = "正在验证账号".into();
        });
        let client = Arc::new(BiliClient::from_discovery(&discovery)?);
        let nav = validate_discovery_with_retry(client.as_ref(), &cancel).await?;
        let uid = uid_from_nav(&nav)?;
        let display_name = display_name_from_nav(&nav);
        let mut discovery = discovery;
        discovery.page_url = "https://www.bilibili.com/".into();
        discovery.room_id = 0;
        discovery.task_ids.clear();
        let accounts = {
            let mut cache = self.cache.lock();
            cache.upsert(CachedAccountRecord {
                uid: uid.clone(),
                display_name,
                last_used_at: Utc::now().to_rfc3339(),
                discovery,
            })?;
            if let Some(old_uid) = replace_uid.filter(|old_uid| old_uid != &uid) {
                cache.remove(&old_uid)?;
            }
            cache.summaries()
        };
        self.update(|snapshot| snapshot.accounts = accounts);
        Ok(uid)
    }

    async fn run_watch(
        &self,
        cancel: CancellationToken,
        sessions: u16,
        client: Arc<BiliClient>,
        uid: String,
        discovery: DiscoveryResult,
        room: crate::domain::LiveRoomInfo,
    ) -> anyhow::Result<()> {
        let mut discovery = discovery;
        let mut tracker: Option<RewardTracker> = None;
        let mut active_task_ids = Vec::new();
        let mut monitor = ProgressMonitor::default();
        let mut manager: Option<Arc<WatchManager>> = None;
        self.update(|snapshot| {
            snapshot.view = "run".into();
            snapshot.phase = AppPhase::ReadingDrops;
            snapshot.phase_message = "读取中".into();
            snapshot.identity = Some(Identity {
                uid: uid.clone(),
                room_id: room.room_id,
            });
            snapshot.progress.clear();
            snapshot.diagnostics.errors.clear();
        });

        let options = WatchManagerOptions {
            max_sessions: sessions,
            ..WatchManagerOptions::default()
        };
        self.update(|snapshot| {
            snapshot.view = "run".into();
            snapshot.phase = AppPhase::ReadingDrops;
            snapshot.phase_message = "读取中".into();
        });
        let mut stats_tick = tokio::time::interval(Duration::from_millis(250));
        let mut progress_tick = tokio::time::interval(PROGRESS_INTERVAL);
        let mut discover_tick = tokio::time::interval(DISCOVER_INTERVAL);
        discover_tick.tick().await;
        let mut task_ids = client
            .discover_task_ids(&cancel, room.room_id)
            .await
            .unwrap_or_default();
        let result = loop {
            tokio::select! {
                _ = cancel.cancelled() => break Ok(()),
                _ = stats_tick.tick() => {
                    if let Some(active_manager) = manager.as_ref() {
                        let stats = active_manager.stats();
                        self.update(|snapshot| {
                            snapshot.sessions = stats.clone();
                            if stats.established > 0 {
                                snapshot.phase = AppPhase::Running;
                                snapshot.phase_message = "运行中".into();
                            }
                        });
                    }
                }
                _ = discover_tick.tick() => {
                    match client.discover_task_ids(&cancel, room.room_id).await {
                        Ok(ids) => {
                            task_ids = ids;
                            discovery.task_ids = task_ids.clone();
                            if task_ids.is_empty() {
                                if let Some(active_manager) = manager.take() {
                                    active_manager.stop().await;
                                }
                                tracker = None;
                                self.update(|snapshot| {
                                    snapshot.phase = AppPhase::IdleDrops;
                                    snapshot.phase_message = "空闲中".into();
                                    snapshot.progress.clear();
                                    snapshot.sessions = SessionStats {
                                        target: sessions,
                                        ..Default::default()
                                    };
                                    snapshot.diagnostics.rate = None;
                                    snapshot.diagnostics.updated_at = None;
                                    snapshot.diagnostics.errors.clear();
                                });
                            } else if task_ids != active_task_ids {
                                active_task_ids = task_ids.clone();
                                tracker = Some(RewardTracker::new(active_task_ids.clone()));
                            }
                        }
                        Err(_) => {}
                    }
                }
                _ = progress_tick.tick() => {
                    let current = if task_ids.is_empty() {
                        Ok((Vec::new(), Vec::new()))
                    } else {
                        load_drop_progress(client.as_ref(), &cancel, room.room_id, &mut task_ids).await
                    };

                    match current {
                        Ok((ids, progress)) if progress.is_empty() => {
                            discovery.task_ids = ids;
                            if let Some(active_manager) = manager.take() {
                                active_manager.stop().await;
                            }
                            tracker = None;
                            self.update(|snapshot| {
                                snapshot.phase = AppPhase::IdleDrops;
                                snapshot.phase_message = "空闲中".into();
                                snapshot.progress.clear();
                                snapshot.sessions = SessionStats {
                                    target: sessions,
                                    ..Default::default()
                                };
                                snapshot.diagnostics.rate = None;
                                snapshot.diagnostics.updated_at = None;
                                snapshot.diagnostics.errors.clear();
                            });
                        }
                        Ok((ids, progress)) => {
                            discovery.task_ids = ids;
                            if discovery.task_ids != active_task_ids {
                                active_task_ids = discovery.task_ids.clone();
                                tracker = Some(RewardTracker::new(active_task_ids.clone()));
                            }
                            let done = match tracker.as_mut().expect("tracker initialized")
                                .process(&cancel, client.as_ref(), &progress).await {
                                Ok(done) => done,
                                Err(error) => break Err(error),
                            };
                            self.publish_progress(
                                &uid,
                                room.room_id,
                                &mut monitor,
                                progress,
                            );
                            if done {
                                self.update(|snapshot| {
                                    snapshot.phase = AppPhase::Completed;
                                    snapshot.phase_message = "本次掉宝已完成".into();
                                });
                                break Ok(());
                            }
                            if manager.is_none() {
                                let api: Arc<dyn crate::domain::BiliApi> = client.clone();
                                let active_manager = WatchManager::new(
                                    &cancel,
                                    api,
                                    room.clone(),
                                    options.clone(),
                                );
                                active_manager.enter_room().await?;
                                active_manager.scale_to(sessions)?;
                                manager = Some(active_manager);
                                self.update(|snapshot| {
                                    snapshot.phase = AppPhase::ConnectingSessions;
                                    snapshot.phase_message = "运行中".into();
                                });
                            }
                        }
                        Err(_) => {
                            if manager.is_none() {
                                self.update(|snapshot| {
                                    snapshot.phase = AppPhase::ReadingDrops;
                                    snapshot.phase_message = "读取中".into();
                                    snapshot.progress.clear();
                                    snapshot.sessions = SessionStats {
                                        target: sessions,
                                        ..Default::default()
                                    };
                                    snapshot.diagnostics.errors.clear();
                                });
                            }
                        }
                    }
                }
            }
        };
        if let Some(active_manager) = manager {
            active_manager.stop().await;
        }
        result
    }

    fn publish_progress(
        &self,
        uid: &str,
        room_id: u64,
        monitor: &mut ProgressMonitor,
        progress: Vec<TaskProgress>,
    ) {
        let rate = monitor.update(&progress);
        let sampled_at = progress.first().map(|item| item.sampled_at.clone());
        self.update(|snapshot| {
            snapshot.view = "run".into();
            snapshot.identity = Some(Identity {
                uid: uid.into(),
                room_id,
            });
            snapshot.progress = progress;
            if !snapshot.progress.is_empty() {
                snapshot.diagnostics.rate = rate;
                snapshot.diagnostics.updated_at = sampled_at;
                snapshot.diagnostics.errors.clear();
            }
        });
    }

    fn finish_run(&self, was_cancelled: bool, result: &anyhow::Result<()>) {
        match result {
            Ok(()) if was_cancelled => self.update(|snapshot| reset_to_setup(snapshot, "已停止")),
            Ok(()) => {}
            Err(error) if is_user_cancel(error) => {
                self.update(|snapshot| reset_to_setup(snapshot, "已取消登录"));
            }
            Err(error) if was_cancelled && is_only_operation_canceled(error) => {
                self.update(|snapshot| reset_to_setup(snapshot, "已停止"));
            }
            Err(error) => self.update(|snapshot| {
                snapshot.view = "run".into();
                snapshot.phase = AppPhase::Failed;
                snapshot.phase_message = "运行失败".into();
                snapshot.diagnostics.errors = vec![error.to_string()];
            }),
        }
    }

    fn update(&self, update: impl FnOnce(&mut AppSnapshot)) {
        let snapshot = {
            let mut snapshot = self.snapshot.lock();
            update(&mut snapshot);
            snapshot.revision = snapshot.revision.wrapping_add(1);
            snapshot.clone()
        };
        let _ = self.app.emit(SNAPSHOT_EVENT, snapshot);
    }
}

async fn validate_discovery_with_retry(
    client: &BiliClient,
    cancel: &CancellationToken,
) -> anyhow::Result<serde_json::Value> {
    const RETRIES: usize = 4;
    let mut last_error = None;

    for attempt in 0..=RETRIES {
        if cancel.is_cancelled() {
            bail!("操作已取消");
        }
        match crate::domain::BiliApi::nav(client, cancel).await {
            Ok(nav) => return Ok(nav),
            Err(error) => {
                last_error = Some(error);
                if attempt == RETRIES {
                    break;
                }
                let delay = Duration::from_millis(500 * (attempt as u64 + 1));
                tokio::select! {
                    _ = cancel.cancelled() => bail!("操作已取消"),
                    _ = sleep(delay) => {}
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("账号验证失败")))
}

#[derive(Default)]
struct ProgressMonitor {
    baselines: HashMap<String, ProgressBaseline>,
}

#[derive(Clone, Copy)]
struct ProgressBaseline {
    current: f64,
    sampled_at: Option<DateTime<Utc>>,
}

impl ProgressMonitor {
    fn update(&mut self, progress: &[TaskProgress]) -> Option<f64> {
        let mut total_rate = 0.0;
        let mut has_rate = false;
        for item in progress.iter().filter(|item| item.limit > 0.0) {
            let sampled_at = DateTime::parse_from_rfc3339(&item.sampled_at)
                .ok()
                .map(|value| value.with_timezone(&Utc));
            let baseline = self
                .baselines
                .entry(item.id.clone())
                .or_insert(ProgressBaseline {
                    current: item.current,
                    sampled_at,
                });
            if let (Some(start), Some(now)) = (baseline.sampled_at, sampled_at) {
                if now > start && item.current > baseline.current {
                    total_rate += (item.current - baseline.current)
                        / (now - start).num_milliseconds() as f64
                        * 60_000.0;
                    has_rate = true;
                } else if now <= start || item.current < baseline.current {
                    baseline.current = item.current;
                    baseline.sampled_at = sampled_at;
                }
            } else {
                baseline.current = item.current;
                baseline.sampled_at = sampled_at;
            }
        }
        has_rate.then_some(total_rate)
    }
}

fn reset_to_setup(snapshot: &mut AppSnapshot, message: &str) {
    snapshot.view = "setup".into();
    snapshot.phase = AppPhase::Idle;
    snapshot.phase_message = message.into();
    snapshot.started_at = None;
    snapshot.identity = None;
    snapshot.progress.clear();
    snapshot.sessions = SessionStats::default();
    snapshot.diagnostics.errors.clear();
    snapshot.diagnostics.rate = None;
    snapshot.diagnostics.updated_at = None;
}

fn is_user_cancel(error: &anyhow::Error) -> bool {
    error.downcast_ref::<UserCanceled>().is_some()
}

fn is_only_operation_canceled(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| cause.to_string() == "操作已取消")
        || error.to_string().ends_with("：操作已取消")
}

fn live_room_url(room_id: &str) -> anyhow::Result<String> {
    let room_id = room_id.trim().parse::<u64>()?;
    anyhow::ensure!(room_id > 0, "直播间号必须大于 0");
    Ok(format!("https://live.bilibili.com/{room_id}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;

    fn task(current: f64, sampled_at: DateTime<Utc>) -> TaskProgress {
        TaskProgress {
            id: "t".into(),
            task_key: "drop-1".into(),
            name: "watch".into(),
            current,
            limit: 240.0,
            raw_status: 0,
            sampled_at: sampled_at.to_rfc3339(),
            checkpoints: vec![],
        }
    }

    #[test]
    fn rate_stays_none_until_progress_increases() {
        let mut monitor = ProgressMonitor::default();
        let t0 = Utc::now();
        assert_eq!(monitor.update(&[task(10.0, t0)]), None);
        assert_eq!(
            monitor.update(&[task(10.0, t0 + ChronoDuration::seconds(10))]),
            None
        );
        let rate = monitor
            .update(&[task(11.0, t0 + ChronoDuration::seconds(60))])
            .unwrap();
        assert!((rate - 1.0).abs() < 1e-6);
    }
}
