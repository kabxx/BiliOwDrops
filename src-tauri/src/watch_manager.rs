use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{anyhow, bail};
use parking_lot::Mutex;
use tokio::{sync::Semaphore, task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::domain::{BiliApi, LiveRoomInfo, SessionStats, TraceState, MAX_SESSIONS};

const MAX_IN_FLIGHT_CONNECTS: usize = 2;

#[derive(Clone, Debug)]
pub struct WatchManagerOptions {
    pub max_sessions: u16,
    pub launch_delay_min: Duration,
    pub launch_delay_max: Duration,
    pub reconnect_delays: Vec<Duration>,
    pub minimum_heartbeat_interval: Duration,
    pub rate_limit_pause: Duration,
    pub heartbeat_pace: Duration,
}

impl Default for WatchManagerOptions {
    fn default() -> Self {
        Self {
            max_sessions: 50,
            launch_delay_min: Duration::ZERO,
            launch_delay_max: Duration::ZERO,
            reconnect_delays: vec![
                Duration::from_secs(2),
                Duration::from_secs(4),
                Duration::from_secs(8),
                Duration::from_secs(16),
                Duration::from_secs(30),
            ],
            minimum_heartbeat_interval: Duration::from_secs(5),
            rate_limit_pause: Duration::from_secs(30),
            heartbeat_pace: Duration::from_millis(30),
        }
    }
}

impl WatchManagerOptions {
    fn normalize(mut self) -> Self {
        if self.max_sessions == 0 || self.max_sessions > MAX_SESSIONS {
            self.max_sessions = MAX_SESSIONS;
        }
        if self.launch_delay_max < self.launch_delay_min {
            self.launch_delay_max = self.launch_delay_min;
        }
        if self.reconnect_delays.is_empty() {
            self.reconnect_delays = WatchManagerOptions::default().reconnect_delays;
        }
        if self.minimum_heartbeat_interval.is_zero() {
            self.minimum_heartbeat_interval = Duration::from_secs(5);
        }
        if self.rate_limit_pause.is_zero() {
            self.rate_limit_pause = Duration::from_secs(30);
        }
        if self.heartbeat_pace.is_zero() {
            self.heartbeat_pace = Duration::from_millis(30);
        }
        self
    }
}

#[derive(Clone, Debug)]
struct SessionRuntime {
    page_uuid: String,
    established: bool,
    healthy: bool,
    heartbeats: u64,
    reconnects: u64,
    last_error: String,
}

#[derive(Debug, Default)]
struct ManagerState {
    target: u16,
    sessions: BTreeMap<u16, SessionRuntime>,
    rate_limits: u64,
}

#[derive(Debug)]
struct RequestPacer {
    next: tokio::sync::Mutex<Instant>,
    interval: Duration,
}

impl RequestPacer {
    fn new(interval: Duration) -> Self {
        Self {
            next: tokio::sync::Mutex::new(Instant::now()),
            interval,
        }
    }

    async fn wait(&self, cancel: &CancellationToken) -> anyhow::Result<()> {
        let ready_at = {
            let mut next = self.next.lock().await;
            let now = Instant::now();
            let ready_at = (*next).max(now);
            *next = ready_at + self.interval;
            ready_at
        };
        wait_until(cancel, ready_at).await
    }
}

#[derive(Clone, Copy, Debug)]
struct GateState {
    until: Instant,
    generation: u64,
}

#[derive(Debug)]
struct RateLimitGate(Mutex<GateState>);

impl RateLimitGate {
    fn new() -> Self {
        Self(Mutex::new(GateState {
            until: Instant::now(),
            generation: 0,
        }))
    }

    fn pause(&self, duration: Duration) {
        let mut state = self.0.lock();
        let until = Instant::now() + duration;
        if until > state.until {
            state.until = until;
            state.generation = state.generation.wrapping_add(1);
        }
    }

    async fn wait_generation(&self, cancel: &CancellationToken) -> anyhow::Result<u64> {
        loop {
            let state = *self.0.lock();
            if state.until <= Instant::now() {
                return Ok(state.generation);
            }
            wait_until(cancel, state.until).await?;
        }
    }

    #[cfg(test)]
    fn is_current(&self, generation: u64) -> bool {
        let state = self.0.lock();
        state.generation == generation && state.until <= Instant::now()
    }
}

pub struct WatchManager {
    api: Arc<dyn BiliApi>,
    room: LiveRoomInfo,
    options: WatchManagerOptions,
    cancel: CancellationToken,
    connect_semaphore: Arc<Semaphore>,
    gate: RateLimitGate,
    heartbeat_pacer: RequestPacer,
    room_entered: AtomicBool,
    enter_lock: tokio::sync::Mutex<()>,
    state: Mutex<ManagerState>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl WatchManager {
    pub fn new(
        parent: &CancellationToken,
        api: Arc<dyn BiliApi>,
        room: LiveRoomInfo,
        options: WatchManagerOptions,
    ) -> Arc<Self> {
        let options = options.normalize();
        Arc::new(Self {
            api,
            room,
            cancel: parent.child_token(),
            connect_semaphore: Arc::new(Semaphore::new(MAX_IN_FLIGHT_CONNECTS)),
            gate: RateLimitGate::new(),
            heartbeat_pacer: RequestPacer::new(options.heartbeat_pace),
            room_entered: AtomicBool::new(false),
            enter_lock: tokio::sync::Mutex::new(()),
            state: Mutex::new(ManagerState::default()),
            tasks: Mutex::new(Vec::new()),
            options,
        })
    }

    pub fn scale_to(self: &Arc<Self>, target: u16) -> anyhow::Result<()> {
        if target == 0 || target > self.options.max_sessions {
            bail!(
                "观看会话数 {target} 超出范围 1..={}",
                self.options.max_sessions
            );
        }
        loop {
            let id = {
                let mut state = self.state.lock();
                if usize::from(target) < state.sessions.len() {
                    bail!(
                        "当前已登记 {} 个会话，不支持在运行中缩减到 {target}",
                        state.sessions.len()
                    );
                }
                state.target = target;
                if state.sessions.len() >= usize::from(target) {
                    return Ok(());
                }
                let id = u16::try_from(state.sessions.len() + 1)?;
                state.sessions.insert(
                    id,
                    SessionRuntime {
                        page_uuid: new_page_uuid(),
                        established: false,
                        healthy: false,
                        heartbeats: 0,
                        reconnects: 0,
                        last_error: String::new(),
                    },
                );
                id
            };
            let manager = Arc::clone(self);
            let task = tokio::spawn(async move { manager.run_session(id).await });
            self.tasks.lock().push(task);
        }
    }

    pub fn stats(&self) -> SessionStats {
        let state = self.state.lock();
        let mut stats = SessionStats {
            target: state.target,
            registered: state.sessions.len().try_into().unwrap_or(u16::MAX),
            rate_limits: state.rate_limits,
            ..SessionStats::default()
        };
        let mut errors = HashMap::<String, usize>::new();
        for session in state.sessions.values() {
            stats.established += u16::from(session.established);
            stats.healthy += u16::from(session.healthy);
            stats.heartbeats += session.heartbeats;
            stats.reconnects += session.reconnects;
            if !session.last_error.is_empty() {
                *errors.entry(session.last_error.clone()).or_default() += 1;
            }
        }
        let mut errors = errors.into_iter().collect::<Vec<_>>();
        errors.sort_by(|(left_message, left_count), (right_message, right_count)| {
            right_count
                .cmp(left_count)
                .then_with(|| left_message.cmp(right_message))
        });
        stats.errors = errors
            .into_iter()
            .take(3)
            .map(|(message, count)| format!("{count}x {message}"))
            .collect();
        stats
    }

    pub async fn stop(&self) {
        self.cancel.cancel();
        let tasks = std::mem::take(&mut *self.tasks.lock());
        for task in tasks {
            let _ = task.await;
        }
    }

    async fn run_session(self: Arc<Self>, id: u16) {
        if wait_duration(&self.cancel, self.launch_delay())
            .await
            .is_err()
        {
            return;
        }
        let mut attempt = 0usize;
        while !self.cancel.is_cancelled() {
            // Connecting/reconnecting sessions share the cool-down.
            // Established heartbeats do not wait on this gate.
            if self.gate.wait_generation(&self.cancel).await.is_err() {
                return;
            }
            let page_uuid = {
                let state = self.state.lock();
                match state.sessions.get(&id) {
                    Some(session) => session.page_uuid.clone(),
                    None => return,
                }
            };
            let mut trace = match self.establish(&page_uuid).await {
                Ok(trace) => trace,
                Err(error) => {
                    if self.cancel.is_cancelled() {
                        return;
                    }
                    self.record_failure(id, &error, true);
                    if wait_duration(&self.cancel, self.reconnect_delay(attempt))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    attempt = attempt.saturating_add(1);
                    continue;
                }
            };
            self.set_established(id);
            while !self.cancel.is_cancelled() {
                let interval = trace
                    .heartbeat_interval
                    .max(self.options.minimum_heartbeat_interval);
                if wait_duration(&self.cancel, interval).await.is_err() {
                    return;
                }
                if self.heartbeat_pacer.wait(&self.cancel).await.is_err() {
                    return;
                }
                match self.api.trace_heartbeat(&self.cancel, &trace).await {
                    Ok(next) => {
                        trace = next;
                        attempt = 0;
                        self.record_heartbeat(id);
                    }
                    Err(error) => {
                        if self.cancel.is_cancelled() {
                            return;
                        }
                        self.record_failure(id, &error, true);
                        if is_rate_limit_error(&error) {
                            self.pause_for_rate_limit();
                        }
                        break;
                    }
                }
            }
            if wait_duration(&self.cancel, self.reconnect_delay(attempt))
                .await
                .is_err()
            {
                return;
            }
            attempt = attempt.saturating_add(1);
        }
    }

    async fn establish(&self, page_uuid: &str) -> anyhow::Result<TraceState> {
        self.ensure_room_entered().await?;
        let permit = tokio::select! {
            permit = Arc::clone(&self.connect_semaphore).acquire_owned() => {
                permit.map_err(|_| anyhow!("建连调度器已关闭"))?
            }
            _ = self.cancel.cancelled() => return Err(anyhow!("操作已取消")),
        };
        let result = self
            .api
            .trace_enter(&self.cancel, &self.room, page_uuid)
            .await;
        drop(permit);
        if let Err(error) = &result {
            if is_rate_limit_error(error) {
                self.pause_for_rate_limit();
            }
        }
        result
    }

    async fn ensure_room_entered(&self) -> anyhow::Result<()> {
        if self.room_entered.load(Ordering::Acquire) {
            return Ok(());
        }
        let _guard = tokio::select! {
            guard = self.enter_lock.lock() => guard,
            _ = self.cancel.cancelled() => return Err(anyhow!("操作已取消")),
        };
        if self.room_entered.load(Ordering::Acquire) {
            return Ok(());
        }
        self.gate.wait_generation(&self.cancel).await?;
        match self.api.enter_room(&self.cancel, &self.room).await {
            Ok(()) => {
                self.room_entered.store(true, Ordering::Release);
                Ok(())
            }
            Err(error) => {
                if is_rate_limit_error(&error) {
                    self.pause_for_rate_limit();
                }
                Err(error)
            }
        }
    }

    fn pause_for_rate_limit(&self) {
        self.state.lock().rate_limits += 1;
        self.gate.pause(self.options.rate_limit_pause);
    }

    fn set_established(&self, id: u16) {
        if let Some(session) = self.state.lock().sessions.get_mut(&id) {
            session.established = true;
            session.healthy = false;
            session.last_error.clear();
        }
    }

    fn record_heartbeat(&self, id: u16) {
        if let Some(session) = self.state.lock().sessions.get_mut(&id) {
            session.established = true;
            session.healthy = true;
            session.heartbeats += 1;
            session.last_error.clear();
        }
    }

    fn record_failure(&self, id: u16, error: &anyhow::Error, reconnect: bool) {
        if let Some(session) = self.state.lock().sessions.get_mut(&id) {
            session.established = false;
            session.healthy = false;
            session.last_error = error.to_string();
            if reconnect {
                session.reconnects += 1;
            }
        }
    }

    fn launch_delay(&self) -> Duration {
        random_duration(self.options.launch_delay_min, self.options.launch_delay_max)
    }

    fn reconnect_delay(&self, attempt: usize) -> Duration {
        let index = attempt.min(self.options.reconnect_delays.len() - 1);
        let base = self.options.reconnect_delays[index];
        let jitter_millis = (base.as_millis() as u64).saturating_mul(20) / 100;
        let low = base.saturating_sub(Duration::from_millis(jitter_millis));
        let high = base.saturating_add(Duration::from_millis(jitter_millis));
        random_duration(low, high)
    }
}

fn random_duration(low: Duration, high: Duration) -> Duration {
    if high <= low {
        return low;
    }
    let range = (high - low).as_nanos().min(u64::MAX as u128) as u64;
    low + Duration::from_nanos(rand::random::<u64>() % (range.saturating_add(1)))
}

async fn wait_until(cancel: &CancellationToken, deadline: Instant) -> anyhow::Result<()> {
    tokio::select! {
        _ = tokio::time::sleep_until(deadline) => Ok(()),
        _ = cancel.cancelled() => Err(anyhow!("操作已取消")),
    }
}

async fn wait_duration(cancel: &CancellationToken, duration: Duration) -> anyhow::Result<()> {
    wait_until(cancel, Instant::now() + duration).await
}

fn new_page_uuid() -> String {
    format!(
        "{}-{}",
        chrono::Utc::now().timestamp(),
        uuid::Uuid::new_v4()
    )
}

pub fn is_rate_limit_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_lowercase();
    message.contains("-702")
        || message.contains("-509")
        || message.contains("http 429")
        || message.contains("频率")
        || message.contains("频繁")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use serde_json::{json, Value};

    use crate::domain::{TaskProgress, TraceState};

    use super::*;

    struct MockApi {
        enters: AtomicUsize,
        traces: AtomicUsize,
        fail_first: AtomicUsize,
        rate_limit_after_traces: usize,
        heartbeat_interval: Duration,
    }

    impl MockApi {
        fn new(fail_first: usize) -> Self {
            Self {
                enters: AtomicUsize::new(0),
                traces: AtomicUsize::new(0),
                fail_first: AtomicUsize::new(fail_first),
                rate_limit_after_traces: usize::MAX,
                heartbeat_interval: Duration::from_secs(60),
            }
        }

        fn rate_limited_after(mut self, traces: usize, heartbeat_interval: Duration) -> Self {
            self.rate_limit_after_traces = traces;
            self.heartbeat_interval = heartbeat_interval;
            self
        }
    }

    #[async_trait]
    impl BiliApi for MockApi {
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
        async fn enter_room(
            &self,
            cancel: &CancellationToken,
            _: &LiveRoomInfo,
        ) -> anyhow::Result<()> {
            self.enters.fetch_add(1, Ordering::SeqCst);
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(1)) => Ok(()),
                _ = cancel.cancelled() => Err(anyhow!("cancelled")),
            }
        }
        async fn trace_enter(
            &self,
            _: &CancellationToken,
            room: &LiveRoomInfo,
            page_uuid: &str,
        ) -> anyhow::Result<TraceState> {
            let n = self.traces.fetch_add(1, Ordering::SeqCst);
            if n >= self.rate_limit_after_traces {
                return Err(anyhow!("-702 频繁"));
            }
            if self
                .fail_first
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                    value.checked_sub(1)
                })
                .is_ok()
            {
                return Err(anyhow!("temporary"));
            }
            Ok(TraceState {
                room: room.clone(),
                page_uuid: page_uuid.into(),
                seq: 0,
                timestamp: 0,
                heartbeat_interval: self.heartbeat_interval,
                secret_key: "secret".into(),
                secret_rule: vec![0],
            })
        }
        async fn trace_heartbeat(
            &self,
            _: &CancellationToken,
            state: &TraceState,
        ) -> anyhow::Result<TraceState> {
            Ok(state.clone())
        }
        async fn claim_reward(&self, _: &CancellationToken, _: &str) -> anyhow::Result<()> {
            unreachable!()
        }
    }

    fn room() -> LiveRoomInfo {
        LiveRoomInfo {
            room_id: 1,
            ruid: 2,
            parent_area_id: 3,
            area_id: 4,
            live_status: 1,
        }
    }

    fn fast_options(max_sessions: u16) -> WatchManagerOptions {
        WatchManagerOptions {
            max_sessions,
            launch_delay_min: Duration::ZERO,
            launch_delay_max: Duration::ZERO,
            reconnect_delays: vec![Duration::from_millis(1)],
            minimum_heartbeat_interval: Duration::from_secs(30),
            rate_limit_pause: Duration::from_millis(8),
            heartbeat_pace: Duration::from_millis(1),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn scale_registers_target_and_establishes_sessions() {
        let cancel = CancellationToken::new();
        let api = Arc::new(MockApi::new(0));
        let manager = WatchManager::new(&cancel, api.clone(), room(), fast_options(50));
        manager.scale_to(50).unwrap();
        assert_eq!(manager.stats().registered, 50);
        tokio::time::timeout(Duration::from_secs(1), async {
            while manager.stats().established < 50 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        manager.stop().await;
        assert_eq!(api.enters.load(Ordering::SeqCst), 1);
        assert_eq!(api.traces.load(Ordering::SeqCst), 50);
    }

    #[test]
    fn production_defaults_keep_required_request_limits() {
        let options = WatchManagerOptions::default();
        assert_eq!(options.launch_delay_min, Duration::ZERO);
        assert_eq!(options.launch_delay_max, Duration::ZERO);
        assert_eq!(options.heartbeat_pace, Duration::from_millis(30));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failures_rebuild_and_stop_reaps_all_tasks() {
        let cancel = CancellationToken::new();
        let api = Arc::new(MockApi::new(2));
        let manager = WatchManager::new(&cancel, api.clone(), room(), fast_options(4));
        manager.scale_to(4).unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while manager.stats().established < 4 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        manager.stop().await;
        assert!(manager.tasks.lock().is_empty());
        assert!(manager.stats().reconnects >= 2);
        assert_eq!(api.enters.load(Ordering::SeqCst), 1);
        assert!(api.traces.load(Ordering::SeqCst) >= 6);
    }

    #[tokio::test]
    async fn gate_generation_forces_queued_request_to_wait_again() {
        let gate = Arc::new(RateLimitGate::new());
        let cancel = CancellationToken::new();
        let initial = gate.wait_generation(&cancel).await.unwrap();
        gate.pause(Duration::from_millis(12));
        assert!(!gate.is_current(initial));
        let next = gate.wait_generation(&cancel).await.unwrap();
        assert!(next > initial);
        assert!(gate.is_current(next));
    }

    #[tokio::test]
    async fn request_pacer_preserves_spacing() {
        let pacer = RequestPacer::new(Duration::from_millis(5));
        let cancel = CancellationToken::new();
        let started = Instant::now();
        pacer.wait(&cancel).await.unwrap();
        pacer.wait(&cancel).await.unwrap();
        pacer.wait(&cancel).await.unwrap();
        assert!(started.elapsed() >= Duration::from_millis(9));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn established_heartbeats_continue_during_connect_rate_limit() {
        let cancel = CancellationToken::new();
        let api = Arc::new(
            MockApi::new(0).rate_limited_after(1, Duration::from_millis(15)),
        );
        let options = WatchManagerOptions {
            max_sessions: 3,
            launch_delay_min: Duration::ZERO,
            launch_delay_max: Duration::ZERO,
            reconnect_delays: vec![Duration::from_millis(1)],
            minimum_heartbeat_interval: Duration::from_millis(15),
            rate_limit_pause: Duration::from_millis(400),
            heartbeat_pace: Duration::from_millis(1),
        };
        let manager = WatchManager::new(&cancel, api.clone(), room(), options);
        manager.scale_to(3).unwrap();
        tokio::time::timeout(Duration::from_millis(200), async {
            while manager.stats().heartbeats < 3 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("established session should keep heartbeating while others are rate-limited");
        assert_eq!(manager.stats().established, 1);
        assert!(manager.stats().rate_limits >= 1);
        assert_eq!(api.enters.load(Ordering::SeqCst), 1);
        manager.stop().await;
    }
}
