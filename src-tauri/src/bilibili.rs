use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Duration,
};

use anyhow::{bail, Context};
use chrono::Utc;
use md5::{Digest as Md5Digest, Md5};
use reqwest::{header, Method};
use serde_json::{json, Map, Value};
use tokio_util::sync::CancellationToken;

use crate::{
    domain::{BiliApi, Cookie, LiveRoomInfo, TaskProgress, TraceState},
    tasks::parse_task_progress_with_allowed_by_task,
    watch_protocol::{build_x25kn_signature, strict_secret_rules, X25knSignaturePayload},
};

pub const BILIBILI_USER_AGENT: &str =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0";

const WBI_MIXIN_KEY_TABLE: [usize; 64] = [
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35, 27, 43, 5, 49, 33, 9, 42, 19, 29,
    28, 14, 39, 12, 38, 41, 13, 37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4, 22, 25,
    54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
];

const NAV: &str = "https://api.bilibili.com/x/web-interface/nav";
const TASK: &str = "https://api.bilibili.com/x/task/totalv2";
const MISSION: &str = "https://api.bilibili.com/x/activity_components/mission/info";
const RECEIVE: &str = "https://api.bilibili.com/x/activity_components/mission/receive";
const ROOM_BASE: &str = "https://api.live.bilibili.com/xlive/web-room/v1/index/getRoomBaseInfo";
const LIVE_ROOM_PAGE: &str = "https://live.bilibili.com/";
const ROOM_ENTRY: &str = "https://api.live.bilibili.com/xlive/web-room/v1/index/roomEntryAction";
const TRACE_ENTER: &str = "https://live-trace.bilibili.com/xlive/data-interface/v1/x25Kn/E";
const TRACE_HEARTBEAT: &str = "https://live-trace.bilibili.com/xlive/data-interface/v1/x25Kn/X";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RewardInfo {
    pub task_id: String,
    pub task_name: String,
    pub status: i32,
    pub message: String,
    pub activity_id: String,
    pub activity_name: String,
    pub reward_name: String,
}

#[derive(Clone)]
pub struct BiliClient {
    http: reqwest::Client,
    cookies: HashMap<String, String>,
    cookie_header: String,
    csrf: String,
    user_agent: String,
    wbi: std::sync::Arc<std::sync::Mutex<WbiState>>,
    wbi_refresh: std::sync::Arc<tokio::sync::Mutex<()>>,
    active_checkpoint_ids_by_task:
        std::sync::Arc<std::sync::Mutex<HashMap<String, HashSet<String>>>>,
    endpoint_base: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct WbiState {
    img_key: String,
    sub_key: String,
    generation: u64,
}

impl BiliClient {
    pub fn new(browser_cookies: &[Cookie], browser_user_agent: &str) -> anyhow::Result<Self> {
        let mut values = HashMap::new();
        for cookie in preferred_cookies(browser_cookies) {
            if !allowed_cookie_name(&cookie.name) {
                continue;
            }
            values.insert(cookie.name.clone(), cookie.value.clone());
        }
        for required in ["SESSDATA", "bili_jct", "DedeUserID"] {
            if values
                .get(required)
                .is_none_or(|value| value.trim().is_empty())
            {
                bail!("Bilibili cookie is missing {required}");
            }
        }
        if values
            .get("buvid3")
            .is_none_or(|value| value.trim().is_empty())
        {
            values.insert(
                "buvid3".into(),
                format!("{}infoc", uuid::Uuid::new_v4().simple()),
            );
        }
        Ok(Self::from_parts(
            reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(20))
                .pool_max_idle_per_host(64)
                .build()?,
            values,
            browser_user_agent,
            None,
        ))
    }

    pub fn from_discovery(discovery: &crate::domain::DiscoveryResult) -> anyhow::Result<Self> {
        Self::new(&discovery.cookies, &discovery.user_agent)
    }

    #[cfg(test)]
    pub fn for_test(http: reqwest::Client, cookies: HashMap<String, String>, base: &str) -> Self {
        Self::from_parts(
            http,
            cookies,
            "test-agent",
            Some(base.trim_end_matches('/').into()),
        )
    }

    fn from_parts(
        http: reqwest::Client,
        cookies: HashMap<String, String>,
        browser_user_agent: &str,
        endpoint_base: Option<String>,
    ) -> Self {
        let mut names: Vec<_> = cookies.keys().cloned().collect();
        names.sort();
        let cookie_header = names
            .into_iter()
            .filter_map(|name| cookies.get(&name).map(|value| format!("{name}={value}")))
            .collect::<Vec<_>>()
            .join("; ");
        Self {
            http,
            csrf: cookies.get("bili_jct").cloned().unwrap_or_default(),
            cookies,
            cookie_header,
            user_agent: if browser_user_agent.trim().is_empty() {
                BILIBILI_USER_AGENT.into()
            } else {
                browser_user_agent.into()
            },
            wbi: Default::default(),
            wbi_refresh: Default::default(),
            active_checkpoint_ids_by_task: Default::default(),
            endpoint_base,
        }
    }

    pub async fn nav(&self, cancel: &CancellationToken) -> anyhow::Result<Value> {
        let payload = self
            .request_json(
                cancel,
                Method::GET,
                self.endpoint(NAV),
                &BTreeMap::new(),
                None,
                None,
                &BTreeMap::new(),
            )
            .await?;
        ensure_code(&payload, "Bilibili login check")?;
        let data = payload
            .get("data")
            .and_then(Value::as_object)
            .context("nav has no data")?;
        if data.get("isLogin").and_then(Value::as_bool) == Some(false) {
            bail!("Bilibili session is not logged in");
        }
        let image = data
            .get("wbi_img")
            .and_then(Value::as_object)
            .context("nav has no WBI image")?;
        let img_key = image_key(
            image
                .get("img_url")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        );
        let sub_key = image_key(
            image
                .get("sub_url")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        );
        if img_key.is_empty() || sub_key.is_empty() {
            bail!("Bilibili nav response did not contain WBI keys");
        }
        let mut wbi = self
            .wbi
            .lock()
            .map_err(|_| anyhow::anyhow!("WBI state poisoned"))?;
        wbi.img_key = img_key;
        wbi.sub_key = sub_key;
        wbi.generation += 1;
        Ok(payload)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn signed_request(
        &self,
        cancel: &CancellationToken,
        method: Method,
        endpoint: &str,
        params: &BTreeMap<String, String>,
        body: Option<Value>,
        form: Option<BTreeMap<String, String>>,
        headers: &BTreeMap<String, String>,
    ) -> anyhow::Result<Value> {
        for attempt in 0..2 {
            self.ensure_wbi(cancel).await?;
            let (signed, generation) = self.sign_wbi(params)?;
            let payload = self
                .request_json(
                    cancel,
                    method.clone(),
                    endpoint.to_string(),
                    &signed,
                    body.clone(),
                    form.clone(),
                    headers,
                )
                .await?;
            if payload_code(&payload) == "0"
                || attempt == 1
                || is_rate_limited(&payload)
                || !is_wbi_signature_error(&payload)
                || method != Method::GET
            {
                if method != Method::GET && is_wbi_signature_error(&payload) {
                    self.clear_wbi_if_generation(generation);
                }
                return Ok(payload);
            }
            self.clear_wbi_if_generation(generation);
        }
        bail!("unreachable signed request state")
    }

    async fn ensure_wbi(&self, cancel: &CancellationToken) -> anyhow::Result<()> {
        let _refresh = self.wbi_refresh.lock().await;
        let ready = self
            .wbi
            .lock()
            .map_err(|_| anyhow::anyhow!("WBI state poisoned"))
            .map(|wbi| !wbi.img_key.is_empty() && !wbi.sub_key.is_empty())?;
        if !ready {
            self.nav(cancel).await?;
        }
        Ok(())
    }

    fn sign_wbi(
        &self,
        params: &BTreeMap<String, String>,
    ) -> anyhow::Result<(BTreeMap<String, String>, u64)> {
        let wbi = self
            .wbi
            .lock()
            .map_err(|_| anyhow::anyhow!("WBI state poisoned"))?;
        let combined = format!("{}{}", wbi.img_key, wbi.sub_key);
        let mixin: String = WBI_MIXIN_KEY_TABLE
            .iter()
            .filter_map(|index| combined.as_bytes().get(*index).copied())
            .map(char::from)
            .take(32)
            .collect();
        let mut signed = params.clone();
        signed.insert("wts".into(), Utc::now().timestamp().to_string());
        let query = wbi_signing_query(&signed);
        signed.insert(
            "w_rid".into(),
            md5_hex(format!("{query}{mixin}").as_bytes()),
        );
        Ok((signed, wbi.generation))
    }

    fn clear_wbi_if_generation(&self, generation: u64) {
        if let Ok(mut wbi) = self.wbi.lock() {
            if wbi.generation == generation {
                wbi.img_key.clear();
                wbi.sub_key.clear();
            }
        }
    }

    pub async fn task_progress(
        &self,
        cancel: &CancellationToken,
        task_ids: &[String],
    ) -> anyhow::Result<Vec<TaskProgress>> {
        let ids = normalized_task_ids(task_ids);
        if ids.is_empty() {
            bail!("no task IDs were discovered");
        }
        let mut params = BTreeMap::new();
        params.insert("csrf".into(), self.csrf.clone());
        params.insert("task_ids".into(), ids.join(","));
        params.insert("web_location".into(), "0.0".into());
        let payload = self
            .signed_request(
                cancel,
                Method::GET,
                &self.endpoint(TASK),
                &params,
                None,
                None,
                &BTreeMap::new(),
            )
            .await?;
        ensure_code(&payload, "get task progress")?;
        let data_value = payload
            .get("data")
            .context("task progress data is not ready")?;
        let Some(data) = data_value.as_object() else {
            if data_value.is_null()
                || data_value
                    .as_array()
                    .map(|items| items.is_empty())
                    .unwrap_or(false)
            {
                return Ok(Vec::new());
            }
            bail!("task progress data is not ready");
        };
        let Some(task_list) = data.get("list").or_else(|| data.get("tasks")) else {
            // A successful response with no task list means the room currently
            // has no active drops. Transport and malformed-payload failures
            // still return an error and stay in the reading state.
            return Ok(Vec::new());
        };
        if task_list.is_null() {
            return Ok(Vec::new());
        }
        anyhow::ensure!(task_list.is_array(), "task progress list is not ready");
        let allowed_checkpoint_ids_by_task = self
            .active_checkpoint_ids_by_task
            .lock()
            .map_err(|_| anyhow::anyhow!("今天的掉宝节点读取状态不可用"))?
            .clone();
        let progress =
            parse_task_progress_with_allowed_by_task(&payload, &allowed_checkpoint_ids_by_task);
        Ok(progress)
    }

    pub async fn discover_task_ids(
        &self,
        cancel: &CancellationToken,
        room_id: u64,
    ) -> anyhow::Result<Vec<String>> {
        anyhow::ensure!(room_id > 0, "直播间 ID 必须为正数");
        let html = self
            .request_text(cancel, format!("{LIVE_ROOM_PAGE}{room_id}"))
            .await?;
        let (task_ids, checkpoint_ids_by_task) = extract_task_scope_from_page(&html)?;
        if let Ok(mut active) = self.active_checkpoint_ids_by_task.lock() {
            *active = checkpoint_ids_by_task;
        }
        Ok(task_ids)
    }

    pub async fn resolve_room(
        &self,
        cancel: &CancellationToken,
        requested_room_id: u64,
    ) -> anyhow::Result<LiveRoomInfo> {
        if requested_room_id == 0 {
            bail!("直播间 ID 必须为正数");
        }
        let mut params = BTreeMap::new();
        params.insert("room_ids".into(), requested_room_id.to_string());
        params.insert("req_biz".into(), "web_heartbeat".into());
        let mut headers = BTreeMap::new();
        headers.insert(
            "Referer".into(),
            format!("https://live.bilibili.com/{requested_room_id}"),
        );
        let payload = self
            .request_json(
                cancel,
                Method::GET,
                self.endpoint(ROOM_BASE),
                &params,
                None,
                None,
                &headers,
            )
            .await?;
        ensure_code(&payload, "获取直播间信息")?;
        let raw = payload
            .get("data")
            .and_then(|value| value.get("by_room_ids"));
        let room = find_room_base_info(raw, requested_room_id).context("直播间信息缺失")?;
        if room.room_id == 0 || room.ruid == 0 || room.parent_area_id == 0 || room.area_id == 0 {
            bail!("直播间信息不完整");
        }
        Ok(room)
    }

    pub async fn enter_room(
        &self,
        cancel: &CancellationToken,
        room: &LiveRoomInfo,
    ) -> anyhow::Result<()> {
        let mut params = BTreeMap::new();
        params.insert("csrf".into(), self.csrf.clone());
        let body = json!({"room_id": room.room_id, "platform": "pc"});
        let mut headers = BTreeMap::new();
        headers.insert(
            "Referer".into(),
            format!("https://live.bilibili.com/{}", room.room_id),
        );
        let payload = self
            .signed_request(
                cancel,
                Method::POST,
                &self.endpoint(ROOM_ENTRY),
                &params,
                Some(body),
                None,
                &headers,
            )
            .await?;
        ensure_code(&payload, "进入直播间")
    }

    pub async fn trace_enter(
        &self,
        cancel: &CancellationToken,
        room: &LiveRoomInfo,
        page_uuid: &str,
    ) -> anyhow::Result<TraceState> {
        let page_uuid = if page_uuid.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            page_uuid.into()
        };
        let buvid = self.live_trace_buvid();
        let mut params = BTreeMap::new();
        params.insert(
            "id".into(),
            serde_json::to_string(&[room.parent_area_id, room.area_id, 0, room.room_id])?,
        );
        params.insert(
            "device".into(),
            serde_json::to_string(&[buvid, page_uuid.clone()])?,
        );
        params.insert("ruid".into(), room.ruid.to_string());
        params.insert("ts".into(), Utc::now().timestamp_millis().to_string());
        params.insert("is_patch".into(), "0".into());
        params.insert("heart_beat".into(), "[]".into());
        params.insert("ua".into(), self.user_agent.clone());
        params.insert("web_location".into(), "444.8".into());
        params.insert("csrf".into(), self.csrf.clone());
        let mut headers = BTreeMap::new();
        headers.insert(
            "Referer".into(),
            format!("https://live.bilibili.com/{}", room.room_id),
        );
        let payload = self
            .signed_request(
                cancel,
                Method::POST,
                &self.endpoint(TRACE_ENTER),
                &params,
                None,
                None,
                &headers,
            )
            .await?;
        ensure_code(&payload, "x25Kn/E")?;
        let data = payload
            .get("data")
            .and_then(Value::as_object)
            .context("x25Kn/E 没有 data")?;
        let heartbeat = strict_i64(data.get("heartbeat_interval"))
            .context("x25Kn/E 缺少 heartbeat_interval")?;
        let timestamp = strict_i64(data.get("timestamp")).context("x25Kn/E 缺少 timestamp")?;
        let secret_key = data
            .get("secret_key")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_string();
        let secret_rule = strict_secret_rules(data.get("secret_rule"))?;
        if heartbeat <= 0 || timestamp <= 0 || secret_key.is_empty() || secret_rule.is_empty() {
            bail!("x25Kn/E 返回的会话状态不完整");
        }
        Ok(TraceState {
            room: room.clone(),
            page_uuid,
            seq: 0,
            timestamp,
            heartbeat_interval: Duration::from_secs(heartbeat as u64),
            secret_key,
            secret_rule,
        })
    }

    pub async fn trace_heartbeat(
        &self,
        cancel: &CancellationToken,
        state: &TraceState,
    ) -> anyhow::Result<TraceState> {
        if state.secret_key.is_empty() || state.secret_rule.is_empty() || state.page_uuid.is_empty()
        {
            bail!("x25Kn 会话状态不完整");
        }
        let duration = if state.heartbeat_interval.is_zero() {
            60
        } else {
            state.heartbeat_interval.as_secs() as i64
        };
        let next_seq = state.seq + 1;
        let now = Utc::now().timestamp_millis();
        let signature = build_x25kn_signature(
            &X25knSignaturePayload {
                platform: "web",
                parent_id: state.room.parent_area_id,
                area_id: state.room.area_id,
                seq_id: next_seq,
                room_id: state.room.room_id,
                buvid: self.live_trace_buvid(),
                uuid: &state.page_uuid,
                ets: state.timestamp,
                time: duration,
                ts: now,
            },
            &state.secret_key,
            &state.secret_rule,
        )?;
        let buvid = self.live_trace_buvid();
        let mut params = BTreeMap::new();
        params.insert("s".into(), signature);
        params.insert(
            "id".into(),
            serde_json::to_string(&json!([
                state.room.parent_area_id,
                state.room.area_id,
                next_seq,
                state.room.room_id
            ]))?,
        );
        params.insert(
            "device".into(),
            serde_json::to_string(&[buvid, state.page_uuid.clone()])?,
        );
        params.insert("ruid".into(), state.room.ruid.to_string());
        params.insert("ets".into(), state.timestamp.to_string());
        params.insert("benchmark".into(), state.secret_key.clone());
        params.insert("time".into(), duration.to_string());
        params.insert("ts".into(), now.to_string());
        params.insert("trackid".into(), "-999998".into());
        params.insert("ua".into(), self.user_agent.clone());
        params.insert("web_location".into(), "444.8".into());
        params.insert("csrf".into(), self.csrf.clone());
        let mut headers = BTreeMap::new();
        headers.insert(
            "Referer".into(),
            format!("https://live.bilibili.com/{}", state.room.room_id),
        );
        let payload = self
            .signed_request(
                cancel,
                Method::POST,
                &self.endpoint(TRACE_HEARTBEAT),
                &params,
                None,
                None,
                &headers,
            )
            .await?;
        ensure_code(&payload, "x25Kn/X")?;
        let data = payload
            .get("data")
            .and_then(Value::as_object)
            .context("x25Kn/X 没有 data")?;
        let mut next = state.clone();
        next.seq = next_seq;
        next.timestamp = strict_i64(data.get("timestamp"))
            .filter(|timestamp| *timestamp > 0)
            .unwrap_or(state.timestamp + duration);
        if let Some(interval) = strict_i64(data.get("heartbeat_interval")) {
            if interval > 0 {
                next.heartbeat_interval = Duration::from_secs(interval as u64);
            }
        }
        if let Some(key) = data
            .get("secret_key")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            next.secret_key = key.into();
        }
        if let Some(raw_rules) = data.get("secret_rule").filter(|value| !value.is_null()) {
            next.secret_rule = strict_secret_rules(Some(raw_rules))?;
            if next.secret_rule.is_empty() {
                bail!("x25Kn/X secret_rule 为空");
            }
        }
        Ok(next)
    }

    pub fn live_trace_buvid(&self) -> String {
        self.cookies
            .get("LIVE_BUVID")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .unwrap_or("null")
            .into()
    }

    pub async fn reward_info(
        &self,
        cancel: &CancellationToken,
        task_id: &str,
    ) -> anyhow::Result<RewardInfo> {
        let mut params = BTreeMap::new();
        params.insert("task_id".into(), task_id.into());
        let mut headers = BTreeMap::new();
        headers.insert(
            "Referer".into(),
            format!(
                "https://www.bilibili.com/blackboard/era/award-exchange.html?task_id={task_id}"
            ),
        );
        let payload = self
            .signed_request(
                cancel,
                Method::GET,
                &self.endpoint(MISSION),
                &params,
                None,
                None,
                &headers,
            )
            .await?;
        ensure_code(&payload, "get reward info")?;
        let data = payload
            .get("data")
            .and_then(Value::as_object)
            .context("reward info has no data")?;
        let status =
            strict_i64(data.get("status")).context("reward info has no valid status")? as i32;
        let returned_task = string_value(data.get("task_id"));
        if !returned_task.is_empty() && returned_task != task_id {
            bail!("reward info task mismatch: requested {task_id}, got {returned_task}");
        }
        let reward = data.get("reward_info").and_then(Value::as_object);
        let info = RewardInfo {
            task_id: if returned_task.is_empty() {
                task_id.into()
            } else {
                returned_task
            },
            task_name: {
                let value = string_value(data.get("task_name"));
                if value.trim().is_empty() {
                    task_id.into()
                } else {
                    value
                }
            },
            status,
            message: string_value(data.get("message")),
            activity_id: string_value(data.get("act_id")),
            activity_name: string_value(data.get("act_name")),
            reward_name: reward
                .map(|value| string_value(value.get("award_name")))
                .unwrap_or_default(),
        };
        if info.status == 0 {
            validate_claimable_reward(&info)?;
        }
        Ok(info)
    }

    pub async fn receive_reward(
        &self,
        cancel: &CancellationToken,
        info: &RewardInfo,
    ) -> anyhow::Result<()> {
        if is_reward_settled_status(info.status) {
            return Ok(());
        }
        if info.status != 0 {
            bail!(
                "reward {} is not claimable: status={} message={}",
                info.task_id,
                info.status,
                info.message
            );
        }
        validate_claimable_reward(info)?;
        let form = BTreeMap::from([
            ("task_id".into(), info.task_id.clone()),
            ("activity_id".into(), info.activity_id.clone()),
            ("activity_name".into(), info.activity_name.clone()),
            ("task_name".into(), info.task_name.clone()),
            ("reward_name".into(), info.reward_name.clone()),
            ("gaia_vtoken".into(), String::new()),
            ("receive_from".into(), "missionPage".into()),
            ("csrf".into(), self.csrf.clone()),
            ("csrf_token".into(), self.csrf.clone()),
        ]);
        let mut headers = BTreeMap::new();
        headers.insert(
            "Content-Type".into(),
            "application/x-www-form-urlencoded".into(),
        );
        headers.insert(
            "Referer".into(),
            format!(
                "https://www.bilibili.com/blackboard/era/award-exchange.html?task_id={}",
                info.task_id
            ),
        );
        let payload = self
            .signed_request(
                cancel,
                Method::POST,
                &self.endpoint(RECEIVE),
                &BTreeMap::new(),
                None,
                Some(form),
                &headers,
            )
            .await?;
        ensure_code(&payload, &format!("claim reward for {}", info.task_id))
    }

    #[allow(clippy::too_many_arguments)]
    async fn request_json(
        &self,
        cancel: &CancellationToken,
        method: Method,
        endpoint: String,
        params: &BTreeMap<String, String>,
        body: Option<Value>,
        form: Option<BTreeMap<String, String>>,
        headers: &BTreeMap<String, String>,
    ) -> anyhow::Result<Value> {
        let max_attempts = if method == Method::GET { 3 } else { 1 };
        let mut last_error = None;
        for attempt in 0..max_attempts {
            if cancel.is_cancelled() {
                bail!("操作已取消");
            }
            let request_url = if params.is_empty() {
                endpoint.clone()
            } else {
                format!("{endpoint}?{}", encode_query_values(params))
            };
            let mut request = self.http.request(method.clone(), &request_url);
            request = request
                .header(header::ACCEPT, "application/json, text/plain, */*")
                .header(header::USER_AGENT, &self.user_agent)
                .header(header::COOKIE, &self.cookie_header);
            if endpoint.contains("live.bilibili.com")
                || endpoint.contains("live-trace.bilibili.com")
            {
                request = request
                    .header(header::ORIGIN, "https://live.bilibili.com")
                    .header(header::REFERER, "https://live.bilibili.com/");
            } else {
                request = request
                    .header(header::ORIGIN, "https://www.bilibili.com")
                    .header(header::REFERER, "https://www.bilibili.com/");
            }
            for (key, value) in headers {
                request = request.header(key, value);
            }
            if let Some(body) = &body {
                request = request.json(body);
            }
            if let Some(form) = &form {
                request = request.form(form);
            }
            let response = tokio::select! {
                _ = cancel.cancelled() => bail!("操作已取消"),
                response = request.send() => response,
            };
            match response {
                Ok(response) => {
                    let status = response.status();
                    if !status.is_success() {
                        bail!("HTTP {} from {}", status, endpoint_path(&endpoint));
                    }
                    if response
                        .content_length()
                        .is_some_and(|length| length > 4 << 20)
                    {
                        bail!("response from {} exceeds 4 MiB", endpoint_path(&endpoint));
                    }
                    let bytes = tokio::select! {
                        _ = cancel.cancelled() => bail!("操作已取消"),
                        bytes = response.bytes() => bytes,
                    };
                    match bytes {
                        Ok(bytes) => {
                            if bytes.len() > 4 << 20 {
                                bail!("response from {} exceeds 4 MiB", endpoint_path(&endpoint));
                            }
                            return serde_json::from_slice(&bytes).with_context(|| {
                                format!("invalid JSON from {}", endpoint_path(&endpoint))
                            });
                        }
                        Err(error) => {
                            last_error = Some(error);
                            if attempt + 1 < max_attempts {
                                tokio::select! {
                                    _ = cancel.cancelled() => bail!("操作已取消"),
                                    _ = tokio::time::sleep([Duration::from_millis(350), Duration::from_millis(800)][attempt]) => {}
                                }
                                continue;
                            }
                        }
                    }
                }
                Err(error) => {
                    last_error = Some(error);
                    if attempt + 1 < max_attempts {
                        tokio::select! { _ = cancel.cancelled() => bail!("操作已取消"), _ = tokio::time::sleep([Duration::from_millis(350), Duration::from_millis(800)][attempt]) => {} }
                    }
                }
            }
        }
        Err(anyhow::anyhow!(
            "request to {} failed after retries: {}",
            endpoint_path(&endpoint),
            last_error
                .map(|v| v.to_string())
                .unwrap_or_else(|| "request failed".into())
        ))
    }

    async fn request_text(
        &self,
        cancel: &CancellationToken,
        endpoint: String,
    ) -> anyhow::Result<String> {
        if cancel.is_cancelled() {
            bail!("操作已取消");
        }
        let request = self
            .http
            .get(&endpoint)
            .header(header::ACCEPT, "text/html,application/xhtml+xml")
            .header(header::USER_AGENT, &self.user_agent)
            .header(header::COOKIE, &self.cookie_header)
            .header(header::ORIGIN, "https://live.bilibili.com")
            .header(header::REFERER, "https://live.bilibili.com/");
        let response = tokio::select! {
            _ = cancel.cancelled() => bail!("操作已取消"),
            response = request.send() => response.context("请求直播间页面失败")?,
        };
        anyhow::ensure!(
            response.status().is_success(),
            "HTTP {} from {}",
            response.status(),
            endpoint_path(&endpoint)
        );
        if response
            .content_length()
            .is_some_and(|length| length > 8 << 20)
        {
            bail!("直播间页面超过 8 MiB");
        }
        let bytes = tokio::select! {
            _ = cancel.cancelled() => bail!("操作已取消"),
            bytes = response.bytes() => bytes.context("读取直播间页面失败")?,
        };
        anyhow::ensure!(bytes.len() <= 8 << 20, "直播间页面超过 8 MiB");
        String::from_utf8(bytes.to_vec()).context("直播间页面不是有效文本")
    }

    fn endpoint(&self, absolute: &str) -> String {
        self.endpoint_base
            .as_ref()
            .map(|base| format!("{base}/{}", absolute.rsplit('/').next().unwrap_or_default()))
            .unwrap_or_else(|| absolute.into())
    }
}

#[async_trait::async_trait]
impl BiliApi for BiliClient {
    async fn nav(&self, cancel: &CancellationToken) -> anyhow::Result<Value> {
        self.nav(cancel).await
    }
    async fn task_progress(
        &self,
        cancel: &CancellationToken,
        task_ids: &[String],
    ) -> anyhow::Result<Vec<TaskProgress>> {
        self.task_progress(cancel, task_ids).await
    }
    async fn resolve_room(
        &self,
        cancel: &CancellationToken,
        requested_room_id: u64,
    ) -> anyhow::Result<LiveRoomInfo> {
        self.resolve_room(cancel, requested_room_id).await
    }
    async fn enter_room(
        &self,
        cancel: &CancellationToken,
        room: &LiveRoomInfo,
    ) -> anyhow::Result<()> {
        self.enter_room(cancel, room).await
    }
    async fn trace_enter(
        &self,
        cancel: &CancellationToken,
        room: &LiveRoomInfo,
        page_uuid: &str,
    ) -> anyhow::Result<crate::domain::TraceState> {
        self.trace_enter(cancel, room, page_uuid).await
    }
    async fn trace_heartbeat(
        &self,
        cancel: &CancellationToken,
        state: &crate::domain::TraceState,
    ) -> anyhow::Result<crate::domain::TraceState> {
        self.trace_heartbeat(cancel, state).await
    }
    async fn claim_reward(
        &self,
        cancel: &CancellationToken,
        checkpoint_id: &str,
    ) -> anyhow::Result<()> {
        let delays = [
            Duration::ZERO,
            Duration::from_millis(1_500),
            Duration::from_secs(3),
        ];
        let mut last_error = None;
        for (attempt, delay) in delays.into_iter().enumerate() {
            if !delay.is_zero() {
                tokio::select! { _ = cancel.cancelled() => bail!("操作已取消"), _ = tokio::time::sleep(delay) => {} }
            }
            match self.reward_info(cancel, checkpoint_id).await {
                Ok(info) => {
                    if is_reward_settled_status(info.status) {
                        return Ok(());
                    }
                    if info.status != 0 {
                        bail!(
                            "奖励 {checkpoint_id} 当前不可领取 status={} message={}",
                            info.status,
                            info.message
                        );
                    }
                    return self.receive_reward(cancel, &info).await;
                }
                Err(error) => {
                    let retry = is_rate_limit_error(&error) && attempt + 1 < delays.len();
                    last_error = Some(error);
                    if !retry {
                        break;
                    }
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("claim reward failed")))
    }
}

fn is_reward_settled_status(status: i32) -> bool {
    matches!(status, 3 | 6)
}

pub fn preferred_cookies(cookies: &[Cookie]) -> Vec<Cookie> {
    let mut selected = HashMap::<String, Cookie>::new();
    for cookie in cookies {
        if cookie.name.is_empty() || cookie.value.is_empty() || !is_bilibili_host(&cookie.domain) {
            continue;
        }
        let replace = selected
            .get(&cookie.name)
            .is_none_or(|current| cookie_preference(cookie) > cookie_preference(current));
        if replace {
            selected.insert(cookie.name.clone(), cookie.clone());
        }
    }
    let mut result: Vec<_> = selected.into_values().collect();
    result.sort_by(|left, right| left.name.cmp(&right.name));
    result
}

pub fn allowed_cookie_name(name: &str) -> bool {
    matches!(
        name,
        "SESSDATA"
            | "bili_jct"
            | "DedeUserID"
            | "DedeUserID__ckMd5"
            | "buvid3"
            | "b_nut"
            | "sid"
            | "LIVE_BUVID"
            | "buvid4"
            | "buvid_fp"
            | "b_lsid"
    )
}

fn is_bilibili_host(value: &str) -> bool {
    let host = value.trim().trim_start_matches('.').to_lowercase();
    host == "bilibili.com" || host.ends_with(".bilibili.com")
}
fn cookie_preference(cookie: &Cookie) -> f64 {
    (if cookie
        .domain
        .trim()
        .trim_start_matches('.')
        .eq_ignore_ascii_case("bilibili.com")
    {
        100.0
    } else {
        0.0
    }) + (if cookie.path.is_empty() || cookie.path == "/" {
        10.0
    } else {
        0.0
    }) + cookie.expiration_date.unwrap_or_default() / 1e12
}
fn normalized_task_ids(values: &[String]) -> Vec<String> {
    let mut ids = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for value in values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        if seen.insert(value.to_string()) {
            ids.push(value.to_string());
        }
    }
    ids
}

type TaskCheckpointMap = HashMap<String, HashSet<String>>;

fn extract_task_scope_from_page(html: &str) -> anyhow::Result<(Vec<String>, TaskCheckpointMap)> {
    let marker = "window.__BILIACT_EVAPAGEDATA__";
    let Some(marker_start) = html.find(marker) else {
        // A valid room page without the activity payload means there is no
        // active drop panel today. Transport errors are returned by the caller.
        return Ok((Vec::new(), HashMap::new()));
    };
    let assignment_start =
        eva_assignment_start(html, marker, marker_start).context("直播间掉宝页面数据不完整")?;
    let start = assignment_start;
    let payload = html[start..].trim_start();
    if payload.starts_with("null") || payload.starts_with("undefined") || payload.starts_with("[]")
    {
        return Ok((Vec::new(), HashMap::new()));
    }
    let raw_json = balanced_json_object(payload).context("直播间掉宝页面数据不完整")?;
    let root = serde_json::from_str::<Value>(raw_json).context("直播间掉宝页面数据无效")?;
    if root.is_null() || root.is_array() {
        return Ok((Vec::new(), HashMap::new()));
    }
    let Some(active_panel_id) = find_activated_panel_id(&root) else {
        // The page payload is valid, but there is no active panel for today.
        // This is a confirmed empty state, not a transient request failure.
        return Ok((Vec::new(), HashMap::new()));
    };
    let Some(active_panel) = find_active_panel(&root, active_panel_id) else {
        // Some rooms publish a valid activity payload without a rendered tab
        // panel when today's drop list is empty.
        return Ok((Vec::new(), HashMap::new()));
    };

    let mut values = Vec::new();
    let mut checkpoint_ids_by_task = HashMap::new();
    let has_task_list =
        collect_task_list_data(active_panel, &mut values, &mut checkpoint_ids_by_task);
    if !has_task_list {
        // The active panel is present, but a no-drop panel has no tasklist at all.
        // Missing marker/panel still errors above and remains in the retry state.
        return Ok((Vec::new(), HashMap::new()));
    }
    let task_ids = normalized_task_ids(&values);
    for task_id in &task_ids {
        checkpoint_ids_by_task.entry(task_id.clone()).or_default();
    }
    Ok((task_ids, checkpoint_ids_by_task))
}

fn eva_assignment_start(html: &str, marker: &str, first_marker: usize) -> Option<usize> {
    let bytes = html.as_bytes();
    let mut marker_start = first_marker;
    loop {
        let mut cursor = marker_start + marker.len();
        while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        if bytes.get(cursor) == Some(&b'=') {
            cursor += 1;
            while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
                cursor += 1;
            }
            if matches!(
                bytes.get(cursor),
                Some(&b'{') | Some(&b'[') | Some(&b'n') | Some(&b'u')
            ) {
                return Some(cursor);
            }
        }
        let next = html[cursor.min(html.len())..].find(marker)?;
        marker_start = cursor + next;
    }
}

fn balanced_json_object(raw: &str) -> Option<&str> {
    let bytes = raw.as_bytes();
    let start = bytes.iter().position(|byte| *byte == b'{')?;
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for index in start..bytes.len() {
        let byte = bytes[index];
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' => depth += 1,
            b'}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(&raw[start..=index]);
                }
            }
            _ => {}
        }
    }
    None
}

fn find_active_panel<'a>(value: &'a Value, active_panel_id: &str) -> Option<&'a Value> {
    match value {
        Value::Object(object) => {
            let props = object.get("props").and_then(Value::as_object);
            let is_panel = object.get("id").and_then(Value::as_str) == Some(active_panel_id)
                || props.and_then(|props| props.get("id").and_then(Value::as_str))
                    == Some(active_panel_id);
            if is_panel {
                return Some(value);
            }
            object
                .values()
                .find_map(|child| find_active_panel(child, active_panel_id))
        }
        Value::Array(items) => items
            .iter()
            .find_map(|child| find_active_panel(child, active_panel_id)),
        _ => None,
    }
}

fn find_activated_panel_id(value: &Value) -> Option<&str> {
    match value {
        Value::Object(object) => object
            .get("activatedTabPanelId")
            .and_then(Value::as_str)
            .or_else(|| object.values().find_map(find_activated_panel_id)),
        Value::Array(items) => items.iter().find_map(find_activated_panel_id),
        _ => None,
    }
}

fn collect_task_list_data(
    value: &Value,
    values: &mut Vec<String>,
    checkpoint_ids_by_task: &mut HashMap<String, HashSet<String>>,
) -> bool {
    match value {
        Value::Object(object) => {
            let mut found = false;
            if object.get("tasklist").and_then(Value::as_array).is_some() {
                collect_task_list_entries(object.get("tasklist"), values, checkpoint_ids_by_task);
                found = true;
            }
            let props_tasklist = object
                .get("props")
                .and_then(Value::as_object)
                .and_then(|props| props.get("tasklist"));
            if props_tasklist.and_then(Value::as_array).is_some() {
                collect_task_list_entries(props_tasklist, values, checkpoint_ids_by_task);
                found = true;
            }
            for child in object.values() {
                found |= collect_task_list_data(child, values, checkpoint_ids_by_task);
            }
            found
        }
        Value::Array(items) => {
            let mut found = false;
            for child in items {
                found |= collect_task_list_data(child, values, checkpoint_ids_by_task);
            }
            found
        }
        _ => false,
    }
}

fn collect_task_list_entries(
    raw_tasks: Option<&Value>,
    values: &mut Vec<String>,
    checkpoint_ids_by_task: &mut HashMap<String, HashSet<String>>,
) {
    let Some(tasks) = raw_tasks.and_then(Value::as_array) else {
        return;
    };
    for task in tasks.iter().filter_map(Value::as_object) {
        let Some(task_id) = task
            .get("taskId")
            .or_else(|| task.get("task_id"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|id| !id.is_empty())
        else {
            continue;
        };
        values.push(task_id.to_string());
        let checkpoint_ids = checkpoint_ids_by_task
            .entry(task_id.to_string())
            .or_default();
        if !is_watch_duration_page_task(task) {
            collect_checkpoint_ids(task, checkpoint_ids);
        }
    }
}

fn is_watch_duration_page_task(task: &Map<String, Value>) -> bool {
    ["task_name", "taskName", "name", "title", "alias"]
        .iter()
        .map(|key| string_value(task.get(*key)).to_lowercase())
        .any(|value| {
            value.contains("观看时长")
                || value.contains("watch_time")
                || value.contains("view_time")
        })
}

fn collect_checkpoint_ids(task: &Map<String, Value>, checkpoint_ids: &mut HashSet<String>) {
    let Some(checkpoints) = task.get("checkpoints").and_then(Value::as_array) else {
        return;
    };
    for checkpoint in checkpoints.iter().filter_map(Value::as_object) {
        for key in [
            "awardsid",
            "ztasksid",
            "sid",
            "task_id",
            "taskId",
            "id",
            "checkpoint_id",
            "checkpointId",
        ] {
            if let Some(id) = checkpoint.get(key).and_then(Value::as_str) {
                if !id.trim().is_empty() {
                    checkpoint_ids.insert(id.to_string());
                }
            }
        }
    }
}
fn image_key(value: &str) -> String {
    value
        .rsplit('/')
        .next()
        .unwrap_or(value)
        .split('.')
        .next()
        .unwrap_or_default()
        .into()
}
fn md5_hex(value: &[u8]) -> String {
    let mut digest = Md5::new();
    digest.update(value);
    hex::encode(digest.finalize())
}
fn endpoint_path(value: &str) -> String {
    url::Url::parse(value)
        .map(|url| format!("{}{}", url.host_str().unwrap_or_default(), url.path()))
        .unwrap_or_else(|_| value.into())
}
fn encode_wbi(value: &str) -> String {
    const QUERY_COMPONENT: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~');
    percent_encoding::utf8_percent_encode(value, QUERY_COMPONENT).to_string()
}
fn encode_query_values(values: &BTreeMap<String, String>) -> String {
    values
        .iter()
        .map(|(key, value)| format!("{}={}", encode_wbi(key), encode_wbi(value)))
        .collect::<Vec<_>>()
        .join("&")
}
fn wbi_signing_query(values: &BTreeMap<String, String>) -> String {
    values
        .iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                encode_wbi(key),
                encode_wbi(
                    &value
                        .chars()
                        .filter(|ch| !matches!(ch, '!' | '\'' | '(' | ')' | '*'))
                        .collect::<String>()
                )
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}
fn payload_code(value: &Value) -> String {
    string_value(value.get("code"))
}
fn payload_message(value: &Value) -> String {
    string_value(value.get("message"))
}
fn ensure_code(value: &Value, operation: &str) -> anyhow::Result<()> {
    let code = payload_code(value);
    if code != "0" {
        bail!(
            "{operation} failed: code={code} message={}",
            payload_message(value)
        );
    }
    Ok(())
}
fn is_rate_limited(value: &Value) -> bool {
    let code = payload_code(value);
    let message = payload_message(value);
    matches!(code.as_str(), "-702" | "-509") || message.contains('频')
}
fn is_wbi_signature_error(value: &Value) -> bool {
    let code = payload_code(value);
    let message = payload_message(value).to_lowercase();
    code == "-403"
        || message.contains("w_rid")
        || message.contains("wbi")
        || message.contains("signature")
        || message.contains("签名")
}
pub fn is_rate_limit_error(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("-702")
        || message.contains("-509")
        || message.contains("HTTP 429")
        || message.contains('频')
}

pub fn is_authentication_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("not logged in") || message.contains("code=-101") || message.contains("未登录")
}
fn strict_i64(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(number) => number.as_i64().or_else(|| {
            number
                .as_f64()
                .filter(|v| v.fract() == 0.0)
                .map(|v| v as i64)
        }),
        Value::String(value) => value.trim().parse().ok(),
        _ => None,
    }
}
fn string_value(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(v)) => v.clone(),
        Some(Value::Number(v)) => v.to_string(),
        Some(Value::Bool(v)) => v.to_string(),
        _ => String::new(),
    }
}
fn find_room_base_info(raw: Option<&Value>, requested: u64) -> Option<LiveRoomInfo> {
    let mut candidates = Vec::new();
    match raw {
        Some(Value::Object(map)) => {
            if let Some(direct) = map.get(&requested.to_string()) {
                candidates.push(direct);
            }
            candidates.extend(map.values());
        }
        Some(Value::Array(items)) => candidates.extend(items),
        _ => {}
    }
    candidates
        .into_iter()
        .filter_map(Value::as_object)
        .find_map(|item| {
            let room_id = strict_i64(item.get("room_id").or_else(|| item.get("short_id")))? as u64;
            let short_id = strict_i64(item.get("short_id")).unwrap_or_default() as u64;
            if room_id != requested && short_id != requested {
                return None;
            }
            Some(LiveRoomInfo {
                room_id,
                ruid: strict_i64(item.get("uid").or_else(|| item.get("ruid"))).unwrap_or_default()
                    as u64,
                parent_area_id: strict_i64(item.get("parent_area_id")).unwrap_or_default() as u64,
                area_id: strict_i64(item.get("area_id")).unwrap_or_default() as u64,
                live_status: strict_i64(item.get("live_status")).unwrap_or_default() as i32,
            })
        })
}
fn validate_claimable_reward(info: &RewardInfo) -> anyhow::Result<()> {
    let mut missing = Vec::new();
    for (name, value) in [
        ("task_id", &info.task_id),
        ("activity_id", &info.activity_id),
        ("activity_name", &info.activity_name),
        ("reward_name", &info.reward_name),
    ] {
        if value.trim().is_empty() {
            missing.push(name);
        }
    }
    if missing.is_empty() {
        Ok(())
    } else {
        missing.sort();
        bail!(
            "reward {} is missing required fields: {}",
            info.task_id,
            missing.join(", ")
        )
    }
}

pub fn uid_from_nav(payload: &Value) -> anyhow::Result<String> {
    let uid = payload
        .get("data")
        .and_then(|data| data.get("mid").or_else(|| data.get("uid")))
        .map(|value| string_value(Some(value)))
        .unwrap_or_default();
    if uid.chars().all(|character| character.is_ascii_digit()) && !uid.is_empty() {
        Ok(uid)
    } else {
        bail!("账号接口没有返回有效用户 ID")
    }
}

pub fn display_name_from_nav(payload: &Value) -> Option<String> {
    payload
        .get("data")
        .and_then(|data| data.get("uname").or_else(|| data.get("name")))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::{Query, State},
        http::StatusCode,
        response::IntoResponse,
        routing::{get, post},
        Json, Router,
    };
    use reqwest::Client;
    use serde_json::json;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    async fn test_server(router: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("http://{address}")
    }

    fn test_cookies() -> HashMap<String, String> {
        HashMap::from([
            ("SESSDATA".into(), "session".into()),
            ("bili_jct".into(), "csrf".into()),
            ("DedeUserID".into(), "1".into()),
        ])
    }

    fn install_wbi(client: &BiliClient) {
        let mut wbi = client.wbi.lock().unwrap();
        wbi.img_key = "a".repeat(32);
        wbi.sub_key = "b".repeat(32);
        wbi.generation = 1;
    }

    #[test]
    fn wbi_query_encoding_matches_go_vector() {
        let values = BTreeMap::from([
            (
                String::from("ua"),
                String::from("Mozilla/5.0 (Windows NT 10.0)"),
            ),
            (String::from("room_id"), String::from("23612045")),
        ]);
        assert_eq!(
            encode_query_values(&values),
            "room_id=23612045&ua=Mozilla%2F5.0%20%28Windows%20NT%2010.0%29"
        );
        assert_eq!(
            wbi_signing_query(&values),
            "room_id=23612045&ua=Mozilla%2F5.0%20Windows%20NT%2010.0"
        );
        assert_eq!(encode_wbi("!*'()~"), "%21%2A%27%28%29~");
    }

    #[test]
    fn task_id_normalization_preserves_discovery_order() {
        assert_eq!(
            normalized_task_ids(&[" second ".into(), "first".into(), "second".into()]),
            vec!["second", "first"]
        );
    }

    #[test]
    fn x25kn_hmac_chain_matches_go_vector() {
        let signature = build_x25kn_signature(
            &X25knSignaturePayload {
                platform: "web",
                parent_id: 1,
                area_id: 2,
                seq_id: 3,
                room_id: 4,
                buvid: "buvid".into(),
                uuid: "uuid",
                ets: 5,
                time: 60,
                ts: 6,
            },
            "fixed-test-secret",
            &[2, 5],
        )
        .unwrap();
        assert_eq!(signature, "faf0d72efe229e16365f100183b32d3da96d58b9e2ea9767efb681f2d012803504a1de6975a1c8f067ddf079071835c0");
    }

    #[test]
    fn missing_live_buvid_is_literal_null() {
        let client = BiliClient::for_test(Client::new(), HashMap::new(), "http://localhost");
        assert_eq!(client.live_trace_buvid(), "null");
    }

    #[test]
    fn malformed_secret_rules_are_rejected() {
        for value in [json!(["bad"]), json!([6]), json!(null), json!("not-array")] {
            assert!(strict_secret_rules(Some(&value)).is_err());
        }
    }

    #[test]
    fn mismatched_room_is_rejected() {
        assert!(find_room_base_info(Some(&json!({"999":{"room_id":999}})), 23612045).is_none());
    }

    #[tokio::test]
    async fn concurrent_wbi_refresh_uses_one_nav_request() {
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = calls.clone();
        let router = Router::new().route(
            "/nav",
            get(move || {
                let calls = server_calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Json(json!({"code":0,"data":{"isLogin":true,"wbi_img":{
                        "img_url":format!("https://i.test/{}.png", "a".repeat(32)),
                        "sub_url":format!("https://i.test/{}.png", "b".repeat(32))
                    }}}))
                }
            }),
        );
        let base = test_server(router).await;
        let client = Arc::new(BiliClient::for_test(Client::new(), test_cookies(), &base));
        let cancel = CancellationToken::new();
        let mut tasks = Vec::new();
        for _ in 0..50 {
            let client = client.clone();
            let cancel = cancel.clone();
            tasks.push(tokio::spawn(
                async move { client.ensure_wbi(&cancel).await },
            ));
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn received_http_errors_are_not_retried_and_reward_post_is_sent_once() {
        #[derive(Clone)]
        struct Counts {
            get: Arc<AtomicUsize>,
            post: Arc<AtomicUsize>,
        }
        async fn rejected_get(State(counts): State<Counts>) -> impl IntoResponse {
            counts.get.fetch_add(1, Ordering::SeqCst);
            (StatusCode::SERVICE_UNAVAILABLE, "do not retry status")
        }
        async fn rejected_post(State(counts): State<Counts>) -> impl IntoResponse {
            counts.post.fetch_add(1, Ordering::SeqCst);
            (StatusCode::SERVICE_UNAVAILABLE, "no retry")
        }
        let counts = Counts {
            get: Arc::new(AtomicUsize::new(0)),
            post: Arc::new(AtomicUsize::new(0)),
        };
        let router = Router::new()
            .route("/nav", get(rejected_get))
            .route("/receive", post(rejected_post))
            .with_state(counts.clone());
        let base = test_server(router).await;
        let client = BiliClient::for_test(Client::new(), test_cookies(), &base);
        let cancel = CancellationToken::new();
        let nav_error = client
            .request_json(
                &cancel,
                Method::GET,
                format!("{base}/nav"),
                &BTreeMap::new(),
                None,
                None,
                &BTreeMap::new(),
            )
            .await
            .unwrap_err();
        assert!(nav_error.to_string().contains("HTTP 503"));
        let error = client
            .request_json(
                &cancel,
                Method::POST,
                format!("{base}/receive"),
                &BTreeMap::new(),
                None,
                Some(BTreeMap::new()),
                &BTreeMap::new(),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("HTTP 503"));
        assert_eq!(counts.get.load(Ordering::SeqCst), 1);
        assert_eq!(counts.post.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn signed_post_wbi_failure_is_never_replayed() {
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = calls.clone();
        let router = Router::new().route(
            "/receive",
            post(move || {
                let calls = server_calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Json(json!({"code":-403,"message":"invalid w_rid"}))
                }
            }),
        );
        let base = test_server(router).await;
        let client = BiliClient::for_test(Client::new(), test_cookies(), &base);
        install_wbi(&client);
        let payload = client
            .signed_request(
                &CancellationToken::new(),
                Method::POST,
                &format!("{base}/receive"),
                &BTreeMap::new(),
                None,
                Some(BTreeMap::new()),
                &BTreeMap::new(),
            )
            .await
            .unwrap();
        assert_eq!(payload_code(&payload), "-403");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let wbi = client.wbi.lock().unwrap();
        assert!(wbi.img_key.is_empty() && wbi.sub_key.is_empty());
    }

    #[test]
    fn stale_wbi_generation_cannot_clear_new_keys() {
        let client = BiliClient::for_test(Client::new(), test_cookies(), "http://localhost");
        {
            let mut wbi = client.wbi.lock().unwrap();
            wbi.img_key = "old-img".into();
            wbi.sub_key = "old-sub".into();
            wbi.generation = 4;
        }
        let (_, old_generation) = client.sign_wbi(&BTreeMap::new()).unwrap();
        {
            let mut wbi = client.wbi.lock().unwrap();
            wbi.img_key = "new-img".into();
            wbi.sub_key = "new-sub".into();
            wbi.generation = 5;
        }
        client.clear_wbi_if_generation(old_generation);
        let wbi = client.wbi.lock().unwrap();
        assert_eq!(
            (&wbi.img_key, &wbi.sub_key),
            (&"new-img".to_string(), &"new-sub".to_string())
        );
    }

    #[tokio::test]
    async fn trace_requests_use_null_buvid_and_advance_missing_timestamp() {
        #[derive(Clone, Default)]
        struct TraceCapture(Arc<std::sync::Mutex<Vec<HashMap<String, String>>>>);
        async fn enter(
            State(capture): State<TraceCapture>,
            Query(query): Query<HashMap<String, String>>,
        ) -> Json<Value> {
            capture.0.lock().unwrap().push(query);
            Json(
                json!({"code":0,"data":{"heartbeat_interval":60,"timestamp":100,"secret_key":"secret","secret_rule":[2]}}),
            )
        }
        async fn heartbeat(
            State(capture): State<TraceCapture>,
            Query(query): Query<HashMap<String, String>>,
        ) -> Json<Value> {
            capture.0.lock().unwrap().push(query);
            Json(json!({"code":0,"data":{"heartbeat_interval":55}}))
        }
        let capture = TraceCapture::default();
        let router = Router::new()
            .route("/E", post(enter))
            .route("/X", post(heartbeat))
            .with_state(capture.clone());
        let base = test_server(router).await;
        let client = BiliClient::for_test(Client::new(), test_cookies(), &base);
        install_wbi(&client);
        let room = LiveRoomInfo {
            room_id: 4,
            ruid: 9,
            parent_area_id: 1,
            area_id: 2,
            live_status: 1,
        };
        let cancel = CancellationToken::new();
        let state = client.trace_enter(&cancel, &room, "uuid").await.unwrap();
        let next = client.trace_heartbeat(&cancel, &state).await.unwrap();
        assert_eq!((next.seq, next.timestamp), (1, 160));
        assert_eq!(next.heartbeat_interval, Duration::from_secs(55));
        let captured = capture.0.lock().unwrap();
        assert_eq!(captured.len(), 2);
        for request in captured.iter() {
            assert_eq!(
                serde_json::from_str::<Vec<String>>(&request["device"]).unwrap(),
                vec!["null", "uuid"]
            );
        }
        assert_eq!(captured[1]["trackid"], "-999998");
    }

    #[tokio::test]
    async fn heartbeat_rejects_explicit_empty_secret_rule() {
        let router = Router::new().route(
            "/X",
            post(|| async { Json(json!({"code":0,"data":{"timestamp":200,"secret_rule":[]}})) }),
        );
        let base = test_server(router).await;
        let client = BiliClient::for_test(Client::new(), test_cookies(), &base);
        install_wbi(&client);
        let state = TraceState {
            room: LiveRoomInfo {
                room_id: 4,
                ruid: 9,
                parent_area_id: 1,
                area_id: 2,
                live_status: 1,
            },
            page_uuid: "uuid".into(),
            seq: 0,
            timestamp: 100,
            heartbeat_interval: Duration::from_secs(60),
            secret_key: "secret".into(),
            secret_rule: vec![2],
        };
        assert!(client
            .trace_heartbeat(&CancellationToken::new(), &state)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn zero_local_interval_uses_safe_sixty_second_fallback() {
        let captured_time = Arc::new(std::sync::Mutex::new(String::new()));
        let server_time = captured_time.clone();
        let router = Router::new().route(
            "/X",
            post(move |Query(query): Query<HashMap<String, String>>| {
                let time = server_time.clone();
                async move {
                    *time.lock().unwrap() = query.get("time").cloned().unwrap_or_default();
                    Json(json!({"code":0,"data":{}}))
                }
            }),
        );
        let base = test_server(router).await;
        let client = BiliClient::for_test(Client::new(), test_cookies(), &base);
        install_wbi(&client);
        let state = TraceState {
            room: LiveRoomInfo {
                room_id: 4,
                ruid: 9,
                parent_area_id: 1,
                area_id: 2,
                live_status: 1,
            },
            page_uuid: "uuid".into(),
            seq: 0,
            timestamp: 100,
            heartbeat_interval: Duration::ZERO,
            secret_key: "secret".into(),
            secret_rule: vec![2],
        };
        let next = client
            .trace_heartbeat(&CancellationToken::new(), &state)
            .await
            .unwrap();
        assert_eq!(&*captured_time.lock().unwrap(), "60");
        assert_eq!(next.timestamp, 160);
    }

    #[test]
    fn reward_validation_rejects_missing_fields_and_task_mismatch() {
        let info = RewardInfo {
            task_id: "cp".into(),
            task_name: "watch".into(),
            status: 0,
            message: String::new(),
            activity_id: String::new(),
            activity_name: String::new(),
            reward_name: String::new(),
        };
        assert!(validate_claimable_reward(&info).is_err());
        let payload = json!({"data":{"mid":496836187}});
        assert_eq!(uid_from_nav(&payload).unwrap(), "496836187");
    }
}
