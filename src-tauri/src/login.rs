use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use tauri::{
    webview::{Cookie as TauriCookie, PageLoadEvent},
    AppHandle, Manager, PhysicalRect, PhysicalSize, WebviewUrl, WebviewWindow,
    WebviewWindowBuilder, WindowEvent, Wry,
};
use tokio::{
    sync::{oneshot, Notify},
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::domain::{Cookie, DiscoveryResult};

#[cfg(not(target_os = "macos"))]
use crate::bilibili::BILIBILI_USER_AGENT;

const LOGIN_WINDOW_LABEL: &str = "bilibili-login";
const LOGIN_URL: &str = "https://passport.bilibili.com/login";
const COOKIE_POLL_INTERVAL: Duration = Duration::from_millis(800);
const PAGE_LOAD_TIMEOUT: Duration = Duration::from_secs(10);
const USER_AGENT_TIMEOUT: Duration = Duration::from_secs(5);
const WINDOW_LAYOUT_TIMEOUT: Duration = Duration::from_secs(3);
const WINDOW_LAYOUT_TOLERANCE: i32 = 2;
const ALLOWED_COOKIE_NAMES: [&str; 8] = [
    "SESSDATA",
    "bili_jct",
    "DedeUserID",
    "DedeUserID__ckMd5",
    "buvid3",
    "b_nut",
    "sid",
    "LIVE_BUVID",
];
const REQUIRED_COOKIE_NAMES: [&str; 3] = ["SESSDATA", "bili_jct", "DedeUserID"];

struct PageLoadState {
    finished: AtomicBool,
    notify: Notify,
}

impl PageLoadState {
    fn mark_finished(&self) {
        self.finished.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }
}

pub async fn capture_login(
    app: &AppHandle,
    cancel: &CancellationToken,
) -> anyhow::Result<Option<DiscoveryResult>> {
    if cancel.is_cancelled() {
        return Ok(None);
    }

    if let Some(existing) = app.get_webview_window(LOGIN_WINDOW_LABEL) {
        let _ = existing.set_focus();
        bail!("B 站登录窗口已经打开");
    }

    let page_load = Arc::new(PageLoadState {
        finished: AtomicBool::new(false),
        notify: Notify::new(),
    });
    let login_user_agent = login_user_agent()?;
    let work_area = login_work_area(app)?;
    let page_load_for_event = Arc::clone(&page_load);
    let builder = WebviewWindowBuilder::new(
        app,
        LOGIN_WINDOW_LABEL,
        WebviewUrl::External(Url::parse(LOGIN_URL)?),
    )
    .on_page_load(move |_window, payload| {
        if payload.event() == PageLoadEvent::Finished {
            page_load_for_event.mark_finished();
        }
    })
    .title("登录 Bilibili")
    .resizable(true)
    .maximizable(true)
    .visible(false)
    .focused(true)
    .incognito(true);
    #[cfg(target_os = "macos")]
    let builder = builder.user_agent(&login_user_agent);
    let window = builder.build().context("打开 B 站登录窗口失败")?;

    let closed = Arc::new(AtomicBool::new(false));
    let closed_for_event = Arc::clone(&closed);
    window.on_window_event(move |event| {
        if matches!(
            event,
            WindowEvent::CloseRequested { .. } | WindowEvent::Destroyed
        ) {
            closed_for_event.store(true, Ordering::Release);
        }
    });

    let result = async {
        show_login_window(&window, work_area).await?;
        capture_from_window(app, &window, &closed, &page_load, &login_user_agent, cancel).await
    }
    .await;
    cleanup_window(&window);
    result
}

async fn capture_from_window(
    app: &AppHandle,
    window: &WebviewWindow<Wry>,
    closed: &AtomicBool,
    page_load: &PageLoadState,
    fallback_user_agent: &str,
    cancel: &CancellationToken,
) -> anyhow::Result<Option<DiscoveryResult>> {
    // A slow page-load event must not prevent the user from completing login.
    // If the callback is not delivered in time, the platform-specific fallback
    // UA is still valid for the subsequent HTTP session.
    let page_loaded = wait_for_page_load(page_load, closed, cancel).await?;
    let user_agent = if page_loaded {
        read_user_agent(window, fallback_user_agent)
            .await
            .unwrap_or_else(|_| fallback_user_agent.to_string())
    } else {
        fallback_user_agent.to_string()
    };

    loop {
        if cancel.is_cancelled() || closed.load(Ordering::Acquire) {
            return Ok(None);
        }
        if app.get_webview_window(LOGIN_WINDOW_LABEL).is_none() {
            return Ok(None);
        }

        // WebView cookie stores can briefly reject a read while a QR redirect
        // is committing. Treat that as a transient sample and keep polling.
        let Ok(cookies) = read_login_cookies(window) else {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(None),
                _ = sleep(COOKIE_POLL_INTERVAL) => {}
            }
            continue;
        };
        if has_required_cookies(&cookies) {
            return Ok(Some(DiscoveryResult {
                cookies,
                user_agent: user_agent.clone(),
                page_url: "https://www.bilibili.com/".into(),
                room_id: 0,
                task_ids: vec![],
                observed_paths: vec![],
            }));
        }

        tokio::select! {
            _ = cancel.cancelled() => return Ok(None),
            _ = sleep(COOKIE_POLL_INTERVAL) => {}
        }
    }
}

async fn wait_for_page_load(
    page_load: &PageLoadState,
    closed: &AtomicBool,
    cancel: &CancellationToken,
) -> anyhow::Result<bool> {
    let deadline = Instant::now() + PAGE_LOAD_TIMEOUT;
    loop {
        if page_load.is_finished() {
            return Ok(true);
        }
        if closed.load(Ordering::Acquire) || cancel.is_cancelled() {
            return Ok(false);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            // Loading is only needed to obtain the best UA. The login window
            // can still complete auth through its cookie store if this event
            // is delayed by the WebView implementation.
            return Ok(false);
        }

        let notified = page_load.notify.notified();
        if page_load.is_finished() {
            return Ok(true);
        }
        tokio::select! {
            _ = notified => {}
            _ = cancel.cancelled() => return Ok(false),
            _ = sleep(remaining.min(Duration::from_millis(250))) => {}
        }
    }
}

async fn read_user_agent(
    window: &WebviewWindow<Wry>,
    fallback_user_agent: &str,
) -> anyhow::Result<String> {
    let (sender, receiver) = oneshot::channel();
    let sender = Arc::new(std::sync::Mutex::new(Some(sender)));
    let sender_for_callback = Arc::clone(&sender);
    window
        .eval_with_callback("JSON.stringify(navigator.userAgent)", move |serialized| {
            let value = serde_json::from_str::<String>(&serialized)
                .unwrap_or_else(|_| serialized.trim_matches('"').to_string());
            if let Ok(mut sender) = sender_for_callback.lock() {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(value);
                }
            }
        })
        .context("读取登录窗口 User-Agent 失败")?;
    let value = tokio::time::timeout(USER_AGENT_TIMEOUT, receiver)
        .await
        .context("读取登录窗口 User-Agent 超时")?
        .context("登录窗口 User-Agent 回调已关闭")?;
    let value = value.replace(['\r', '\n'], " ").trim().to_string();
    if value.is_empty() {
        Ok(fallback_user_agent.into())
    } else {
        Ok(value)
    }
}

fn read_login_cookies(window: &WebviewWindow<Wry>) -> anyhow::Result<Vec<Cookie>> {
    let mut selected = HashMap::<String, Cookie>::new();
    for cookie in window.cookies().context("读取 B 站登录窗口 Cookie 失败")? {
        let Some(converted) = convert_cookie(&cookie) else {
            continue;
        };
        let replace = selected
            .get(&converted.name)
            .is_none_or(|current| cookie_priority(&converted) > cookie_priority(current));
        if replace {
            selected.insert(converted.name.clone(), converted);
        }
    }
    let mut cookies: Vec<_> = selected.into_values().collect();
    cookies.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(cookies)
}

fn convert_cookie(raw: &TauriCookie<'static>) -> Option<Cookie> {
    let name = raw.name().trim();
    if !ALLOWED_COOKIE_NAMES.contains(&name) {
        return None;
    }
    let domain = raw
        .domain()
        .unwrap_or("bilibili.com")
        .trim()
        .to_ascii_lowercase();
    if !is_bilibili_cookie_domain(&domain) {
        return None;
    }
    let value = raw.value();
    if value.is_empty() {
        return None;
    }
    Some(Cookie {
        name: name.into(),
        value: value.into(),
        domain,
        path: raw.path().unwrap_or("/").into(),
        secure: raw.secure().unwrap_or(false),
        http_only: raw.http_only().unwrap_or(false),
        same_site: raw
            .same_site()
            .map(|value| format!("{value:?}"))
            .unwrap_or_default(),
        expiration_date: raw
            .expires_datetime()
            .map(|value| value.unix_timestamp() as f64),
    })
}

fn is_bilibili_cookie_domain(domain: &str) -> bool {
    let host = domain.trim().trim_start_matches('.');
    host.eq_ignore_ascii_case("bilibili.com")
        || host.to_ascii_lowercase().ends_with(".bilibili.com")
}

fn cookie_priority(cookie: &Cookie) -> (u8, u8, i64) {
    (
        u8::from(cookie.domain == "bilibili.com"),
        u8::from(cookie.path == "/"),
        cookie.expiration_date.unwrap_or_default() as i64,
    )
}

fn has_required_cookies(cookies: &[Cookie]) -> bool {
    REQUIRED_COOKIE_NAMES.iter().all(|required| {
        cookies
            .iter()
            .any(|cookie| cookie.name == *required && !cookie.value.trim().is_empty())
    })
}

fn cleanup_window(window: &WebviewWindow<Wry>) {
    let _ = window.clear_all_browsing_data();
    let _ = window.destroy();
}

#[cfg(not(target_os = "macos"))]
fn login_user_agent() -> anyhow::Result<String> {
    Ok(BILIBILI_USER_AGENT.into())
}

#[cfg(target_os = "macos")]
fn login_user_agent() -> anyhow::Result<String> {
    const SAFARI_INFO: &str = "/Applications/Safari.app/Contents/Info.plist";
    let info = plist::Value::from_file(SAFARI_INFO).context("读取 Safari 版本信息失败")?;
    let version = info
        .as_dictionary()
        .and_then(|values| values.get("CFBundleShortVersionString"))
        .and_then(plist::Value::as_string)
        .map(str::trim)
        .filter(|version| {
            !version.is_empty()
                && version
                    .chars()
                    .all(|character| character.is_ascii_digit() || character == '.')
        })
        .context("Safari 版本信息无效")?;
    Ok(safari_user_agent(version))
}

#[cfg(target_os = "macos")]
fn safari_user_agent(version: &str) -> String {
    format!(
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 \
         (KHTML, like Gecko) Version/{version} Safari/605.1.15"
    )
}

fn login_work_area(app: &AppHandle) -> anyhow::Result<PhysicalRect<i32, u32>> {
    let main_monitor = app
        .get_webview_window("main")
        .and_then(|window| window.current_monitor().ok().flatten());
    let monitor = match main_monitor {
        Some(monitor) => monitor,
        None => app
            .primary_monitor()
            .context("读取主显示器失败")?
            .context("没有可用的显示器")?,
    };
    Ok(*monitor.work_area())
}

async fn show_login_window(
    window: &WebviewWindow<Wry>,
    work_area: PhysicalRect<i32, u32>,
) -> anyhow::Result<()> {
    let initial_outer = window.outer_size().context("读取 B 站登录窗口外框失败")?;
    let initial_inner = window
        .inner_size()
        .context("读取 B 站登录窗口内容尺寸失败")?;
    let frame_width = initial_outer.width.saturating_sub(initial_inner.width);
    let frame_height = initial_outer.height.saturating_sub(initial_inner.height);
    let target_inner = PhysicalSize::new(
        work_area.size.width.saturating_sub(frame_width).max(1),
        work_area.size.height.saturating_sub(frame_height).max(1),
    );

    window
        .set_size(target_inner)
        .context("设置 B 站登录窗口尺寸失败")?;
    window
        .set_position(work_area.position)
        .context("设置 B 站登录窗口位置失败")?;

    let deadline = Instant::now() + WINDOW_LAYOUT_TIMEOUT;
    loop {
        let position = window
            .outer_position()
            .context("读取 B 站登录窗口位置失败")?;
        let size = window.outer_size().context("读取 B 站登录窗口尺寸失败")?;
        if layout_matches_work_area(position, size, work_area) {
            break;
        }
        if Instant::now() >= deadline {
            bail!("B 站登录窗口布局超时，请重试");
        }
        sleep(Duration::from_millis(16)).await;
    }

    window.show().context("显示 B 站登录窗口失败")?;
    window.set_focus().context("聚焦 B 站登录窗口失败")?;
    Ok(())
}

fn layout_matches_work_area(
    position: tauri::PhysicalPosition<i32>,
    size: PhysicalSize<u32>,
    work_area: PhysicalRect<i32, u32>,
) -> bool {
    within_layout_tolerance(position.x, work_area.position.x)
        && within_layout_tolerance(position.y, work_area.position.y)
        && within_layout_tolerance(size.width as i64, work_area.size.width as i64)
        && within_layout_tolerance(size.height as i64, work_area.size.height as i64)
}

fn within_layout_tolerance<T>(value: T, expected: T) -> bool
where
    T: Into<i64> + Copy,
{
    (value.into() - expected.into()).abs() <= i64::from(WINDOW_LAYOUT_TOLERANCE)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cookie(name: &str, value: &str) -> Cookie {
        Cookie {
            name: name.into(),
            value: value.into(),
            domain: "bilibili.com".into(),
            path: "/".into(),
            secure: true,
            http_only: true,
            same_site: "Lax".into(),
            expiration_date: None,
        }
    }

    #[test]
    fn required_cookie_check_needs_all_login_values() {
        assert!(!has_required_cookies(&[cookie("SESSDATA", "x")]));
        assert!(has_required_cookies(&[
            cookie("SESSDATA", "x"),
            cookie("bili_jct", "x"),
            cookie("DedeUserID", "1"),
        ]));
    }

    #[test]
    fn cookie_filter_rejects_non_bilibili_and_unknown_names() {
        let raw = cookie("SESSDATA", "x");
        assert_eq!(cookie_priority(&raw), (1, 1, 0));
        assert!(!ALLOWED_COOKIE_NAMES.contains(&"unknown"));
    }

    #[test]
    fn parent_domain_login_cookie_is_accepted() {
        assert!(is_bilibili_cookie_domain(".bilibili.com"));
        assert!(is_bilibili_cookie_domain("passport.bilibili.com"));
        assert!(!is_bilibili_cookie_domain("bilibili.com.example.org"));

        let raw = TauriCookie::build(("SESSDATA", "session"))
            .domain(".bilibili.com")
            .path("/")
            .secure(true)
            .http_only(true)
            .build();
        let converted = convert_cookie(&raw).expect("parent-domain login cookie must be kept");
        assert_eq!(converted.domain, "bilibili.com");
        assert_eq!(converted.name, "SESSDATA");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_login_user_agent_identifies_the_installed_safari() {
        let user_agent = login_user_agent().unwrap();
        assert!(user_agent.contains("Macintosh"));
        assert!(user_agent.contains(" Version/"));
        assert!(user_agent.ends_with(" Safari/605.1.15"));
        assert!(!user_agent.contains("Windows"));
    }
}
