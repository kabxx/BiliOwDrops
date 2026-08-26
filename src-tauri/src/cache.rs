use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context};

use crate::domain::{CacheFile, CachedAccountRecord, CachedAccountSummary, DiscoveryResult};

pub const CACHE_VERSION: u32 = 1;
#[cfg(not(target_os = "macos"))]
pub const CACHE_DIRECTORY: &str = "BiliOwDropsData";
pub const CACHE_FILENAME: &str = "accounts.json";
const ALLOWED_COOKIES: [&str; 8] = [
    "SESSDATA",
    "bili_jct",
    "DedeUserID",
    "DedeUserID__ckMd5",
    "buvid3",
    "b_nut",
    "sid",
    "LIVE_BUVID",
];

#[cfg(not(target_os = "macos"))]
pub fn cache_path_for_executable(executable: impl AsRef<Path>) -> PathBuf {
    executable
        .as_ref()
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(CACHE_DIRECTORY)
        .join(CACHE_FILENAME)
}

pub fn cache_path_for_current_executable() -> anyhow::Result<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .ok_or_else(|| anyhow!("无法定位 macOS 用户目录"))?;
        Ok(home
            .join("Library")
            .join("Application Support")
            .join("BiliOwDrops")
            .join(CACHE_FILENAME))
    }

    #[cfg(not(target_os = "macos"))]
    {
        Ok(cache_path_for_executable(
            std::env::current_exe().context("无法定位程序目录")?,
        ))
    }
}

#[derive(Clone, Debug)]
pub struct AccountCache {
    path: PathBuf,
    file: CacheFile,
}

impl AccountCache {
    pub fn load(path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let path = path.into();
        let file = match fs::read(&path) {
            Ok(bytes) => {
                anyhow::ensure!(bytes.len() <= 1 << 20, "账号缓存文件过大");
                serde_json::from_slice::<CacheFile>(&bytes).context("解析账号缓存失败")?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => CacheFile {
                version: CACHE_VERSION,
                accounts: vec![],
            },
            Err(error) => return Err(error).context("读取账号缓存失败"),
        };
        anyhow::ensure!(
            file.version == CACHE_VERSION,
            "不支持的账号缓存版本 {}",
            file.version
        );
        let mut cache = Self { path, file };
        cache.sanitize();
        Ok(cache)
    }

    pub fn summaries(&self) -> Vec<CachedAccountSummary> {
        self.file
            .accounts
            .iter()
            .map(|account| CachedAccountSummary {
                uid: account.uid.clone(),
                display_name: account.display_name.clone(),
                room_id: account.discovery.room_id,
                last_used_at: account.last_used_at.clone(),
            })
            .collect()
    }

    pub fn get(&self, uid: &str) -> Option<CachedAccountRecord> {
        self.file
            .accounts
            .iter()
            .find(|account| account.uid == uid)
            .cloned()
    }

    pub fn records(&self) -> Vec<CachedAccountRecord> {
        self.file.accounts.clone()
    }

    pub fn set_display_name(&mut self, uid: &str, display_name: &str) -> anyhow::Result<bool> {
        let Some(account) = self
            .file
            .accounts
            .iter_mut()
            .find(|account| account.uid == uid)
        else {
            return Ok(false);
        };
        let Some(display_name) = sanitize_display_name(display_name) else {
            return Ok(false);
        };
        if account.display_name.as_deref() == Some(display_name.as_str()) {
            return Ok(false);
        }
        account.display_name = Some(display_name);
        self.save()?;
        Ok(true)
    }

    pub fn upsert(&mut self, mut record: CachedAccountRecord) -> anyhow::Result<()> {
        sanitize_record(&mut record)?;
        if let Some(existing) = self
            .file
            .accounts
            .iter_mut()
            .find(|account| account.uid == record.uid)
        {
            *existing = record;
        } else {
            self.file.accounts.push(record);
        }
        self.sanitize();
        self.save()
    }

    pub fn touch_with_display_name(
        &mut self,
        uid: &str,
        now: String,
        display_name: Option<&str>,
    ) -> anyhow::Result<()> {
        chrono::DateTime::parse_from_rfc3339(&now).context("账号缓存时间无效")?;
        let account = self
            .file
            .accounts
            .iter_mut()
            .find(|account| account.uid == uid)
            .ok_or_else(|| anyhow!("账号缓存不存在"))?;
        account.last_used_at = now;
        if let Some(display_name) = display_name.and_then(sanitize_display_name) {
            account.display_name = Some(display_name);
        }
        self.save()
    }

    pub fn remove(&mut self, uid: &str) -> anyhow::Result<()> {
        self.file.accounts.retain(|account| account.uid != uid);
        self.save()
    }

    pub fn save(&self) -> anyhow::Result<()> {
        let parent = self
            .path
            .parent()
            .ok_or_else(|| anyhow!("账号缓存目录无效"))?;
        fs::create_dir_all(parent).context("创建账号缓存目录失败")?;
        let bytes = serde_json::to_vec_pretty(&self.file).context("编码账号缓存失败")?;
        let temp_name = format!(
            ".accounts-{}-{}.tmp",
            std::process::id(),
            uuid::Uuid::new_v4()
        );
        let temp_path = parent.join(temp_name);
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp_path)
            .context("创建账号缓存临时文件失败")?;
        restrict_permissions_best_effort(&file);
        let write_result = file
            .write_all(&bytes)
            .context("写入账号缓存失败")
            .and_then(|_| file.sync_all().context("同步账号缓存失败"));
        drop(file);
        if let Err(error) = write_result {
            let _ = fs::remove_file(&temp_path);
            return Err(error);
        }
        if let Err(error) = atomic_replace(&temp_path, &self.path).context("提交账号缓存失败")
        {
            let _ = fs::remove_file(&temp_path);
            return Err(error);
        }
        Ok(())
    }

    fn sanitize(&mut self) {
        self.file
            .accounts
            .retain_mut(|account| sanitize_record(account).is_ok());
        self.file
            .accounts
            .sort_by(|left, right| right.last_used_at.cmp(&left.last_used_at));
        let mut seen = std::collections::HashSet::new();
        self.file
            .accounts
            .retain(|account| seen.insert(account.uid.clone()));
    }
}

pub fn validate_record(record: &CachedAccountRecord) -> anyhow::Result<()> {
    let uid = record.uid.trim();
    anyhow::ensure!(
        !uid.is_empty() && uid.len() <= 20 && uid.chars().all(|c| c.is_ascii_digit()) && uid != "0",
        "账号 UID 无效"
    );
    anyhow::ensure!(
        !record.discovery.page_url.trim().is_empty(),
        "缓存活动地址为空"
    );
    anyhow::ensure!(
        !record.discovery.user_agent.trim().is_empty(),
        "缓存 User-Agent 为空"
    );
    let required = ["SESSDATA", "bili_jct", "DedeUserID"];
    for name in required {
        anyhow::ensure!(
            record
                .discovery
                .cookies
                .iter()
                .any(|cookie| cookie.name == name && !cookie.value.is_empty()),
            "缓存缺少 Cookie {name}"
        );
    }
    Ok(())
}

fn sanitize_record(record: &mut CachedAccountRecord) -> anyhow::Result<()> {
    record.uid = record.uid.trim().to_string();
    record.display_name = record
        .display_name
        .take()
        .and_then(|value| sanitize_display_name(&value));
    record.last_used_at = record.last_used_at.trim().to_string();
    chrono::DateTime::parse_from_rfc3339(&record.last_used_at).context("账号缓存时间无效")?;
    record.discovery.user_agent = record
        .discovery
        .user_agent
        .replace(['\r', '\n'], "")
        .trim()
        .to_string();
    if record.discovery.user_agent.len() > 512 {
        let mut end = 512;
        while !record.discovery.user_agent.is_char_boundary(end) {
            end -= 1;
        }
        record.discovery.user_agent.truncate(end);
    }
    record.discovery.page_url = record.discovery.page_url.trim().to_string();
    let page_url = url::Url::parse(&record.discovery.page_url).context("缓存活动地址无效")?;
    anyhow::ensure!(page_url.scheme() == "https", "缓存活动地址必须使用 HTTPS");
    anyhow::ensure!(
        page_url.host_str().is_some_and(is_bilibili_host),
        "缓存活动地址不是 B 站域名"
    );
    normalize_strings(&mut record.discovery.task_ids);
    normalize_strings(&mut record.discovery.observed_paths);
    let mut names = std::collections::HashSet::new();
    record.discovery.cookies.retain_mut(|cookie| {
        cookie.name = cookie.name.trim().to_string();
        cookie.domain = cookie.domain.trim().to_ascii_lowercase();
        cookie.path = cookie.path.trim().to_string();
        cookie.same_site = cookie.same_site.trim().to_string();
        ALLOWED_COOKIES.contains(&cookie.name.as_str())
            && !cookie.value.is_empty()
            && is_bilibili_host(cookie.domain.trim_start_matches('.'))
            && names.insert(cookie.name.clone())
    });
    validate_record(record)
}

fn sanitize_display_name(value: &str) -> Option<String> {
    let value = value
        .replace(['\r', '\n', '\t'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let mut value = value.trim().to_string();
    if value.is_empty() {
        return None;
    }
    if value.chars().count() > 80 {
        value = value.chars().take(80).collect();
    }
    Some(value)
}

fn is_bilibili_host(host: &str) -> bool {
    let host = host.trim().trim_end_matches('.').to_ascii_lowercase();
    host == "bilibili.com" || host.ends_with(".bilibili.com")
}

fn normalize_strings(values: &mut Vec<String>) {
    let mut seen = std::collections::HashSet::new();
    values.retain_mut(|value| {
        *value = value.trim().to_string();
        !value.is_empty() && seen.insert(value.clone())
    });
}

pub fn discovery_is_expired(discovery: &DiscoveryResult, now_unix: f64) -> bool {
    discovery.cookies.iter().any(|cookie| {
        ["SESSDATA", "bili_jct", "DedeUserID"].contains(&cookie.name.as_str())
            && cookie
                .expiration_date
                .is_some_and(|value| value > 0.0 && value <= now_unix)
    })
}

pub fn unix_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

#[cfg(windows)]
fn atomic_replace(source: &Path, destination: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    const MOVEFILE_REPLACE_EXISTING: u32 = 0x0000_0001;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn MoveFileExW(existing: *const u16, replacement: *const u16, flags: u32) -> i32;
    }
    let source = source
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    let destination = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect::<Vec<_>>();
    let result = unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn restrict_permissions_best_effort(file: &fs::File) {
    use std::os::unix::fs::PermissionsExt;
    let _ = file.set_permissions(fs::Permissions::from_mode(0o600));
}

#[cfg(not(unix))]
fn restrict_permissions_best_effort(_: &fs::File) {}

#[cfg(not(windows))]
fn atomic_replace(source: &Path, destination: &Path) -> std::io::Result<()> {
    if destination.exists() {
        fs::remove_file(destination)?;
    }
    fs::rename(source, destination)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Cookie, DiscoveryResult};
    fn record(uid: &str) -> CachedAccountRecord {
        CachedAccountRecord {
            uid: uid.into(),
            display_name: None,
            last_used_at: "2026-01-01T00:00:00Z".into(),
            discovery: DiscoveryResult {
                cookies: vec!["SESSDATA", "bili_jct", "DedeUserID"]
                    .into_iter()
                    .map(|name| Cookie {
                        name: name.into(),
                        value: "x".into(),
                        domain: ".bilibili.com".into(),
                        path: "/".into(),
                        secure: true,
                        http_only: true,
                        same_site: "lax".into(),
                        expiration_date: None,
                    })
                    .collect(),
                user_agent: "Mozilla/5.0 WebView".into(),
                page_url: "https://live.bilibili.com/1".into(),
                room_id: 1,
                task_ids: vec!["task".into()],
                observed_paths: vec![],
            },
        }
    }
    #[cfg(not(target_os = "macos"))]
    #[test]
    fn path_is_next_to_executable_and_plain_json_name_is_stable() {
        assert_eq!(
            cache_path_for_executable(r"C:\app\BiliOwDrops.exe"),
            PathBuf::from(r"C:\app\BiliOwDropsData\accounts.json")
        );
    }
    #[test]
    fn invalid_uid_and_missing_login_cookie_rejected() {
        assert!(validate_record(&record("abc")).is_err());
        let mut invalid = record("123");
        invalid
            .discovery
            .cookies
            .retain(|cookie| cookie.name != "SESSDATA");
        assert!(validate_record(&invalid).is_err());
    }
    #[test]
    fn tampered_domains_and_unknown_cookies_are_removed() {
        let mut value = record("123");
        value.discovery.cookies.push(Cookie {
            name: "SESSDATA".into(),
            value: "evil".into(),
            domain: ".example.com".into(),
            path: "/".into(),
            secure: true,
            http_only: true,
            same_site: "lax".into(),
            expiration_date: None,
        });
        value.discovery.cookies.push(Cookie {
            name: "UNEXPECTED".into(),
            value: "value".into(),
            domain: ".bilibili.com".into(),
            path: "/".into(),
            secure: true,
            http_only: true,
            same_site: "lax".into(),
            expiration_date: None,
        });
        sanitize_record(&mut value).unwrap();
        assert_eq!(value.discovery.cookies.len(), 3);
        assert!(value
            .discovery
            .cookies
            .iter()
            .all(|cookie| cookie.domain == ".bilibili.com"));
    }
    #[test]
    fn duplicate_uid_keeps_most_recent_record_on_load() {
        let root =
            std::env::temp_dir().join(format!("bili-cache-duplicate-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("accounts.json");
        let mut old = record("123");
        old.last_used_at = "2026-01-01T00:00:00Z".into();
        old.discovery.room_id = 1;
        let mut new = record("123");
        new.last_used_at = "2026-02-01T00:00:00Z".into();
        new.discovery.room_id = 2;
        fs::write(
            &path,
            serde_json::to_vec(&CacheFile {
                version: CACHE_VERSION,
                accounts: vec![old, new],
            })
            .unwrap(),
        )
        .unwrap();
        let cache = AccountCache::load(&path).unwrap();
        assert_eq!(cache.get("123").unwrap().discovery.room_id, 2);
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn cache_does_not_silently_evict_additional_accounts() {
        let root = std::env::temp_dir().join(format!("bili-cache-many-{}", uuid::Uuid::new_v4()));
        let path = root.join("accounts.json");
        let mut cache = AccountCache::load(&path).unwrap();
        for uid in 1..=20 {
            cache.upsert(record(&uid.to_string())).unwrap();
        }
        assert_eq!(AccountCache::load(&path).unwrap().summaries().len(), 20);
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn save_load_remove_is_atomic_enough_for_user_cache() {
        let root = std::env::temp_dir().join(format!("bili-cache-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let path = root.join("BiliOwDropsData").join("accounts.json");
        let mut cache = AccountCache::load(&path).unwrap();
        cache.upsert(record("123")).unwrap();
        assert!(AccountCache::load(&path).unwrap().get("123").is_some());
        cache.remove("123").unwrap();
        assert!(AccountCache::load(&path).unwrap().get("123").is_none());
        let _ = fs::remove_dir_all(root);
    }
}
