use std::collections::{HashMap, HashSet};

use chrono::Utc;
use serde_json::{Map, Value};

use crate::domain::{CheckpointState, TaskCheckpoint, TaskProgress};

#[cfg(test)]
pub fn parse_task_progress(payload: &Value) -> Vec<TaskProgress> {
    parse_task_progress_internal(payload, CheckpointScope::All)
}

pub fn parse_task_progress_with_allowed_by_task(
    payload: &Value,
    allowed_checkpoint_ids_by_task: &HashMap<String, HashSet<String>>,
) -> Vec<TaskProgress> {
    parse_task_progress_internal(
        payload,
        CheckpointScope::ByTask(allowed_checkpoint_ids_by_task),
    )
}

#[derive(Clone, Copy)]
enum CheckpointScope<'a> {
    #[cfg(test)]
    All,
    ByTask(&'a HashMap<String, HashSet<String>>),
}

impl<'a> CheckpointScope<'a> {
    fn allowed_for(self, task_id: &str) -> Option<&'a HashSet<String>> {
        match self {
            #[cfg(test)]
            Self::All => None,
            Self::ByTask(ids) => ids.get(task_id),
        }
    }

    fn is_by_task(self) -> bool {
        matches!(self, Self::ByTask(_))
    }
}

fn parse_task_progress_internal(payload: &Value, scope: CheckpointScope<'_>) -> Vec<TaskProgress> {
    let data = payload.get("data").and_then(Value::as_object);
    let list = data
        .and_then(|value| value.get("list").or_else(|| value.get("tasks")))
        .and_then(Value::as_array);
    let sampled_at = Utc::now().to_rfc3339();

    let mut tasks = list
        .into_iter()
        .flatten()
        .filter_map(Value::as_object)
        .filter_map(|item| {
            let id = string_value(first_value(item, &["task_id", "sid", "id"]));
            let id = id.trim().to_string();
            if id.is_empty() {
                return None;
            }
            let allowed_checkpoint_ids = scope.allowed_for(&id);
            if scope.is_by_task() && allowed_checkpoint_ids.is_none() {
                return None;
            }
            let (mut current, mut limit) = task_numbers(item);
            let has_explicit_limit =
                first_value(item, &["limit", "target", "total", "max", "max_value"]).is_some();
            let is_watch_duration = is_watch_duration_task(item);
            let raw_checkpoints = first_value(item, &["check_points", "accumulative_check_points"]);
            let mut checkpoints = if scope.is_by_task() && is_watch_duration {
                Vec::new()
            } else {
                parse_checkpoints(raw_checkpoints, allowed_checkpoint_ids)
            };
            checkpoints.sort_by(|left, right| {
                left.limit
                    .total_cmp(&right.limit)
                    .then_with(|| left.id.cmp(&right.id))
            });
            for (index, checkpoint) in checkpoints.iter_mut().enumerate() {
                checkpoint.key = format!("checkpoint-{}", index + 1);
            }
            if limit <= 0.0 && !is_watch_duration && !has_explicit_limit {
                if let Some(last) = checkpoints
                    .iter()
                    .max_by(|left, right| left.limit.total_cmp(&right.limit))
                {
                    current = last.current;
                    limit = last.limit;
                }
            }
            let raw_status = int_value(first_value(item, &["task_status", "status"]));
            Some(TaskProgress {
                id: id.clone(),
                task_key: "watch-progress".into(),
                name: nonempty_or(
                    &id,
                    string_value(first_value(item, &["task_name", "name", "title", "alias"])),
                ),
                current,
                limit,
                raw_status,
                sampled_at: sampled_at.clone(),
                checkpoints,
            })
        })
        .collect::<Vec<_>>();
    for (index, task) in tasks.iter_mut().enumerate() {
        task.task_key = format!("drop-{}", index + 1);
        if task.name == task.id {
            task.name = format!("掉宝 {}", index + 1);
        }
    }
    tasks
}

pub fn parse_checkpoints(
    raw: Option<&Value>,
    allowed_checkpoint_ids: Option<&HashSet<String>>,
) -> Vec<TaskCheckpoint> {
    let mut seen = HashSet::new();
    raw.and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_object)
        .filter_map(|item| {
            let id = string_value(first_value(
                item,
                &["sid", "task_id", "id", "checkpoint_id"],
            ));
            if id.is_empty()
                || allowed_checkpoint_ids.is_some_and(|allowed| !allowed.contains(&id))
                || !seen.insert(id.clone())
            {
                return None;
            }
            let raw_status = int_value(first_value(item, &["status", "task_status"]));
            let (current, limit) = checkpoint_numbers(item);
            Some(TaskCheckpoint {
                id: id.clone(),
                key: String::new(),
                name: nonempty_or(
                    &id,
                    string_value(first_value(item, &["alias", "task_name", "name", "title"])),
                ),
                current,
                limit,
                state: checkpoint_state(raw_status),
                raw_status,
            })
        })
        .collect()
}

pub fn task_numbers(item: &Map<String, Value>) -> (f64, f64) {
    let current = number_value(first_value(
        item,
        &["cur_value", "cur", "current", "progress"],
    ));
    let limit = number_value(first_value(
        item,
        &["limit", "target", "total", "max", "max_value"],
    ));
    if let Some(indicators) = item.get("indicators").and_then(Value::as_array) {
        let mut fallback = None;
        for indicator in indicators.iter().filter_map(Value::as_object) {
            let name = string_value(first_value(
                indicator,
                &["name", "key", "type", "indicator"],
            ))
            .to_lowercase();
            let candidate = (
                number_value(first_value(
                    indicator,
                    &["cur_value", "cur", "current", "progress", "value"],
                )),
                number_value(first_value(
                    indicator,
                    &["limit", "target", "total", "max", "max_value"],
                )),
            );
            if is_watch_indicator(&name) {
                return candidate;
            }
            fallback.get_or_insert(candidate);
        }
        if current <= 0.0 && limit <= 0.0 {
            if let Some(fallback) = fallback {
                return fallback;
            }
        }
    }
    (current, limit)
}

pub fn checkpoint_numbers(item: &Map<String, Value>) -> (f64, f64) {
    let direct = task_numbers(item);
    if direct.1 > 0.0 {
        return direct;
    }
    if let Some(list) = item.get("list").and_then(Value::as_array) {
        let mut nested = Map::new();
        nested.insert("indicators".into(), Value::Array(list.clone()));
        return task_numbers(&nested);
    }
    direct
}

pub fn is_watch_indicator(value: &str) -> bool {
    value == "watch_time"
        || value == "view_time"
        || value.contains("watch")
        || value.contains("观看")
}

fn is_watch_duration_task(item: &Map<String, Value>) -> bool {
    ["task_name", "taskName", "name", "title", "alias"]
        .iter()
        .map(|key| string_value(item.get(*key)).to_lowercase())
        .any(|value| {
            value.contains("观看时长")
                || value.contains("watch_time")
                || value.contains("view_time")
        })
}

pub fn checkpoint_state(raw_status: i32) -> CheckpointState {
    match raw_status {
        2 => CheckpointState::Claimable,
        3 | 6 => CheckpointState::Claimed,
        _ => CheckpointState::Pending,
    }
}

pub(crate) fn first_value<'a>(values: &'a Map<String, Value>, keys: &[&str]) -> Option<&'a Value> {
    keys.iter()
        .find_map(|key| values.get(*key).filter(|value| !value.is_null()))
}

pub(crate) fn string_value(value: Option<&Value>) -> String {
    match value {
        Some(Value::String(value)) => value.clone(),
        Some(Value::Number(value)) => value.to_string(),
        Some(Value::Bool(value)) => value.to_string(),
        _ => String::new(),
    }
}

pub(crate) fn int_value(value: Option<&Value>) -> i32 {
    strict_i64(value).unwrap_or_default() as i32
}

pub(crate) fn strict_i64(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(value) => value.as_i64().or_else(|| {
            let float = value.as_f64()?;
            (float.fract() == 0.0).then_some(float as i64)
        }),
        Value::String(value) => value.trim().parse().ok(),
        _ => None,
    }
}

pub(crate) fn number_value(value: Option<&Value>) -> f64 {
    match value {
        Some(Value::Number(value)) => value.as_f64().unwrap_or_default(),
        Some(Value::String(value)) => value.trim().parse().unwrap_or_default(),
        _ => 0.0,
    }
}

fn nonempty_or(fallback: &str, value: String) -> String {
    if value.trim().is_empty() {
        fallback.to_string()
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_dynamic_limits_and_nested_checkpoint_indicators() {
        let payload = json!({"data":{"list":[{
            "task_id":"parent","indicators":[
                {"type":"share","current":1,"target":1},
                {"type":"watch_time","current":75,"target":240}
            ],"check_points":[
                {"sid":"cp-90","status":2,"list":[{"type":"watch_time","cur_value":75,"limit":90}]},
                {"sid":"cp-240","status":0,"cur_value":75,"limit":240}
            ]
        }]}});
        let parsed = parse_task_progress(&payload);
        assert_eq!((parsed[0].current, parsed[0].limit), (75.0, 240.0));
        assert_eq!(parsed[0].checkpoints.len(), 2);
        assert_eq!(parsed[0].checkpoints[0].limit, 90.0);
        assert_eq!(parsed[0].checkpoints[0].state, CheckpointState::Claimable);
    }

    #[test]
    fn malformed_values_do_not_become_claimed() {
        let payload = json!({"data":{"list":[{"task_id":"x","status":"bad","limit":"bad","check_points":[{"sid":"cp","limit":60,"status":"bad"}]}]}});
        let parsed = parse_task_progress(&payload);
        assert_eq!(parsed[0].raw_status, 0);
        assert_eq!(parsed[0].limit, 0.0);
        assert_ne!(parsed[0].raw_status, 3);
    }
}
