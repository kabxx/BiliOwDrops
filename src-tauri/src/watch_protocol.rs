use anyhow::{bail, Context};
use hmac::{Hmac, Mac};
use md5::Md5;
use serde::Serialize;
use serde_json::Value;
use sha1::Sha1;
use sha2::{Sha224, Sha256, Sha384, Sha512};

#[derive(Serialize)]
pub(crate) struct X25knSignaturePayload<'a> {
    pub platform: &'a str,
    pub parent_id: u64,
    pub area_id: u64,
    pub seq_id: i64,
    pub room_id: u64,
    pub buvid: String,
    pub uuid: &'a str,
    pub ets: i64,
    pub time: i64,
    pub ts: i64,
}

pub fn build_x25kn_signature(
    payload: &impl Serialize,
    secret_key: &str,
    rules: &[i32],
) -> anyhow::Result<String> {
    let mut current = serde_json::to_vec(payload)?;
    for rule in rules {
        macro_rules! sign {
            ($digest:ty) => {{
                let mut mac = Hmac::<$digest>::new_from_slice(secret_key.as_bytes())
                    .map_err(|_| anyhow::anyhow!("无效 HMAC 密钥"))?;
                mac.update(&current);
                hex::encode(mac.finalize().into_bytes())
            }};
        }
        let encoded = match rule {
            0 => sign!(Md5),
            1 => sign!(Sha1),
            2 => sign!(Sha256),
            3 => sign!(Sha224),
            4 => sign!(Sha512),
            5 => sign!(Sha384),
            other => bail!("不支持的 x25Kn secret_rule={other}"),
        };
        current = encoded.into_bytes();
    }
    String::from_utf8(current).context("x25Kn signature was not UTF-8")
}

pub fn strict_secret_rules(value: Option<&Value>) -> anyhow::Result<Vec<i32>> {
    let items = value
        .and_then(Value::as_array)
        .context("规则列表不是数组")?;
    items
        .iter()
        .map(|item| {
            let rule = match item {
                Value::Number(value) => value.as_i64().or_else(|| {
                    value
                        .as_f64()
                        .filter(|value| value.fract() == 0.0)
                        .map(|value| value as i64)
                }),
                Value::String(value) => value.trim().parse().ok(),
                _ => None,
            }
            .context("规则值不是整数")? as i32;
            if !(0..=5).contains(&rule) {
                bail!("规则值 {rule} 超出 0..5");
            }
            Ok(rule)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn signature_matches_independent_go_python_vector() {
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
    fn rules_are_strict_integers_between_zero_and_five() {
        assert_eq!(
            strict_secret_rules(Some(&json!([0, "5", 2]))).unwrap(),
            vec![0, 5, 2]
        );
        for malformed in [json!([1.5]), json!([6]), json!([null]), json!("bad")] {
            assert!(strict_secret_rules(Some(&malformed)).is_err());
        }
    }
}
