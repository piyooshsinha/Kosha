//! IAM-style policies: parsing, request classification and evaluation.
//!
//! Supported: `Effect`, `Principal` (bucket policies), `Action`/`NotAction`,
//! `Resource`/`NotResource` with `*`/`?` wildcards and `${aws:username}`
//! variables, and `Condition` with the string, numeric, IP, `Bool` and
//! `Null` operators (plus `...IfExists`).
//!
//! Evaluation follows AWS: an explicit Deny anywhere wins, otherwise an
//! Allow from the identity policy or the bucket policy grants access,
//! otherwise access is denied.

use std::collections::HashMap;
use std::net::IpAddr;

use axum::http::{HeaderMap, Method};
use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Effect {
    Allow,
    Deny,
}

#[derive(Clone, Debug)]
enum Principals {
    Any,
    List(Vec<String>),
}

#[derive(Clone, Debug)]
struct Matcher {
    patterns: Vec<String>,
    negated: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    StringEquals,
    StringNotEquals,
    StringEqualsIgnoreCase,
    StringNotEqualsIgnoreCase,
    StringLike,
    StringNotLike,
    NumericEquals,
    NumericNotEquals,
    NumericLessThan,
    NumericLessThanEquals,
    NumericGreaterThan,
    NumericGreaterThanEquals,
    IpAddress,
    NotIpAddress,
    Bool,
    Null,
}

#[derive(Clone, Debug)]
struct Condition {
    op: Op,
    if_exists: bool,
    /// Lower-cased condition key, e.g. `aws:sourceip`.
    key: String,
    values: Vec<String>,
}

#[derive(Clone, Debug)]
struct Statement {
    effect: Effect,
    principal: Option<Principals>,
    actions: Matcher,
    resources: Matcher,
    conditions: Vec<Condition>,
}

#[derive(Clone, Debug)]
pub struct Policy {
    statements: Vec<Statement>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Deny,
    /// No statement applies.
    Implicit,
}

/// What kind of document is being parsed.
#[derive(Clone, Copy, Debug)]
pub enum PolicyKind<'a> {
    /// A bucket policy: statements need a `Principal` and may only name
    /// that bucket's resources.
    Bucket(&'a str),
    /// A user's identity policy: no `Principal`.
    Identity,
}

/// Who is making the request.
#[derive(Clone, Debug)]
pub enum Principal {
    Anonymous,
    User { name: String, access_key: String },
}

/// Facts about a request that conditions and variables can refer to.
#[derive(Clone, Debug)]
pub struct RequestFacts {
    pub principal: Principal,
    /// Lower-cased condition key -> value.
    pub values: HashMap<String, String>,
}

impl RequestFacts {
    pub fn new(principal: Principal) -> Self {
        let mut values = HashMap::new();
        match &principal {
            Principal::Anonymous => {
                values.insert("aws:principaltype".into(), "Anonymous".into());
            }
            Principal::User { name, access_key } => {
                values.insert("aws:principaltype".into(), "User".into());
                values.insert("aws:username".into(), name.clone());
                values.insert("aws:userid".into(), access_key.clone());
            }
        }
        RequestFacts { principal, values }
    }

    pub fn set(&mut self, key: &str, value: impl Into<String>) {
        self.values.insert(key.to_ascii_lowercase(), value.into());
    }

    fn get(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }

    /// Replace `${aws:username}`-style variables. Unknown variables become
    /// a value that matches nothing.
    fn expand(&self, s: &str) -> String {
        if !s.contains("${") {
            return s.to_string();
        }
        let mut out = String::new();
        let mut rest = s;
        while let Some(i) = rest.find("${") {
            out.push_str(&rest[..i]);
            let after = &rest[i + 2..];
            let Some(j) = after.find('}') else {
                out.push_str(&rest[i..]);
                return out;
            };
            let var = &after[..j];
            match var {
                "*" | "?" | "$" => out.push_str(var),
                _ => match self.get(&var.to_ascii_lowercase()) {
                    Some(v) => out.push_str(v),
                    None => out.push('\u{0}'),
                },
            }
            rest = &after[j + 1..];
        }
        out.push_str(rest);
        out
    }
}

/// `*` matches any run of characters, `?` any one character.
pub fn glob(pattern: &str, text: &str, ignore_case: bool) -> bool {
    let eq = |a: char, b: char| {
        if ignore_case {
            a.eq_ignore_ascii_case(&b)
        } else {
            a == b
        }
    };
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0, 0);
    let (mut star, mut mark) = (None, 0);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || (p[pi] != '*' && eq(p[pi], t[ti]))) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    p[pi..].iter().all(|&c| c == '*')
}

fn strings(v: &Value, what: &str) -> Result<Vec<String>, String> {
    let one = |v: &Value| match v {
        Value::String(s) => Ok(s.clone()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Number(n) => Ok(n.to_string()),
        _ => Err(format!("{what} must be a string or a list of strings")),
    };
    match v {
        Value::Array(a) if a.is_empty() => Err(format!("{what} must not be empty")),
        Value::Array(a) => a.iter().map(one).collect(),
        v => Ok(vec![one(v)?]),
    }
}

fn parse_op(name: &str) -> Result<(Op, bool), String> {
    // Our condition keys are single-valued, so the set qualifiers reduce
    // to the plain operator.
    let name = name
        .strip_prefix("ForAnyValue:")
        .or_else(|| name.strip_prefix("ForAllValues:"))
        .unwrap_or(name);
    let (base, if_exists) = match name.strip_suffix("IfExists") {
        Some(b) if b != "Null" => (b, true),
        _ => (name, false),
    };
    let op = match base {
        "StringEquals" => Op::StringEquals,
        "StringNotEquals" => Op::StringNotEquals,
        "StringEqualsIgnoreCase" => Op::StringEqualsIgnoreCase,
        "StringNotEqualsIgnoreCase" => Op::StringNotEqualsIgnoreCase,
        "StringLike" => Op::StringLike,
        "StringNotLike" => Op::StringNotLike,
        "NumericEquals" => Op::NumericEquals,
        "NumericNotEquals" => Op::NumericNotEquals,
        "NumericLessThan" => Op::NumericLessThan,
        "NumericLessThanEquals" => Op::NumericLessThanEquals,
        "NumericGreaterThan" => Op::NumericGreaterThan,
        "NumericGreaterThanEquals" => Op::NumericGreaterThanEquals,
        "IpAddress" => Op::IpAddress,
        "NotIpAddress" => Op::NotIpAddress,
        "Bool" => Op::Bool,
        "Null" => Op::Null,
        _ => return Err(format!("unsupported condition operator {name}")),
    };
    Ok((op, if_exists))
}

fn parse_cidr(s: &str) -> Option<(IpAddr, u8)> {
    let (addr, bits) = match s.split_once('/') {
        Some((a, b)) => (a, Some(b.parse::<u8>().ok()?)),
        None => (s, None),
    };
    let ip: IpAddr = addr.parse().ok()?;
    let max = if ip.is_ipv4() { 32 } else { 128 };
    let bits = bits.unwrap_or(max);
    (bits <= max).then_some((ip, bits))
}

fn in_cidr(ip: IpAddr, net: (IpAddr, u8)) -> bool {
    let ip = match (ip, net.0) {
        (IpAddr::V6(v6), IpAddr::V4(_)) => v6.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(ip),
        _ => ip,
    };
    match (ip, net.0) {
        (IpAddr::V4(a), IpAddr::V4(b)) => {
            let mask = u32::MAX.checked_shl(32 - net.1 as u32).unwrap_or(0);
            u32::from(a) & mask == u32::from(b) & mask
        }
        (IpAddr::V6(a), IpAddr::V6(b)) => {
            let mask = u128::MAX.checked_shl(128 - net.1 as u32).unwrap_or(0);
            u128::from(a) & mask == u128::from(b) & mask
        }
        _ => false,
    }
}

impl Policy {
    pub fn parse(json: &str, kind: PolicyKind<'_>) -> Result<Policy, String> {
        let doc: Value =
            serde_json::from_str(json).map_err(|e| format!("policy is not valid JSON: {e}"))?;
        let obj = doc.as_object().ok_or("policy must be a JSON object")?;
        if let Some(v) = obj.get("Version")
            && !matches!(v.as_str(), Some("2012-10-17" | "2008-10-17"))
        {
            return Err("Version must be 2012-10-17".into());
        }
        let stmts = match obj.get("Statement") {
            Some(Value::Array(a)) if !a.is_empty() => a.clone(),
            Some(v @ Value::Object(_)) => vec![v.clone()],
            _ => return Err("policy needs at least one Statement".into()),
        };
        let statements = stmts
            .iter()
            .map(|s| Self::parse_statement(s, kind))
            .collect::<Result<_, _>>()?;
        Ok(Policy { statements })
    }

    fn parse_statement(v: &Value, kind: PolicyKind<'_>) -> Result<Statement, String> {
        let o = v.as_object().ok_or("each Statement must be an object")?;
        let effect = match o.get("Effect").and_then(Value::as_str) {
            Some("Allow") => Effect::Allow,
            Some("Deny") => Effect::Deny,
            _ => return Err("Effect must be Allow or Deny".into()),
        };
        if o.contains_key("NotPrincipal") {
            return Err("NotPrincipal is not supported".into());
        }
        let principal = match (kind, o.get("Principal")) {
            (PolicyKind::Identity, Some(_)) => {
                return Err("identity policies cannot have a Principal".into());
            }
            (PolicyKind::Identity, None) => None,
            (PolicyKind::Bucket(_), None) => {
                return Err("bucket policy statements need a Principal".into());
            }
            (PolicyKind::Bucket(_), Some(Value::String(s))) if s == "*" => Some(Principals::Any),
            (PolicyKind::Bucket(_), Some(Value::Object(m))) => {
                let aws = m.get("AWS").ok_or("only AWS principals are supported")?;
                let list = strings(aws, "Principal")?;
                if list.iter().any(|p| p == "*") {
                    Some(Principals::Any)
                } else {
                    Some(Principals::List(list))
                }
            }
            _ => return Err("Principal must be \"*\" or {\"AWS\": ...}".into()),
        };
        let matcher = |pos: &str, neg: &str| -> Result<Matcher, String> {
            match (o.get(pos), o.get(neg)) {
                (Some(v), None) => Ok(Matcher {
                    patterns: strings(v, pos)?,
                    negated: false,
                }),
                (None, Some(v)) => Ok(Matcher {
                    patterns: strings(v, neg)?,
                    negated: true,
                }),
                _ => Err(format!("a statement needs exactly one of {pos} and {neg}")),
            }
        };
        let actions = matcher("Action", "NotAction")?;
        for a in &actions.patterns {
            if a != "*" && !a.to_ascii_lowercase().starts_with("s3:") {
                return Err(format!("unsupported action {a}: only s3:* actions exist"));
            }
        }
        let resources = matcher("Resource", "NotResource")?;
        for r in &resources.patterns {
            if r == "*" {
                continue;
            }
            let Some(rest) = r.strip_prefix("arn:aws:s3:::") else {
                return Err(format!(
                    "invalid resource {r}: expected arn:aws:s3:::bucket[/key]"
                ));
            };
            if let PolicyKind::Bucket(bucket) = kind {
                let b = rest.split('/').next().unwrap_or_default();
                if !glob(b, bucket, false) {
                    return Err(format!(
                        "Policy has invalid resource {r}: it must refer to bucket {bucket}"
                    ));
                }
            }
        }
        let mut conditions = Vec::new();
        if let Some(c) = o.get("Condition") {
            let c = c.as_object().ok_or("Condition must be an object")?;
            for (op_name, block) in c {
                let (op, if_exists) = parse_op(op_name)?;
                let block = block
                    .as_object()
                    .ok_or_else(|| format!("{op_name} must map keys to values"))?;
                for (key, values) in block {
                    let values = strings(values, op_name)?;
                    if matches!(op, Op::IpAddress | Op::NotIpAddress)
                        && values.iter().any(|v| parse_cidr(v).is_none())
                    {
                        return Err(format!("invalid IP address or CIDR in {op_name}"));
                    }
                    conditions.push(Condition {
                        op,
                        if_exists,
                        key: key.to_ascii_lowercase(),
                        values,
                    });
                }
            }
        }
        Ok(Statement {
            effect,
            principal,
            actions,
            resources,
            conditions,
        })
    }

    /// Evaluate one action on one resource.
    pub fn evaluate(&self, action: &str, resource: &str, facts: &RequestFacts) -> Decision {
        let mut decision = Decision::Implicit;
        for s in &self.statements {
            if !s.applies(action, resource, facts) {
                continue;
            }
            match s.effect {
                Effect::Deny => return Decision::Deny,
                Effect::Allow => decision = Decision::Allow,
            }
        }
        decision
    }

    /// True when some statement allows anonymous (`*`) access, for
    /// GetBucketPolicyStatus.
    pub fn is_public(&self) -> bool {
        self.statements.iter().any(|s| {
            s.effect == Effect::Allow
                && matches!(s.principal, Some(Principals::Any))
                && s.conditions.is_empty()
        })
    }
}

impl Statement {
    fn applies(&self, action: &str, resource: &str, facts: &RequestFacts) -> bool {
        if let Some(p) = &self.principal {
            let ok = match (p, &facts.principal) {
                (Principals::Any, _) => true,
                (Principals::List(_), Principal::Anonymous) => false,
                (Principals::List(list), Principal::User { name, access_key }) => {
                    list.iter().any(|e| {
                        e == name
                            || e == access_key
                            || e.rsplit_once(":user/").is_some_and(|(_, n)| n == name)
                    })
                }
            };
            if !ok {
                return false;
            }
        }
        let action_hit = self
            .actions
            .patterns
            .iter()
            .any(|p| p == "*" || glob(p, action, true));
        if action_hit == self.actions.negated {
            return false;
        }
        let resource_hit = self
            .resources
            .patterns
            .iter()
            .any(|p| p == "*" || glob(&facts.expand(p), resource, false));
        if resource_hit == self.resources.negated {
            return false;
        }
        self.conditions.iter().all(|c| c.holds(facts))
    }
}

impl Condition {
    fn holds(&self, facts: &RequestFacts) -> bool {
        let value = facts.get(&self.key);
        if self.op == Op::Null {
            // "Null": "true" means the key must be absent.
            return self.values.iter().any(|v| (v == "true") == value.is_none());
        }
        let Some(value) = value else {
            return self.if_exists;
        };
        let vals = self.values.iter().map(|v| facts.expand(v));
        let num = |v: &str| v.parse::<f64>().ok();
        match self.op {
            Op::StringEquals => vals.into_iter().any(|v| v == value),
            Op::StringNotEquals => vals.into_iter().all(|v| v != value),
            Op::StringEqualsIgnoreCase => vals.into_iter().any(|v| v.eq_ignore_ascii_case(value)),
            Op::StringNotEqualsIgnoreCase => {
                vals.into_iter().all(|v| !v.eq_ignore_ascii_case(value))
            }
            Op::StringLike => vals.into_iter().any(|v| glob(&v, value, false)),
            Op::StringNotLike => vals.into_iter().all(|v| !glob(&v, value, false)),
            Op::Bool => vals.into_iter().any(|v| v.eq_ignore_ascii_case(value)),
            Op::IpAddress | Op::NotIpAddress => {
                let Ok(ip) = value.parse::<IpAddr>() else {
                    return false;
                };
                let hit = self
                    .values
                    .iter()
                    .filter_map(|v| parse_cidr(v))
                    .any(|net| in_cidr(ip, net));
                hit == (self.op == Op::IpAddress)
            }
            Op::NumericEquals
            | Op::NumericNotEquals
            | Op::NumericLessThan
            | Op::NumericLessThanEquals
            | Op::NumericGreaterThan
            | Op::NumericGreaterThanEquals => {
                let Some(x) = num(value) else { return false };
                let cmp = |v: String| {
                    num(&v).is_some_and(|y| match self.op {
                        Op::NumericEquals => x == y,
                        Op::NumericNotEquals => x != y,
                        Op::NumericLessThan => x < y,
                        Op::NumericLessThanEquals => x <= y,
                        Op::NumericGreaterThan => x > y,
                        _ => x >= y,
                    })
                };
                if self.op == Op::NumericNotEquals {
                    vals.into_iter().all(cmp)
                } else {
                    vals.into_iter().any(cmp)
                }
            }
            Op::Null => unreachable!(),
        }
    }
}

/// Combine an identity policy and a bucket policy for one check.
pub fn decide(
    identity: Option<&Policy>,
    bucket: Option<&Policy>,
    action: &str,
    resource: &str,
    facts: &RequestFacts,
) -> bool {
    let a = identity.map_or(Decision::Implicit, |p| p.evaluate(action, resource, facts));
    let b = bucket.map_or(Decision::Implicit, |p| p.evaluate(action, resource, facts));
    if a == Decision::Deny || b == Decision::Deny {
        return false;
    }
    a == Decision::Allow || b == Decision::Allow
}

pub fn bucket_arn(bucket: &str) -> String {
    format!("arn:aws:s3:::{bucket}")
}

pub fn object_arn(bucket: &str, key: &str) -> String {
    format!("arn:aws:s3:::{bucket}/{key}")
}

/// One permission a request needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Check {
    pub action: &'static str,
    pub resource: String,
}

/// The permissions an S3 request needs. Per-key permissions of
/// DeleteObjects and the source of a copy are checked by their handlers.
pub fn required(
    method: &Method,
    bucket: Option<&str>,
    key: Option<&str>,
    query: &[(String, String)],
    headers: &HeaderMap,
) -> Vec<Check> {
    let has = |n: &str| query.iter().any(|(k, _)| k == n);
    let versioned = has("versionId");
    let pick = |plain: &'static str, version: &'static str| if versioned { version } else { plain };
    let m = method.as_str();
    let Some(bucket) = bucket else {
        return vec![Check {
            action: "s3:ListAllMyBuckets",
            resource: "*".into(),
        }];
    };
    let Some(key) = key else {
        let get = m == "GET" || m == "HEAD";
        let action = match () {
            _ if has("policyStatus") => "s3:GetBucketPolicyStatus",
            _ if has("policy") => match m {
                "GET" => "s3:GetBucketPolicy",
                "DELETE" => "s3:DeleteBucketPolicy",
                _ => "s3:PutBucketPolicy",
            },
            _ if has("tagging") => {
                if get {
                    "s3:GetBucketTagging"
                } else {
                    "s3:PutBucketTagging"
                }
            }
            _ if has("cors") => {
                if get {
                    "s3:GetBucketCORS"
                } else {
                    "s3:PutBucketCORS"
                }
            }
            _ if has("versioning") => {
                if get {
                    "s3:GetBucketVersioning"
                } else {
                    "s3:PutBucketVersioning"
                }
            }
            _ if has("lifecycle") => {
                if get {
                    "s3:GetLifecycleConfiguration"
                } else {
                    "s3:PutLifecycleConfiguration"
                }
            }
            _ if has("object-lock") => {
                if get {
                    "s3:GetBucketObjectLockConfiguration"
                } else {
                    "s3:PutBucketObjectLockConfiguration"
                }
            }
            _ if has("encryption") => {
                if get {
                    "s3:GetEncryptionConfiguration"
                } else {
                    "s3:PutEncryptionConfiguration"
                }
            }
            _ if has("acl") => {
                if get {
                    "s3:GetBucketAcl"
                } else {
                    "s3:PutBucketAcl"
                }
            }
            _ if has("location") => "s3:GetBucketLocation",
            _ if has("notification") => {
                if get {
                    "s3:GetBucketNotification"
                } else {
                    "s3:PutBucketNotification"
                }
            }
            _ if has("uploads") => "s3:ListBucketMultipartUploads",
            _ if has("versions") => "s3:ListBucketVersions",
            _ if m == "POST" && has("delete") => return Vec::new(),
            _ if m == "PUT" => "s3:CreateBucket",
            _ if m == "DELETE" => "s3:DeleteBucket",
            _ => "s3:ListBucket",
        };
        return vec![Check {
            action,
            resource: bucket_arn(bucket),
        }];
    };
    let mut checks = Vec::new();
    let action = match m {
        _ if has("tagging") => match m {
            "GET" => pick("s3:GetObjectTagging", "s3:GetObjectVersionTagging"),
            "DELETE" => pick("s3:DeleteObjectTagging", "s3:DeleteObjectVersionTagging"),
            _ => pick("s3:PutObjectTagging", "s3:PutObjectVersionTagging"),
        },
        _ if has("retention") => {
            if m == "GET" {
                "s3:GetObjectRetention"
            } else {
                "s3:PutObjectRetention"
            }
        }
        _ if has("legal-hold") => {
            if m == "GET" {
                "s3:GetObjectLegalHold"
            } else {
                "s3:PutObjectLegalHold"
            }
        }
        _ if has("acl") => {
            if m == "GET" {
                pick("s3:GetObjectAcl", "s3:GetObjectVersionAcl")
            } else {
                "s3:PutObjectAcl"
            }
        }
        "GET" | "HEAD" if has("uploadId") => "s3:ListMultipartUploadParts",
        "GET" | "HEAD" => pick("s3:GetObject", "s3:GetObjectVersion"),
        "DELETE" if has("uploadId") => "s3:AbortMultipartUpload",
        "DELETE" => pick("s3:DeleteObject", "s3:DeleteObjectVersion"),
        _ => "s3:PutObject",
    };
    checks.push(Check {
        action,
        resource: object_arn(bucket, key),
    });
    let bypass = headers
        .get("x-amz-bypass-governance-retention")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("true"));
    if bypass {
        checks.push(Check {
            action: "s3:BypassGovernanceRetention",
            resource: object_arn(bucket, key),
        });
    }
    checks
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(name: &str) -> RequestFacts {
        RequestFacts::new(Principal::User {
            name: name.into(),
            access_key: format!("AK{name}"),
        })
    }

    #[test]
    fn globs() {
        assert!(glob("s3:Get*", "s3:GetObject", true));
        assert!(glob("s3:get*", "s3:GetObject", true));
        assert!(!glob("s3:Put*", "s3:GetObject", true));
        assert!(glob(
            "arn:aws:s3:::b/a?c/*",
            "arn:aws:s3:::b/abc/x/y",
            false
        ));
        assert!(!glob("arn:aws:s3:::b/*", "arn:aws:s3:::bb/x", false));
        assert!(glob("*", "", false));
    }

    #[test]
    fn deny_wins_and_default_denies() {
        let p = Policy::parse(
            r#"{"Version":"2012-10-17","Statement":[
                {"Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::data/*"},
                {"Effect":"Deny","Action":"s3:DeleteObject","Resource":"arn:aws:s3:::data/keep/*"}]}"#,
            PolicyKind::Identity,
        )
        .unwrap();
        let f = user("ana");
        assert_eq!(
            p.evaluate("s3:GetObject", "arn:aws:s3:::data/x", &f),
            Decision::Allow
        );
        assert_eq!(
            p.evaluate("s3:DeleteObject", "arn:aws:s3:::data/keep/x", &f),
            Decision::Deny
        );
        assert_eq!(
            p.evaluate("s3:GetObject", "arn:aws:s3:::other/x", &f),
            Decision::Implicit
        );
        assert!(!decide(
            Some(&p),
            None,
            "s3:GetObject",
            "arn:aws:s3:::other/x",
            &f
        ));
    }

    #[test]
    fn bucket_policy_principals_and_conditions() {
        let p = Policy::parse(
            r#"{"Statement":[
                {"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::web/*",
                 "Condition":{"IpAddress":{"aws:SourceIp":["10.0.0.0/8","::1"]}}},
                {"Effect":"Allow","Principal":{"AWS":["arn:aws:iam::123:user/bob"]},"Action":"s3:ListBucket",
                 "Resource":"arn:aws:s3:::web","Condition":{"StringLike":{"s3:prefix":"${aws:username}/*"}}}]}"#,
            PolicyKind::Bucket("web"),
        )
        .unwrap();
        let mut anon = RequestFacts::new(Principal::Anonymous);
        anon.set("aws:SourceIp", "10.1.2.3");
        assert_eq!(
            p.evaluate("s3:GetObject", "arn:aws:s3:::web/i.html", &anon),
            Decision::Allow
        );
        anon.set("aws:SourceIp", "192.168.0.1");
        assert_eq!(
            p.evaluate("s3:GetObject", "arn:aws:s3:::web/i.html", &anon),
            Decision::Implicit
        );
        anon.set("aws:SourceIp", "::ffff:10.0.0.1");
        assert_eq!(
            p.evaluate("s3:GetObject", "arn:aws:s3:::web/i.html", &anon),
            Decision::Allow
        );

        let mut bob = user("bob");
        bob.set("s3:prefix", "bob/photos/");
        assert_eq!(
            p.evaluate("s3:ListBucket", "arn:aws:s3:::web", &bob),
            Decision::Allow
        );
        bob.set("s3:prefix", "alice/");
        assert_eq!(
            p.evaluate("s3:ListBucket", "arn:aws:s3:::web", &bob),
            Decision::Implicit
        );
        let mut eve = user("eve");
        eve.set("s3:prefix", "eve/");
        assert_eq!(
            p.evaluate("s3:ListBucket", "arn:aws:s3:::web", &eve),
            Decision::Implicit
        );
        assert!(!p.is_public());
    }

    #[test]
    fn rejects_bad_documents() {
        let bad = [
            (r#"{"Statement":[]}"#, PolicyKind::Identity),
            (
                r#"{"Statement":{"Effect":"Maybe","Action":"s3:*","Resource":"*"}}"#,
                PolicyKind::Identity,
            ),
            (
                r#"{"Statement":{"Effect":"Allow","Action":"ec2:*","Resource":"*"}}"#,
                PolicyKind::Identity,
            ),
            (
                r#"{"Statement":{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"*"}}"#,
                PolicyKind::Identity,
            ),
            (
                r#"{"Statement":{"Effect":"Allow","Action":"s3:*","Resource":"*"}}"#,
                PolicyKind::Bucket("b"),
            ),
            (
                r#"{"Statement":{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"arn:aws:s3:::other/*"}}"#,
                PolicyKind::Bucket("b"),
            ),
            (
                r#"{"Statement":{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"arn:aws:s3:::b/*","Condition":{"Teleport":{"a":"b"}}}}"#,
                PolicyKind::Bucket("b"),
            ),
        ];
        for (doc, kind) in bad {
            assert!(Policy::parse(doc, kind).is_err(), "{doc}");
        }
    }

    #[test]
    fn classifies_requests() {
        let h = HeaderMap::new();
        let q = |s: &str| -> Vec<(String, String)> {
            s.split('&')
                .filter(|p| !p.is_empty())
                .map(|p| {
                    let (k, v) = p.split_once('=').unwrap_or((p, ""));
                    (k.into(), v.into())
                })
                .collect()
        };
        let act = |m: Method, b: Option<&str>, k: Option<&str>, qs: &str| {
            required(&m, b, k, &q(qs), &h)[0].action
        };
        assert_eq!(act(Method::GET, None, None, ""), "s3:ListAllMyBuckets");
        assert_eq!(
            act(Method::GET, Some("b"), None, "list-type=2"),
            "s3:ListBucket"
        );
        assert_eq!(act(Method::HEAD, Some("b"), None, ""), "s3:ListBucket");
        assert_eq!(
            act(Method::PUT, Some("b"), None, "policy"),
            "s3:PutBucketPolicy"
        );
        assert_eq!(
            act(Method::GET, Some("b"), Some("k"), "versionId=1"),
            "s3:GetObjectVersion"
        );
        assert_eq!(
            act(Method::POST, Some("b"), Some("k"), "uploads"),
            "s3:PutObject"
        );
        assert_eq!(
            act(Method::DELETE, Some("b"), Some("k"), "uploadId=x"),
            "s3:AbortMultipartUpload"
        );
        assert!(required(&Method::POST, Some("b"), None, &q("delete"), &h).is_empty());
    }
}
