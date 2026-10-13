//! S3 Intelligent-Tiering configurations, which drive Kosha's access-based
//! tiering: objects nobody reads for the configured number of days move to
//! the cold tier, and come back when read.

use std::collections::BTreeMap;

use kosha_engine::TieringRule;
use serde::Deserialize;

use crate::error::{S3Error, S3Result};
use crate::xml::Xml;

#[derive(Deserialize)]
struct TagXml {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "Value")]
    value: String,
}

#[derive(Deserialize, Default)]
struct AndXml {
    #[serde(rename = "Prefix")]
    prefix: Option<String>,
    #[serde(rename = "Tag", default)]
    tags: Vec<TagXml>,
}

#[derive(Deserialize, Default)]
struct FilterXml {
    #[serde(rename = "Prefix")]
    prefix: Option<String>,
    #[serde(rename = "Tag")]
    tag: Option<TagXml>,
    #[serde(rename = "And")]
    and: Option<AndXml>,
}

#[derive(Deserialize)]
struct TieringXml {
    #[serde(rename = "Days")]
    days: u32,
    #[serde(rename = "AccessTier")]
    access_tier: String,
}

#[derive(Deserialize)]
struct ConfigXml {
    #[serde(rename = "Id")]
    id: String,
    #[serde(rename = "Filter", default)]
    filter: FilterXml,
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "Tiering", default)]
    tierings: Vec<TieringXml>,
}

pub fn parse(xml: &str, id: &str) -> S3Result<TieringRule> {
    let c: ConfigXml =
        quick_xml::de::from_str(xml).map_err(|e| S3Error::malformed_xml(e.to_string()))?;
    if c.id != id {
        return Err(S3Error::invalid_argument(
            "the Id must match the id parameter",
        ));
    }
    let enabled = match c.status.as_str() {
        "Enabled" => true,
        "Disabled" => false,
        s => return Err(S3Error::malformed_xml(format!("unknown Status {s:?}"))),
    };
    for t in &c.tierings {
        if !matches!(
            t.access_tier.as_str(),
            "ARCHIVE_ACCESS" | "DEEP_ARCHIVE_ACCESS"
        ) {
            return Err(S3Error::invalid_argument(format!(
                "unknown AccessTier {}",
                t.access_tier
            )));
        }
        if t.days == 0 {
            return Err(S3Error::invalid_argument("Days must be at least 1"));
        }
    }
    // Kosha has one cold tier: the earliest tiering decides when data moves.
    let first = c
        .tierings
        .iter()
        .min_by_key(|t| t.days)
        .ok_or_else(|| S3Error::malformed_xml("a configuration needs a Tiering"))?;
    let mut prefix = c.filter.prefix.unwrap_or_default();
    let mut tags = BTreeMap::new();
    if let Some(t) = c.filter.tag {
        tags.insert(t.key, t.value);
    }
    if let Some(a) = c.filter.and {
        if let Some(p) = a.prefix {
            prefix = p;
        }
        for t in a.tags {
            tags.insert(t.key, t.value);
        }
    }
    Ok(TieringRule {
        id: c.id,
        enabled,
        prefix,
        tags,
        days: first.days,
        access_tier: first.access_tier.clone(),
    })
}

fn write(x: &mut Xml, r: &TieringRule) {
    x.elem("Id", &r.id);
    x.open("Filter");
    if r.tags.is_empty() {
        if !r.prefix.is_empty() {
            x.elem("Prefix", &r.prefix);
        }
    } else {
        x.open("And").elem("Prefix", &r.prefix);
        for (k, v) in &r.tags {
            x.open("Tag").elem("Key", k).elem("Value", v).close("Tag");
        }
        x.close("And");
    }
    x.close("Filter");
    x.elem("Status", if r.enabled { "Enabled" } else { "Disabled" });
    x.open("Tiering")
        .elem("Days", r.days.to_string())
        .elem("AccessTier", &r.access_tier)
        .close("Tiering");
}

pub fn render(r: &TieringRule) -> String {
    let mut x = Xml::new("IntelligentTieringConfiguration");
    write(&mut x, r);
    x.finish("IntelligentTieringConfiguration")
}

pub fn render_list(rules: &[TieringRule]) -> String {
    let mut x = Xml::new("ListBucketIntelligentTieringConfigurationsOutput");
    x.elem("IsTruncated", "false");
    for r in rules {
        x.open("IntelligentTieringConfiguration");
        write(&mut x, r);
        x.close("IntelligentTieringConfiguration");
    }
    x.finish("ListBucketIntelligentTieringConfigurationsOutput")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let xml = r#"<IntelligentTieringConfiguration><Id>media</Id><Filter><And><Prefix>videos/</Prefix><Tag><Key>tier</Key><Value>auto</Value></Tag></And></Filter><Status>Enabled</Status><Tiering><Days>180</Days><AccessTier>DEEP_ARCHIVE_ACCESS</AccessTier></Tiering><Tiering><Days>90</Days><AccessTier>ARCHIVE_ACCESS</AccessTier></Tiering></IntelligentTieringConfiguration>"#;
        let r = parse(xml, "media").unwrap();
        assert_eq!(
            (r.days, r.access_tier.as_str(), r.prefix.as_str()),
            (90, "ARCHIVE_ACCESS", "videos/")
        );
        assert_eq!(r.tags["tier"], "auto");
        assert_eq!(parse(&render(&r), "media").unwrap(), r);
        assert!(parse(xml, "other").is_err());
        assert!(parse(&xml.replace("ARCHIVE_ACCESS", "MOON_ACCESS"), "media").is_err());
    }
}
