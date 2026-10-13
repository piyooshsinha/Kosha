//! Minimal XML helpers. S3 responses are small and flat, so they are built
//! with a string writer; request bodies are parsed with quick-xml + serde.

use chrono::{DateTime, Utc};

pub const NS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

pub fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    out
}

pub fn iso8601(t: &DateTime<Utc>) -> String {
    t.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

pub struct Xml {
    buf: String,
}

impl Xml {
    /// Start a document whose root element carries the S3 namespace.
    pub fn new(root: &str) -> Self {
        Xml {
            buf: format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<{root} xmlns=\"{NS}\">"),
        }
    }

    pub fn open(&mut self, tag: &str) -> &mut Self {
        self.buf.push('<');
        self.buf.push_str(tag);
        self.buf.push('>');
        self
    }

    pub fn close(&mut self, tag: &str) -> &mut Self {
        self.buf.push_str("</");
        self.buf.push_str(tag);
        self.buf.push('>');
        self
    }

    pub fn elem(&mut self, tag: &str, value: impl AsRef<str>) -> &mut Self {
        self.open(tag);
        self.buf.push_str(&escape(value.as_ref()));
        self.close(tag)
    }

    pub fn opt(&mut self, tag: &str, value: Option<impl AsRef<str>>) -> &mut Self {
        if let Some(v) = value {
            self.elem(tag, v);
        }
        self
    }

    /// Text content directly inside the current element.
    pub fn elem_text(&mut self, value: &str) -> &mut Self {
        self.buf.push_str(&escape(value));
        self
    }

    /// Pre-built markup (must be well formed).
    pub fn raw(&mut self, markup: &str) -> &mut Self {
        self.buf.push_str(markup);
        self
    }

    pub fn finish(mut self, root: &str) -> String {
        self.close(root);
        self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_escaped_document() {
        let mut x = Xml::new("Root");
        x.elem("Key", "a<b&c");
        let s = x.finish("Root");
        assert!(s.ends_with("<Key>a&lt;b&amp;c</Key></Root>"));
    }
}
