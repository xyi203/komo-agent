use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use komo_core::domain::{
    cancel::Cancelled,
    context::ToolContext,
    tool::{Tool, ToolError, ToolOutput, parse_args},
};

const MAX_RESULTS: usize = 6;
const USER_AGENT: &str =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 komo-bot/0.1";

#[derive(Deserialize)]
struct SearchArgs {
    query: String,
}

/// Web search via DuckDuckGo's keyless HTML endpoint (best-effort).
pub struct WebSearchTool {
    client: reqwest::Client,
}

impl WebSearchTool {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl Default for WebSearchTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for WebSearchTool {
    fn name(&self) -> &'static str {
        "web_search"
    }

    fn description(&self) -> &'static str {
        "Search the web and return the top result titles, URLs, and snippets."
    }

    /// Read-only query: safe to retry on an ambiguous transient failure.
    fn idempotent(&self) -> bool {
        true
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "query": { "type": "string", "description": "The search query." }
            },
            "required": ["query"]
        })
    }

    async fn call(&self, input: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: SearchArgs = parse_args(&input)?;

        let fetch = async {
            let resp = self
                .client
                .get("https://html.duckduckgo.com/html/")
                .header(reqwest::header::USER_AGENT, USER_AGENT)
                .query(&[("q", args.query.as_str())])
                .send()
                .await
                .map_err(|e| crate::http::transport_error(e, "search request failed"))?;
            resp.text()
                .await
                .map_err(|e| crate::http::transport_error(e, "failed to read search results"))
        };
        // A search is a read with nothing to leave half-done, so a cancelled turn
        // drops the request instead of holding the connection open to its timeout.
        let body = tokio::select! {
            r = fetch => r?,
            _ = ctx.cancelled() => return Err(ToolError::Failed(Cancelled.into())),
        };

        let results = parse_results(&body);
        if results.is_empty() {
            return Ok(ToolOutput::text(format!(
                "No parseable results for `{}`. The search endpoint may have \
                 changed or rate-limited this request.",
                args.query
            )));
        }

        let rendered = results
            .iter()
            .take(MAX_RESULTS)
            .enumerate()
            .map(|(i, r)| {
                let mut s = format!("{}. {}\n   {}", i + 1, r.title, r.url);
                if !r.snippet.is_empty() {
                    s.push_str(&format!("\n   {}", r.snippet));
                }
                s
            })
            .collect::<Vec<_>>()
            .join("\n\n");
        Ok(ToolOutput::text(rendered))
    }
}

struct SearchResult {
    title: String,
    url: String,
    snippet: String,
}

/// Best-effort parse of DuckDuckGo's HTML result list.
fn parse_results(html: &str) -> Vec<SearchResult> {
    let mut results = Vec::new();

    for block in html.split("result__a").skip(1) {
        // href="...">Title</a>
        let url = block
            .split_once("href=\"")
            .and_then(|(_, rest)| rest.split_once('"'))
            .map(|(href, _)| decode_ddg_url(href))
            .unwrap_or_default();
        let title = block
            .split_once('>')
            .and_then(|(_, rest)| rest.split_once("</a>"))
            .map(|(t, _)| strip_tags(t))
            .unwrap_or_default();
        let snippet = block
            .split_once("result__snippet")
            .and_then(|(_, rest)| rest.split_once('>'))
            .and_then(|(_, rest)| rest.split_once("</a>"))
            .map(|(s, _)| strip_tags(s))
            .unwrap_or_default();

        if !title.is_empty() && !url.is_empty() {
            results.push(SearchResult {
                title,
                url,
                snippet,
            });
        }
    }
    results
}

/// DuckDuckGo wraps result URLs in a redirect: `//duckduckgo.com/l/?uddg=<enc>`.
fn decode_ddg_url(href: &str) -> String {
    let target = href
        .split_once("uddg=")
        .map(|(_, rest)| rest.split('&').next().unwrap_or(rest))
        .unwrap_or(href);
    percent_decode(target)
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                if let Ok(byte) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                    out.push(byte);
                    i += 3;
                    continue;
                }
                out.push(b'%');
                i += 1;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn strip_tags(s: &str) -> String {
    let mut out = String::new();
    let mut in_tag = false;
    for ch in s.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_ddg_redirect_url() {
        let href = "//duckduckgo.com/l/?uddg=https%3A%2F%2Fexample.com%2Fa%20b&rut=x";
        assert_eq!(decode_ddg_url(href), "https://example.com/a b");
    }

    #[test]
    fn parses_a_minimal_result_block() {
        let html = r#"<a class="result__a" href="//duckduckgo.com/l/?uddg=https%3A%2F%2Frust-lang.org">The Rust Language</a>
            <a class="result__snippet" href="x">A systems <b>programming</b> language</a>"#;
        let results = parse_results(html);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].title, "The Rust Language");
        assert_eq!(results[0].url, "https://rust-lang.org");
        assert!(results[0].snippet.contains("programming"));
    }
}
