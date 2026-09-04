use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use komo_core::domain::{
    approval::{ActionRef, ApprovalRequest},
    cancel::Cancelled,
    context::ToolContext,
    tool::{Tool, ToolError, ToolOutput, parse_args},
};

/// Ceiling on how much of a response body is downloaded at all. The tool no
/// longer trims for the model — that's the executor's single choke point
/// (`max_tool_result_bytes`) — so this bound exists to stop a 5 MB page from
/// being pulled into memory and parsed, not to size the model's view.
const MAX_RESPONSE_BYTES: usize = 256 * 1024;
const USER_AGENT: &str = "komo-bot/0.1";

/// Per-request timeout for the fetch client. `reqwest`'s default client sets no
/// timeout at all, so a server that accepts the connection then never responds
/// would hang the call until the executor's outer wall-clock backstop fired.
/// This inner timeout fails faster and, being a proper request timeout, is
/// classified transient — an idempotent GET is retried once or twice.
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Cap on caller-supplied request headers — enough for auth + content
/// negotiation, far below anything abusive.
const MAX_HEADERS: usize = 8;

#[derive(Deserialize)]
struct FetchArgs {
    url: String,
    /// Optional request headers — the door to authenticated JSON APIs
    /// (`X-Auth-Token` for Miniflux, `Authorization: Bearer …`), which is what
    /// lets data-source skills work through this one read-only tool.
    #[serde(default)]
    headers: std::collections::HashMap<String, String>,
    #[serde(default)]
    format: Format,
}

/// How the fetched document is rendered for the model. Only affects `text/html`
/// responses — JSON and plain text are returned as served, whatever the format.
#[derive(Deserialize, Clone, Copy, PartialEq, Eq, Debug, Default)]
#[serde(rename_all = "lowercase")]
enum Format {
    /// Headings, lists, code blocks and link targets preserved.
    #[default]
    Markdown,
    /// Prose only — structure markers dropped.
    Text,
    /// The raw document, untouched.
    Html,
}

impl Format {
    /// Ask the server for the representation we actually want, so a site that
    /// serves both markdown and HTML gives us the cheap one (v2's `acceptHeader`).
    fn accept(self) -> &'static str {
        match self {
            Format::Markdown => {
                "text/markdown;q=1.0, text/x-markdown;q=0.9, text/plain;q=0.8, \
                 text/html;q=0.7, */*;q=0.1"
            }
            Format::Text => "text/plain;q=1.0, text/markdown;q=0.9, text/html;q=0.8, */*;q=0.1",
            Format::Html => {
                "text/html;q=1.0, application/xhtml+xml;q=0.9, text/plain;q=0.8, \
                 text/markdown;q=0.7, */*;q=0.1"
            }
        }
    }
}

/// Fetches a URL and returns its content as markdown (default), plain text, or
/// raw HTML. Binary content types (images, PDFs, archives) are refused by
/// `content-type` rather than lossy-decoded into the transcript.
///
/// A GET is read-only (`Risk::Safe`), but it is still an outbound request to an
/// arbitrary URL — untrusted page content can steer the model into fetching an
/// attacker's host with sensitive query params. So the fetch consults the
/// approver with an [`ActionRef::Network`] before sending: the policy layer's
/// deny rules can blackhole hosts (`category = "network"`), while an unmatched
/// URL proceeds without any prompt (safe actions never escalate).
pub struct WebFetchTool {
    client: reqwest::Client,
}

impl WebFetchTool {
    pub fn new() -> Self {
        // A timeout-less client can hang indefinitely on an unresponsive server;
        // fall back to the default client only if the builder somehow fails.
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .unwrap_or_default();
        Self { client }
    }
}

impl Default for WebFetchTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for WebFetchTool {
    fn name(&self) -> &'static str {
        "web_fetch"
    }

    fn description(&self) -> &'static str {
        "Fetch an http/https URL (GET) and return its content as markdown \
         (default), plain text, or raw html. Optional request headers support \
         authenticated JSON APIs (e.g. an X-Auth-Token). Read-only; images, PDFs \
         and other binary content types are refused rather than returned as \
         garbled text."
    }

    /// Read-only GET: safe to retry on an ambiguous transient failure.
    fn idempotent(&self) -> bool {
        true
    }

    /// Header values are exactly credential-shaped (API tokens, bearer auth) —
    /// mask them before the args land in the run ledger. Header *names* stay,
    /// so an audit still shows what kind of auth was sent, just not the secret.
    fn redact_args(&self, args: &str) -> String {
        match serde_json::from_str::<serde_json::Value>(args) {
            Ok(mut v) => {
                if let Some(headers) = v.get_mut("headers").and_then(|h| h.as_object_mut()) {
                    for (_, value) in headers.iter_mut() {
                        *value = serde_json::json!("<redacted>");
                    }
                }
                v.to_string()
            }
            Err(_) => "<web_fetch args redacted>".to_string(),
        }
    }

    fn parameters_schema(&self) -> serde_json::Value {
        json!({
            "type": "object",
            "properties": {
                "url": { "type": "string", "description": "The absolute URL to fetch." },
                "headers": {
                    "type": "object",
                    "additionalProperties": { "type": "string" },
                    "description": "Optional request headers (e.g. {\"X-Auth-Token\": \"…\"} for an authenticated API)."
                },
                "format": {
                    "type": "string",
                    "enum": ["markdown", "text", "html"],
                    "description": "How to render an HTML page: `markdown` (default — keeps \
                     headings, lists, code blocks and link targets), `text` (prose only), or \
                     `html` (raw source). JSON and plain-text responses ignore this."
                }
            },
            "required": ["url"]
        })
    }

    async fn call(&self, input: Value, ctx: &ToolContext) -> Result<ToolOutput, ToolError> {
        let args: FetchArgs = parse_args(&input)?;
        if args.headers.len() > MAX_HEADERS {
            return Err(ToolError::InvalidInput(format!(
                "too many headers ({}, max {MAX_HEADERS})",
                args.headers.len()
            )));
        }
        let scheme = args
            .url
            .split("://")
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        if !matches!(scheme.as_str(), "http" | "https") {
            return Err(ToolError::InvalidInput(format!(
                "`url` must be an absolute http:// or https:// URL (got `{}`)",
                args.url
            )));
        }

        let request =
            ApprovalRequest::safe(format!("fetch {}", args.url)).with_action(ActionRef::Network {
                url: args.url.clone(),
            });
        if !ctx.approve(&request).await {
            return Ok(ToolOutput::text(format!(
                "URL blocked by the permission policy (a `network` deny rule matches {}); \
                 nothing was fetched.",
                args.url
            )));
        }

        // The whole network exchange is one cancellable unit: dropping the
        // request future closes the connection, and a GET has no side effect to
        // leave half-done — so unlike a file mutation, abandoning it costs
        // nothing. Without this a cancelled turn keeps an outbound request alive
        // for up to the 30s request timeout.
        let work = async {
            let mut request = self
                .client
                .get(&args.url)
                .header(reqwest::header::USER_AGENT, USER_AGENT)
                .header(reqwest::header::ACCEPT, args.format.accept())
                .header(reqwest::header::ACCEPT_LANGUAGE, "en-US,en;q=0.9");
            for (name, value) in &args.headers {
                request = request.header(name, value);
            }
            let mut resp = request.send().await.map_err(|e| {
                crate::http::transport_error(e, format!("request to {} failed", args.url))
            })?;

            let status = resp.status();
            let content_type = resp
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            let mime = mime_of(&content_type);
            // Refuse binary content by *type* instead of decoding it. `resp.text()`
            // on a PDF or a PNG lossy-decodes into a screenful of replacement
            // characters that then sits in the transcript for the rest of the
            // session, teaching the model nothing except that the fetch "worked".
            if is_image(&mime) {
                return Err(ToolError::Failed(anyhow::anyhow!(
                    "Unsupported fetched image content type: {mime}"
                )));
            }
            if !is_textual(&mime) {
                return Err(ToolError::Failed(anyhow::anyhow!(
                    "Unsupported fetched file content type: {mime}"
                )));
            }
            // A declared length over the ceiling fails before a byte of body is
            // read; a chunked response has no declared length, so the accumulator
            // below is the backstop.
            if let Some(len) = resp.content_length()
                && len as usize > MAX_RESPONSE_BYTES
            {
                return Err(ToolError::Failed(anyhow::anyhow!(
                    "response too large ({} KB, limit {} KB) — nothing was downloaded",
                    len / 1024,
                    MAX_RESPONSE_BYTES / 1024
                )));
            }

            let mut body = Vec::new();
            let mut overflowed = false;
            while let Some(chunk) = resp
                .chunk()
                .await
                .map_err(|e| crate::http::transport_error(e, "failed to read body"))?
            {
                body.extend_from_slice(&chunk);
                if body.len() >= MAX_RESPONSE_BYTES {
                    // Unlike the declared-length case there is no cheap way to know
                    // this was coming, and the head of a long document is usually
                    // the useful part — so keep it and say what happened, rather
                    // than discarding a download already paid for.
                    body.truncate(MAX_RESPONSE_BYTES);
                    overflowed = true;
                    break;
                }
            }
            let raw = String::from_utf8_lossy(&body);

            let mut text = if mime.contains("html") {
                render_html(&raw, args.format)
            } else {
                raw.to_string()
            };
            if overflowed {
                text.push_str(&format!(
                    "\n\n…[download stopped at the {} KB limit; the rest of the response \
                 was not fetched]",
                    MAX_RESPONSE_BYTES / 1024
                ));
            }
            Ok(
                ToolOutput::text(format!("HTTP {status}\n\n{text}")).with_structured(json!({
                    "url": args.url,
                    "status": status.as_u16(),
                    "content_type": content_type,
                    "format": format!("{:?}", args.format).to_lowercase(),
                    "bytes": body.len(),
                    "truncated": overflowed,
                })),
            )
        };
        tokio::select! {
            out = work => out,
            _ = ctx.cancelled() => Err(ToolError::Failed(Cancelled.into())),
        }
    }
}

/// The bare mime type: `text/html; charset=utf-8` → `text/html`.
fn mime_of(content_type: &str) -> String {
    content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

/// A raster image — refused outright. SVG is excluded: it *is* text, and a model
/// can read it usefully.
fn is_image(mime: &str) -> bool {
    mime.starts_with("image/") && mime != "image/svg+xml"
}

/// Types worth decoding as text. An absent content-type counts as textual — some
/// small APIs send none, and the old behavior (decode anyway) is right there.
fn is_textual(mime: &str) -> bool {
    mime.is_empty()
        || mime.starts_with("text/")
        || mime.ends_with("+json")
        || mime.ends_with("+xml")
        || matches!(
            mime,
            "application/json"
                | "application/xml"
                | "application/javascript"
                | "application/x-javascript"
        )
}

/// HTML → markdown (or plain prose). Deliberately a small hand-rolled walker
/// rather than a full parser dependency: komo needs headings, lists, code blocks
/// and link targets to survive — beyond that, fidelity buys the model nothing.
///
/// `<script>`/`<style>` bodies are dropped wholesale; character entities for the
/// handful that actually change meaning are decoded; whitespace is collapsed as
/// it is emitted, except inside `<pre>`.
fn render_html(html: &str, format: Format) -> String {
    if format == Format::Html {
        return html.to_string();
    }
    let markdown = format == Format::Markdown;
    // ASCII-only lowercasing keeps byte offsets aligned with `html` — a
    // Unicode-aware `to_lowercase` can change a string's length and make every
    // index below drift.
    let lower = html.to_ascii_lowercase();
    let mut out = String::with_capacity(html.len() / 2);
    let mut links: Vec<Option<String>> = Vec::new();
    let mut pre_depth = 0usize;

    let mut i = 0;
    while i < html.len() {
        let mut skipped = false;
        for (open, close) in [("<script", "</script>"), ("<style", "</style>")] {
            if lower[i..].starts_with(open) {
                i = match lower[i..].find(close) {
                    Some(rel) => i + rel + close.len(),
                    None => html.len(),
                };
                skipped = true;
                break;
            }
        }
        if skipped {
            continue;
        }

        let rest = &html[i..];
        if rest.starts_with('<') {
            let Some(close) = rest.find('>') else { break };
            let raw_tag = &rest[1..close];
            i += close + 1;
            let closing = raw_tag.starts_with('/');
            let name: String = raw_tag
                .trim_start_matches('/')
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric())
                .collect::<String>()
                .to_ascii_lowercase();

            match name.as_str() {
                "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => {
                    push_break(&mut out, 2);
                    if !closing && markdown {
                        let level = name[1..].parse::<usize>().unwrap_or(1);
                        out.push_str(&"#".repeat(level));
                        out.push(' ');
                    }
                }
                "p" | "div" | "section" | "article" | "header" | "footer" | "main" | "ul"
                | "ol" | "table" | "blockquote" => push_break(&mut out, 2),
                "br" | "tr" => push_break(&mut out, 1),
                "li" => {
                    push_break(&mut out, 1);
                    if !closing && markdown {
                        out.push_str("- ");
                    }
                }
                "pre" => {
                    if closing {
                        pre_depth = pre_depth.saturating_sub(1);
                        if markdown {
                            push_break(&mut out, 1);
                            out.push_str("```");
                        }
                        push_break(&mut out, 2);
                    } else {
                        push_break(&mut out, 2);
                        pre_depth += 1;
                        if markdown {
                            out.push_str("```\n");
                        }
                    }
                }
                "code" if markdown && pre_depth == 0 => out.push('`'),
                "a" if markdown => {
                    if closing {
                        // An anchor without an href never opened a bracket, so
                        // only a `Some(href)` frame closes one.
                        if let Some(Some(href)) = links.pop() {
                            out.push_str(&format!("]({href})"));
                        }
                    } else {
                        let href = attr(raw_tag, "href");
                        if href.is_some() {
                            out.push('[');
                        }
                        links.push(href);
                    }
                }
                _ => {}
            }
            continue;
        }

        let ch = rest.chars().next().unwrap();
        if ch == '&'
            && let Some((decoded, len)) = decode_entity(rest)
        {
            push_text(&mut out, decoded, pre_depth > 0);
            i += len;
            continue;
        }
        push_text(&mut out, ch, pre_depth > 0);
        i += ch.len_utf8();
    }

    out.trim().to_string()
}

/// Emit one character, collapsing runs of whitespace outside `<pre>` so a
/// prettily-indented document doesn't arrive as a field of spaces.
fn push_text(out: &mut String, ch: char, verbatim: bool) {
    if verbatim {
        out.push(ch);
        return;
    }
    if ch.is_whitespace() {
        if !out.is_empty() && !out.ends_with([' ', '\n']) {
            out.push(' ');
        }
        return;
    }
    out.push(ch);
}

/// Make the output end in exactly `n` newlines — idempotent, so nested block
/// tags (`<div><p>`) produce one paragraph break rather than four.
fn push_break(out: &mut String, n: usize) {
    if out.is_empty() {
        return;
    }
    while out.ends_with([' ', '\n']) {
        out.pop();
    }
    if out.is_empty() {
        return;
    }
    for _ in 0..n {
        out.push('\n');
    }
}

/// The value of an attribute in a raw tag body, quoted or not.
fn attr(raw_tag: &str, name: &str) -> Option<String> {
    let lower = raw_tag.to_ascii_lowercase();
    let mut from = 0;
    // Scan every occurrence: `data-href` must not match `href`.
    while let Some(rel) = lower[from..].find(name) {
        let at = from + rel;
        let before_ok = at == 0 || lower.as_bytes()[at - 1].is_ascii_whitespace();
        let rest = raw_tag[at + name.len()..].trim_start();
        from = at + name.len();
        if !before_ok {
            continue;
        }
        let Some(value) = rest.strip_prefix('=') else {
            continue;
        };
        let value = value.trim_start();
        let quoted = value.starts_with(['"', '\'']);
        let value = if quoted {
            let quote = value.chars().next().unwrap();
            value[1..].split(quote).next().unwrap_or_default()
        } else {
            // Unquoted: ends at whitespace, or at the tag's own `/`/`>`.
            value
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .trim_end_matches(['/', '>'])
        };
        if value.is_empty() {
            return None;
        }
        return Some(value.to_string());
    }
    None
}

/// Decode a leading character entity, returning it and how many bytes it spanned.
/// Only the ones that change meaning — an undecoded `&amp;` in a URL the model
/// then fetches is a broken request, while an undecoded `&hellip;` is cosmetic.
fn decode_entity(rest: &str) -> Option<(char, usize)> {
    const NAMED: &[(&str, char)] = &[
        ("&amp;", '&'),
        ("&lt;", '<'),
        ("&gt;", '>'),
        ("&quot;", '"'),
        ("&apos;", '\''),
        ("&#39;", '\''),
        ("&nbsp;", ' '),
    ];
    NAMED
        .iter()
        .find(|(entity, _)| rest.starts_with(entity))
        .map(|(entity, ch)| (*ch, entity.len()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use komo_core::domain::approval::{Approver, Decision};
    use komo_core::domain::context::{SessionContext, ToolContext};
    use std::sync::Arc;

    struct DenyAll;
    #[async_trait]
    impl Approver for DenyAll {
        async fn decide(&self, _request: &ApprovalRequest) -> Decision {
            Decision::deny()
        }
    }

    fn deny_ctx() -> ToolContext {
        ToolContext::new(
            SessionContext::detached("cli:test"),
            None,
            Arc::new(DenyAll),
        )
    }

    #[tokio::test]
    async fn denied_fetch_reports_the_block_and_sends_nothing() {
        let tool = WebFetchTool::new();
        let out = tool
            .call(
                json!({ "url": "https://blocked.example.com/x" }),
                &deny_ctx(),
            )
            .await
            .unwrap();
        assert!(out.text.contains("blocked by the permission policy"));
        assert!(out.text.contains("blocked.example.com"));
    }

    #[test]
    fn redact_masks_header_values_but_keeps_names_and_url() {
        let tool = WebFetchTool::new();
        let args = json!({
            "url": "http://miniflux:8080/v1/entries",
            "headers": { "X-Auth-Token": "super-secret-token" }
        })
        .to_string();
        let redacted = tool.redact_args(&args);
        assert!(!redacted.contains("super-secret-token"));
        assert!(redacted.contains("X-Auth-Token"));
        assert!(redacted.contains("miniflux:8080"));
    }

    #[tokio::test]
    async fn too_many_headers_is_an_error() {
        let tool = WebFetchTool::new();
        let headers: std::collections::HashMap<String, String> = (0..9)
            .map(|i| (format!("H-{i}"), "v".to_string()))
            .collect();
        let err = tool
            .call(
                json!({ "url": "https://example.com", "headers": headers }),
                &deny_ctx(),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("too many headers"));
    }

    #[test]
    fn strips_tags_scripts_and_styles() {
        let html = "<html><head><style>a{}</style></head><body><script>var x=1;</script>\
            <h1>Hello</h1><p>World &amp; more</p></body></html>";
        let text = render_html(html, Format::Text);
        assert!(text.contains("Hello"));
        assert!(text.contains("World & more"));
        assert!(!text.contains("var x"));
        assert!(!text.contains("a{}"));
        // Plain text carries no structure markers.
        assert!(!text.contains('#'));
    }

    /// The point of 07: a page arrives as readable markdown, not one collapsed
    /// line where every heading, bullet and link target has been thrown away.
    #[test]
    fn markdown_keeps_headings_lists_code_and_links() {
        let html = "<h2>Setup</h2><p>Install it:</p><pre>cargo add komo\n  --features x</pre>\
            <ul><li>first</li><li>second</li></ul>\
            <p>See <a class=\"x\" href=\"https://example.com/docs\">the docs</a> and \
            <code>komo init</code>.</p>";
        let md = render_html(html, Format::Markdown);

        assert!(md.contains("## Setup"), "{md}");
        assert!(md.contains("- first\n- second"), "{md}");
        assert!(md.contains("[the docs](https://example.com/docs)"), "{md}");
        assert!(md.contains("`komo init`"), "{md}");
        // Code block fenced, and its indentation preserved verbatim.
        assert!(md.contains("```\ncargo add komo\n  --features x"), "{md}");
    }

    #[test]
    fn html_format_is_returned_untouched() {
        let src = "<h1>T</h1><script>x</script>";
        assert_eq!(render_html(src, Format::Html), src);
    }

    #[test]
    fn nested_blocks_collapse_to_one_paragraph_break() {
        let md = render_html("<div><p>a</p></div><div><p>b</p></div>", Format::Markdown);
        assert_eq!(md, "a\n\nb");
    }

    #[test]
    fn anchor_without_href_does_not_emit_a_dangling_bracket() {
        let md = render_html("<p>see <a name=\"top\">top</a> now</p>", Format::Markdown);
        assert_eq!(md, "see top now");
    }

    #[test]
    fn attr_matches_whole_names_only() {
        assert_eq!(
            attr("a data-href='x' href=\"y\"", "href").as_deref(),
            Some("y")
        );
        assert_eq!(attr("a href=plain>", "href").as_deref(), Some("plain"));
        assert_eq!(attr("a name='top'", "href"), None);
    }

    #[test]
    fn content_type_gates_binaries_but_admits_svg_and_json() {
        assert!(is_image("image/png"));
        assert!(!is_image("image/svg+xml"));
        assert!(is_textual("application/json"));
        assert!(is_textual("application/vnd.api+json"));
        assert!(is_textual(""));
        assert!(!is_textual("application/pdf"));
        assert!(!is_textual("application/octet-stream"));
        assert_eq!(mime_of("text/HTML; charset=utf-8"), "text/html");
    }

    #[tokio::test]
    async fn non_http_schemes_are_rejected_before_any_request() {
        let tool = WebFetchTool::new();
        for url in ["file:///etc/passwd", "ftp://example.com/x", "example.com"] {
            let err = tool
                .call(json!({ "url": url }), &deny_ctx())
                .await
                .unwrap_err();
            assert!(err.to_string().contains("http"), "{url}: {err}");
        }
    }

    /// Serve one canned response on loopback and return its URL. Raw HTTP so the
    /// test controls the exact headers under test (`content-type`,
    /// `content-length`) — the whole point of the gate is what we do with those.
    async fn serve_once(
        content_type: &str,
        body: &'static str,
        content_length: Option<usize>,
    ) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let head = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\n\
             connection: close\r\n\r\n",
            content_length.unwrap_or(body.len())
        );
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let _ = socket.write_all(head.as_bytes()).await;
            let _ = socket.write_all(body.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
        url
    }

    #[tokio::test]
    async fn a_pdf_response_is_refused_instead_of_decoded() {
        let url = serve_once("application/pdf", "%PDF-1.7\n\u{1}\u{2}garbage", None).await;
        let err = WebFetchTool::new()
            .call(
                json!({ "url": url }),
                &crate::test_support::approving_ctx("cli:test"),
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("Unsupported fetched file content type: application/pdf"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn an_html_page_comes_back_as_markdown_or_raw_by_format() {
        const PAGE: &str =
            "<html><body><h1>Title</h1><p>Read <a href=\"/x\">this</a>.</p></body></html>";

        let url = serve_once("text/html; charset=utf-8", PAGE, None).await;
        let out = WebFetchTool::new()
            .call(
                json!({ "url": url }),
                &crate::test_support::approving_ctx("cli:test"),
            )
            .await
            .unwrap();
        assert!(out.text.contains("# Title"), "{}", out.text);
        assert!(out.text.contains("[this](/x)"), "{}", out.text);
        assert_eq!(out.structured["format"], "markdown");

        let url = serve_once("text/html", PAGE, None).await;
        let out = WebFetchTool::new()
            .call(
                json!({ "url": url, "format": "html" }),
                &crate::test_support::approving_ctx("cli:test"),
            )
            .await
            .unwrap();
        assert!(out.text.contains("<h1>Title</h1>"), "{}", out.text);
    }

    #[tokio::test]
    async fn a_declared_length_over_the_ceiling_fails_before_the_body() {
        let url = serve_once("text/plain", "x", Some(MAX_RESPONSE_BYTES + 1)).await;
        let err = WebFetchTool::new()
            .call(
                json!({ "url": url }),
                &crate::test_support::approving_ctx("cli:test"),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("response too large"), "{err}");
    }

    #[test]
    fn format_defaults_to_markdown_and_parses_the_other_two() {
        let args: FetchArgs = serde_json::from_value(json!({ "url": "https://x" })).unwrap();
        assert_eq!(args.format, Format::Markdown);
        let args: FetchArgs =
            serde_json::from_value(json!({ "url": "https://x", "format": "html" })).unwrap();
        assert_eq!(args.format, Format::Html);
        assert!(args.format.accept().starts_with("text/html"));
        assert!(
            serde_json::from_value::<FetchArgs>(json!({ "url": "https://x", "format": "pdf" }))
                .is_err()
        );
    }
}
