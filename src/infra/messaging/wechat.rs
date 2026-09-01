//! WeChat (微信) ingress channel, over the iLink personal-bot protocol.
//!
//! Built on the `wechatbot` crate (the iLink Bot API: HTTP/JSON long-polling
//! against `ilinkai.weixin.qq.com`, no public callback URL — like telegram's
//! `getUpdates`). Unlike telegram/feishu, the crate owns its own poll loop
//! (`WeChatBot::run`) and fires a **synchronous** `on_message` callback, so the
//! channel adapts rather than drives: it registers a handler that clones the
//! message and `tokio::spawn`s the async pairing + dispatch, then hands the
//! thread to `run()` under a shutdown `select!`.
//!
//! Login is QR-based. Two ways to provision the credentials
//! (`~/.komo/wechat/credentials.json`):
//!   - on a host with a terminal: `komo wechat login` (renders the QR in-term);
//!   - from chat: `/wechat login` on an already-working channel (e.g. Telegram)
//!     drives [`WeChatQrLogin`], which delivers the QR back as a photo. This is
//!     how a headless gateway (TrueNAS/Docker) is set up without shell access.
//!
//! The channel **waits** for those credentials instead of dying without them:
//! `serve` blocks on a [`Notify`] that `WeChatQrLogin` pulses on a successful
//! login, so a freshly-provisioned account comes online with no restart.
//!
//! Proactive sends (`HomeNotifier`) need a `context_token`, only remembered
//! after a user has messaged the bot since process start (kept in memory, not
//! on disk) — which is why [`WeChatSender`] and [`WeChatChannel`] share **one**
//! bot instance: the poll loop populates the token map the sender reads.
//!
//! DM-focused by design: an iLink bot identity generally can't be invited into
//! ordinary WeChat groups, so there is no group/mention gate — the
//! `PairingGuard` is the only admission control.

use komo_agent::gateway::Channel;
use komo_agent::interaction::GatewayDispatcher;
use komo_agent::pairing::PairingGuard;
use komo_core::domain::inbox::InboundOrigin;
use komo_core::domain::session::ChannelPeer;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use qrcode::{Color, QrCode};
use tokio::sync::{Notify, watch};
use tracing::{error, info, warn};
use wechatbot::{BotOptions, WeChatBot};

use crate::{
    domain::{
        gateway::{ReplySink, WeChatLogin},
        pairing::PairingRepository,
    },
    infra::messaging::home_notifier::TextSender,
};
use komo_config::WeChatConfig;

/// Backoff between poll-loop restarts (e.g. after a login or session error).
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

/// Construct the shared bot. Created once at wiring time and handed to *both*
/// the sender and the channel so they share the in-memory `context_token` map
/// (see the module docs on proactive sends).
pub fn build_bot(cred_path: &std::path::Path) -> Arc<WeChatBot> {
    Arc::new(WeChatBot::new(BotOptions {
        cred_path: Some(cred_path.to_string_lossy().into_owned()),
        on_qr_url: Some(Box::new(|_| {
            warn!(
                "wechat: QR login required but the gateway has no terminal — \
                 run `komo wechat login` on the host, or `/wechat login` from chat"
            );
        })),
        on_error: Some(Box::new(|e| warn!(error = %e, "wechat bot error"))),
        ..Default::default()
    }))
}

/// Render a QR payload to a scannable PNG (scaled up, with a quiet border).
/// Uses the `image` crate's PNG codec only — no qrcode `image` feature, so we
/// avoid pulling the heavy image-format/color-management deps.
pub fn render_qr_png(content: &str) -> anyhow::Result<Vec<u8>> {
    use image::{DynamicImage, GrayImage, ImageFormat, Luma};

    const SCALE: usize = 8;
    const QUIET: usize = 4;

    let code = QrCode::new(content.as_bytes())?;
    let colors = code.to_colors();
    let w = code.width();
    let dim = ((w + 2 * QUIET) * SCALE) as u32;

    let mut img = GrayImage::from_pixel(dim, dim, Luma([255]));
    for y in 0..w {
        for x in 0..w {
            if colors[y * w + x] != Color::Light {
                let (px0, py0) = ((x + QUIET) * SCALE, (y + QUIET) * SCALE);
                for dy in 0..SCALE {
                    for dx in 0..SCALE {
                        img.put_pixel((px0 + dx) as u32, (py0 + dy) as u32, Luma([0]));
                    }
                }
            }
        }
    }

    let mut buf = std::io::Cursor::new(Vec::new());
    DynamicImage::ImageLuma8(img).write_to(&mut buf, ImageFormat::Png)?;
    Ok(buf.into_inner())
}

/// Drives an interactive QR login and delivers the QR to the requesting chat as
/// a photo. Shares its `ready` signal with the [`WeChatChannel`] so a
/// successful login wakes the waiting poll loop — no gateway restart needed.
pub struct WeChatQrLogin {
    cred_path: PathBuf,
    ready: Arc<Notify>,
    poll_bot: Arc<WeChatBot>,
    provisioning: Arc<AtomicBool>,
}

impl WeChatQrLogin {
    pub fn new(
        cred_path: PathBuf,
        ready: Arc<Notify>,
        poll_bot: Arc<WeChatBot>,
        provisioning: Arc<AtomicBool>,
    ) -> Self {
        Self {
            cred_path,
            ready,
            poll_bot,
            provisioning,
        }
    }
}

struct ProvisioningGuard {
    active: Arc<AtomicBool>,
    ready: Arc<Notify>,
}

impl Drop for ProvisioningGuard {
    fn drop(&mut self) {
        self.active.store(false, Ordering::SeqCst);
        self.ready.notify_waiters();
    }
}

#[async_trait]
impl WeChatLogin for WeChatQrLogin {
    async fn run(&self, sink: Arc<dyn ReplySink>) -> anyhow::Result<String> {
        if self
            .provisioning
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            anyhow::bail!("wechat login already in progress");
        }
        let _provisioning = ProvisioningGuard {
            active: self.provisioning.clone(),
            ready: self.ready.clone(),
        };
        self.ready.notify_waiters();

        // Stop the shared polling bot before starting a manual QR login. The
        // wechatbot crate force-starts its own QR login on session expiry; this
        // nudges the outer channel loop to reload the credentials written here.
        self.poll_bot.stop().await;

        // A throwaway bot just for login: it writes creds to the shared path,
        // which the channel's serve loop is waiting on. Its QR callback ships
        // the code to the chat as a photo.
        let qr_sink = sink.clone();
        let bot = WeChatBot::new(BotOptions {
            cred_path: Some(self.cred_path.to_string_lossy().into_owned()),
            on_qr_url: Some(Box::new(move |content| {
                let sink = qr_sink.clone();
                match render_qr_png(content) {
                    Ok(png) => {
                        tokio::spawn(async move {
                            if let Err(error) = sink.send_photo(png, "用微信扫码登录 komo").await
                            {
                                let _ =
                                    sink.send(&format!("二维码已生成但发送失败：{error}")).await;
                            }
                        });
                    }
                    Err(error) => {
                        tokio::spawn(async move {
                            let _ = sink.send(&format!("二维码渲染失败：{error}")).await;
                        });
                    }
                }
            })),
            on_error: Some(Box::new(|e| warn!(error = %e, "wechat login error"))),
            ..Default::default()
        });

        // Explicit login should re-provision even when stale creds exist.
        let creds = bot
            .login(true)
            .await
            .map_err(|e| anyhow::anyhow!("wechat login: {e}"))?;
        // Wake the channel's serve loop so it starts polling immediately.
        self.ready.notify_one();
        Ok(creds.user_id)
    }
}

/// Outbound side, shared by the channel (replies) and the `HomeNotifier`
/// (proactive output). Both go through `WeChatBot::send`, so both are subject to
/// the prior-inbound-message constraint described in the module docs.
pub struct WeChatSender {
    bot: Arc<WeChatBot>,
}

impl WeChatSender {
    pub fn new(bot: Arc<WeChatBot>) -> Self {
        Self { bot }
    }
}

#[async_trait]
impl TextSender for WeChatSender {
    async fn send_text(&self, chat_id: &str, text: &str) -> anyhow::Result<()> {
        self.bot
            .send(chat_id, text)
            .await
            .map_err(|e| anyhow::anyhow!("wechat send: {e}"))
    }
}

/// Sends a turn's output (and any mid-turn approval prompts) back to one user.
struct WeChatReplySink {
    bot: Arc<WeChatBot>,
    user_id: String,
}

#[async_trait]
impl ReplySink for WeChatReplySink {
    async fn send(&self, text: &str) -> anyhow::Result<()> {
        self.bot
            .send(&self.user_id, text)
            .await
            .map_err(|e| anyhow::anyhow!("wechat send: {e}"))
    }
}

pub struct WeChatChannel {
    bot: Arc<WeChatBot>,
    guard: Arc<PairingGuard>,
    cred_path: PathBuf,
    ready: Arc<Notify>,
    provisioning: Arc<AtomicBool>,
}

impl WeChatChannel {
    pub fn new(
        bot: Arc<WeChatBot>,
        config: &WeChatConfig,
        cred_path: PathBuf,
        ready: Arc<Notify>,
        provisioning: Arc<AtomicBool>,
        pairings: Arc<dyn PairingRepository>,
    ) -> Self {
        Self {
            bot,
            guard: Arc::new(PairingGuard::new(
                "wechat",
                config.allow_from.clone(),
                pairings,
            )),
            cred_path,
            ready,
            provisioning,
        }
    }

    /// Register the inbound handler on the shared bot (once). The crate's
    /// `on_message` is a sync callback fired from inside `run()`, so we clone
    /// owned data out of the borrowed message and `tokio::spawn` the async work
    /// (pairing + dispatch) — keeping the poll loop flowing and letting an
    /// `/approve` reply land mid-turn, like the other channels.
    async fn register_handler(&self, dispatcher: Arc<GatewayDispatcher>) {
        let guard = self.guard.clone();
        let bot = self.bot.clone();
        self.bot
            .on_message(Box::new(move |msg| {
                let text = msg.text.trim().to_string();
                if text.is_empty() {
                    return;
                }
                let user_id = msg.user_id.clone();
                // iLink exposes no message id on the parsed message, but the
                // wire payload carries both fields below, and both come from
                // the sender rather than from local time — so a redelivery of
                // the same message reproduces the same pair.
                let message_key = format!("{}:{}", msg.raw.client_id, msg.raw.create_time_ms);
                let dispatcher = dispatcher.clone();
                let guard = guard.clone();
                let bot = bot.clone();
                tokio::spawn(async move {
                    // DM-only: sender and chat are the same iLink user id.
                    let reply_bot = bot.clone();
                    let reply_user = user_id.clone();
                    let admitted = guard
                        .admit(&user_id, &user_id, move |reply| async move {
                            reply_bot
                                .send(&reply_user, &reply)
                                .await
                                .map_err(|e| anyhow::anyhow!("{e}"))
                        })
                        .await;
                    if !admitted {
                        return;
                    }
                    info!(user = %user_id, "wechat message received");
                    let sink: Arc<dyn ReplySink> = Arc::new(WeChatReplySink {
                        bot: bot.clone(),
                        user_id: user_id.clone(),
                    });
                    let origin = InboundOrigin::new("wechat", message_key);
                    dispatcher
                        .handle(&ChannelPeer::new("wechat", user_id), origin, text, sink)
                        .await;
                });
            }))
            .await;
    }
}

#[async_trait]
impl Channel for WeChatChannel {
    fn name(&self) -> &str {
        "wechat"
    }

    async fn serve(
        &self,
        dispatcher: Arc<GatewayDispatcher>,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        self.register_handler(dispatcher).await;

        loop {
            // Wait until credentials exist. They may be provisioned later via
            // `/wechat login` (which pulses `ready`) or `komo wechat login`.
            while !self.cred_path.exists() || self.provisioning.load(Ordering::SeqCst) {
                if self.provisioning.load(Ordering::SeqCst) {
                    info!("wechat channel: waiting for manual QR login to finish");
                } else {
                    info!(
                        "wechat channel: waiting for credentials — run `/wechat login` from chat \
                         or `komo wechat login` on the host"
                    );
                }
                tokio::select! {
                    _ = shutdown.changed() => return Ok(()),
                    _ = self.ready.notified() => {}
                }
            }

            // Creds exist → load them without a QR prompt, then poll.
            if let Err(error) = self.bot.login(false).await {
                error!(%error, "wechat login failed; retrying");
                tokio::select! {
                    _ = shutdown.changed() => return Ok(()),
                    _ = tokio::time::sleep(RECONNECT_DELAY) => {}
                }
                continue;
            }
            info!("wechat channel connected");

            // `run()` owns the long-poll loop; dropping its future on shutdown
            // cancels the in-flight poll cleanly.
            tokio::select! {
                _ = shutdown.changed() => return Ok(()),
                _ = self.ready.notified() => {
                    if self.provisioning.load(Ordering::SeqCst) {
                        self.bot.stop().await;
                    }
                }
                result = self.bot.run() => {
                    if let Err(error) = result {
                        error!(%error, "wechat poll loop stopped; will retry");
                    }
                }
            }
            tokio::select! {
                _ = shutdown.changed() => return Ok(()),
                _ = tokio::time::sleep(RECONNECT_DELAY) => {}
            }
        }
    }
}
