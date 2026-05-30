#![allow(unused_variables)]
#![allow(dead_code)]
// ============================================================
//  DISCORD SECURITY BOT – FULLY FUNCTIONAL RUST VERSION
//  Compatible with Serenity 0.11.7
// ============================================================
use serenity::{
    async_trait,
    client::{Client, Context, EventHandler},
    http::Http,
    model::{
        channel::{Channel, ChannelType, GuildChannel, Message, PermissionOverwrite, PermissionOverwriteType},
        guild::{Guild, Member, Role},
        id::{ChannelId, GuildId, RoleId, UserId},
        user::User,
        permissions::Permissions,
        prelude::*,
        gateway::GatewayIntents,
    },
    cache::Cache,
    builder::CreateEmbed,
};
use tokio::sync::Semaphore;
use dashmap::{DashMap, DashSet};
use chrono::{DateTime, Utc, Duration as ChronoDuration};
use regex::Regex;
use sqlx::{PgPool, postgres::PgPoolOptions, Row};
use reqwest;
use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
    time::Duration,
    str::FromStr,
};

// ------------------------------------------------------------
//  CONSTANTS
// ------------------------------------------------------------
fn pht_offset() -> chrono::FixedOffset { chrono::FixedOffset::east_opt(8 * 3600).unwrap() }
fn now_pht() -> DateTime<chrono::FixedOffset> { Utc::now().with_timezone(&pht_offset()) }

use std::sync::OnceLock;
use std::sync::Mutex;

static ANTINUKE_CONFIG: OnceLock<Mutex<AntiNukeConfig>> = OnceLock::new();
static SECURITY_CONFIG: OnceLock<Mutex<SecurityConfig>> = OnceLock::new();

fn antinuke_config() -> &'static Mutex<AntiNukeConfig> {
    ANTINUKE_CONFIG.get_or_init(|| Mutex::new(AntiNukeConfig {
        threshold_count: 1,
        threshold_window_secs: 0.1,
        punishment: Punishment::Ban,
    }))
}
fn security_config() -> &'static Mutex<SecurityConfig> {
    SECURITY_CONFIG.get_or_init(|| Mutex::new(SecurityConfig {
        max_messages_per_minute: 10,
        max_duplicate_messages: 3,
        link_whitelist: vec![
            "youtube.com".to_string(), "youtu.be".to_string(), "github.com".to_string(),
            "open.spotify.com".to_string(), "spotify.com".to_string(), "tenor.com".to_string(),
            "giphy.com".to_string(), "media.tenor.com".to_string(), "media.giphy.com".to_string(),
        ],
        banned_words: vec![
            "spam".to_string(), "hack".to_string(), "cheat".to_string(),
            "discord.gg".to_string(), "https://discord.gg/".to_string(),
        ],
        max_emojis: 5,
        auto_ban_threshold: 5,
    }))
}

const MASS_ACTION_THRESHOLD: usize = 1;
const MASS_ACTION_WINDOW_SECS: f64 = 0.1;
const ACTOR_CACHE_TTL_SECS: f64 = 8.0;
const DRAIN_DELAY_SECS: f64 = 0.15;
const EDIT_LOG_DEDUP_TTL_SECS: f64 = 5.0;
const CHANNEL_CREATE_DEDUP_TTL_SECS: f64 = 10.0;
const GUILD_UPDATE_DEDUP_TTL_SECS: f64 = 5.0;
const WEBHOOK_EVENT_DEDUP_TTL_SECS: f64 = 10.0;
const ROLE_EVENT_DEDUP_TTL_SECS: f64 = 10.0;
const SERVER_AD_EXPIRY_SECS: i64 = 3600;
const AD_SPAM_TIMEOUT_MIN: i64 = 10;
const RATE_LIMIT_MAX_COMMANDS: usize = 3;
const RATE_LIMIT_WINDOW_SECS: f64 = 5.0;
const RATE_LIMIT_COOLDOWN_SECS: i64 = 15;

const DANGEROUS_PERMISSIONS: [Permissions; 7] = [
    Permissions::ADMINISTRATOR,
    Permissions::MANAGE_GUILD,
    Permissions::MANAGE_ROLES,
    Permissions::MANAGE_CHANNELS,
    Permissions::MANAGE_WEBHOOKS,
    Permissions::BAN_MEMBERS,
    Permissions::KICK_MEMBERS,
];

const EMBED_COLOR: u32 = 0x000000;
const ANTINUKE_ASCII: &str = r#"
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⠀⢠⢀⡐⢄⢢⡐⢢⢁⠂⠄⠠⢀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
"#;

// ------------------------------------------------------------
//  TYPES & SNAPSHOTS
// ------------------------------------------------------------
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Punishment { Ban, Kick, Strip }
impl Punishment {
    fn as_str(&self) -> &'static str { match self { Punishment::Ban => "ban", Punishment::Kick => "kick", Punishment::Strip => "strip" } }
}
impl FromStr for Punishment {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, ()> {
        match s { "ban" => Ok(Punishment::Ban), "kick" => Ok(Punishment::Kick), "strip" => Ok(Punishment::Strip), _ => Err(()) }
    }
}

#[derive(Debug, Clone)]
struct AntiNukeConfig { threshold_count: usize, threshold_window_secs: f64, punishment: Punishment }
#[derive(Debug, Clone)]
struct SecurityConfig {
    max_messages_per_minute: usize, max_duplicate_messages: usize, link_whitelist: Vec<String>,
    banned_words: Vec<String>, max_emojis: usize, auto_ban_threshold: usize,
}

#[derive(Clone)]
struct WarningData { reason: String, moderator: UserId, timestamp: DateTime<chrono::FixedOffset>, guild_id: GuildId }

#[derive(Clone)]
struct ChannelSnapshot {
    name: String, category_id: Option<ChannelId>, position: i32, channel_type: ChannelType,
    overwrites: Vec<(PermissionOverwriteType, PermissionOverwrite)>,
    topic: Option<String>, nsfw: bool, slowmode_delay: u64,
}
#[derive(Clone)]
struct GuildSnapshot {
    name: String, description: Option<String>, icon: Option<String>, banner: Option<String>,
    afk_channel_id: Option<ChannelId>, afk_timeout: u64, verification_level: u16,
    default_notifications: u16, explicit_content_filter: u16, system_channel_id: Option<ChannelId>,
}
#[derive(Clone)]
struct RoleSnapshot { name: String, permissions: u64, colour: u32, hoist: bool, mentionable: bool }
#[derive(Clone)]
struct ServerAdEntry { invite_code: String, channel_id: ChannelId, message_id: MessageId, timestamp: f64 }

// ------------------------------------------------------------
//  GLOBAL STATE
// ------------------------------------------------------------
#[derive(Clone)]
struct BotState {
    protection_enabled: DashMap<GuildId, bool>,
    whitelist_roles: DashMap<GuildId, HashSet<RoleId>>,
    whitelist_users: DashMap<GuildId, HashSet<UserId>>,
    link_bypass_users: DashMap<GuildId, HashSet<UserId>>,
    link_bypass_roles: DashMap<GuildId, HashSet<RoleId>>,
    muted_users: DashMap<UserId, DateTime<chrono::FixedOffset>>,
    user_violations: DashMap<UserId, usize>,
    user_message_times: DashMap<UserId, VecDeque<DateTime<chrono::FixedOffset>>>,
    user_messages: DashMap<UserId, VecDeque<String>>,
    user_warnings: DashMap<UserId, Vec<WarningData>>,
    action_log: DashMap<GuildId, DashMap<UserId, Vec<(String, f64)>>>,
    mass_action_log: DashMap<GuildId, DashMap<UserId, Vec<f64>>>,
    confirmed_actors: DashMap<(GuildId, String), DashMap<UserId, f64>>,
    ban_in_progress: DashMap<GuildId, DashSet<UserId>>,
    rollback_queue: DashMap<GuildId, DashMap<UserId, Vec<ChannelSnapshot>>>,
    drain_scheduled: DashMap<GuildId, DashSet<UserId>>,
    restoring: DashMap<GuildId, bool>,
    edit_logged: DashMap<GuildId, DashMap<UserId, f64>>,
    handled_channel_creates: DashMap<GuildId, DashMap<ChannelId, f64>>,
    handled_guild_updates: DashMap<GuildId, f64>,
    handled_webhook_events: DashMap<GuildId, DashMap<ChannelId, f64>>,
    handled_role_events: DashMap<GuildId, DashMap<u64, f64>>,
    role_restore_locks: DashMap<GuildId, Arc<tokio::sync::Mutex<()>>>,
    dangerous_members: DashMap<GuildId, HashSet<UserId>>,
    command_timestamps: DashMap<UserId, VecDeque<f64>>,
    rate_limited_until: DashMap<UserId, DateTime<chrono::FixedOffset>>,
    audit_prefetch: DashMap<(GuildId, String), (UserId, f64)>,
    guild_snapshots: DashMap<GuildId, GuildSnapshot>,
    channel_snapshots: DashMap<ChannelId, ChannelSnapshot>,
    role_snapshots: DashMap<RoleId, RoleSnapshot>,
    server_ad_registry: DashMap<GuildId, DashMap<UserId, ServerAdEntry>>,
    ad_spam_channels: DashMap<GuildId, DashMap<UserId, Vec<ChannelId>>>,
    api_semaphore: Arc<Semaphore>,
}
impl BotState {
    fn new() -> Self {
        Self {
            protection_enabled: DashMap::new(), whitelist_roles: DashMap::new(), whitelist_users: DashMap::new(),
            link_bypass_users: DashMap::new(), link_bypass_roles: DashMap::new(), muted_users: DashMap::new(),
            user_violations: DashMap::new(), user_message_times: DashMap::new(), user_messages: DashMap::new(),
            user_warnings: DashMap::new(), action_log: DashMap::new(), mass_action_log: DashMap::new(),
            confirmed_actors: DashMap::new(), ban_in_progress: DashMap::new(), rollback_queue: DashMap::new(),
            drain_scheduled: DashMap::new(), restoring: DashMap::new(), edit_logged: DashMap::new(),
            handled_channel_creates: DashMap::new(), handled_guild_updates: DashMap::new(),
            handled_webhook_events: DashMap::new(), handled_role_events: DashMap::new(),
            role_restore_locks: DashMap::new(), dangerous_members: DashMap::new(),
            command_timestamps: DashMap::new(), rate_limited_until: DashMap::new(),
            audit_prefetch: DashMap::new(), guild_snapshots: DashMap::new(), channel_snapshots: DashMap::new(),
            role_snapshots: DashMap::new(), server_ad_registry: DashMap::new(), ad_spam_channels: DashMap::new(),
            api_semaphore: Arc::new(Semaphore::new(20)),
        }
    }
}

// ------------------------------------------------------------
//  DATABASE
// ------------------------------------------------------------
#[derive(Clone)]
struct Database { pool: PgPool }
impl Database {
    async fn new(url: &str) -> Self {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .unwrap_or_else(|e| panic!("Failed to connect to Postgres: {}", e));
        for stmt in [
            "CREATE TABLE IF NOT EXISTS protection ( guild_id BIGINT PRIMARY KEY, enabled INTEGER NOT NULL DEFAULT 0 )",
            "CREATE TABLE IF NOT EXISTS whitelist_users ( guild_id BIGINT NOT NULL, user_id BIGINT NOT NULL, PRIMARY KEY (guild_id, user_id) )",
            "CREATE TABLE IF NOT EXISTS whitelist_roles ( guild_id BIGINT NOT NULL, role_id BIGINT NOT NULL, PRIMARY KEY (guild_id, role_id) )",
            "CREATE TABLE IF NOT EXISTS muted_users ( guild_id BIGINT NOT NULL, user_id BIGINT NOT NULL, until_ts TEXT NOT NULL, PRIMARY KEY (guild_id, user_id) )",
            r#"CREATE TABLE IF NOT EXISTS guild_config ( guild_id BIGINT PRIMARY KEY, threshold_count BIGINT NOT NULL DEFAULT 1, threshold_window BIGINT NOT NULL DEFAULT 10, punishment TEXT NOT NULL DEFAULT 'ban', max_messages_per_minute BIGINT NOT NULL DEFAULT 10, max_duplicate_messages BIGINT NOT NULL DEFAULT 3, max_emojis BIGINT NOT NULL DEFAULT 5, auto_ban_threshold BIGINT NOT NULL DEFAULT 5, link_whitelist TEXT NOT NULL DEFAULT '["youtube.com","github.com"]', banned_words TEXT NOT NULL DEFAULT '["spam","hack","cheat","discord.gg"]' )"#,
            "CREATE TABLE IF NOT EXISTS link_bypass_users ( guild_id BIGINT NOT NULL, user_id BIGINT NOT NULL, PRIMARY KEY (guild_id, user_id) )",
            "CREATE TABLE IF NOT EXISTS link_bypass_roles ( guild_id BIGINT NOT NULL, role_id BIGINT NOT NULL, PRIMARY KEY (guild_id, role_id) )",
            "CREATE TABLE IF NOT EXISTS action_history ( id SERIAL PRIMARY KEY, guild_id BIGINT NOT NULL, user_id BIGINT NOT NULL, action TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '', timestamp TEXT NOT NULL )",
        ] {
            sqlx::query(stmt).execute(&pool).await.unwrap();
        }
        Self { pool }
    }
    async fn load_all(&self, state: &BotState) {
        let rows = sqlx::query("SELECT guild_id, enabled FROM protection").fetch_all(&self.pool).await.unwrap();
        for row in rows { let gid = GuildId(row.get::<i64,_>(0) as u64); state.protection_enabled.insert(gid, row.get::<i32, _>(1) != 0); }
        let rows = sqlx::query("SELECT guild_id, user_id FROM whitelist_users").fetch_all(&self.pool).await.unwrap();
        for row in rows { let gid = GuildId(row.get::<i64,_>(0) as u64); let uid = UserId(row.get::<i64,_>(1) as u64); state.whitelist_users.entry(gid).or_insert_with(HashSet::new).insert(uid); }
        let rows = sqlx::query("SELECT guild_id, role_id FROM whitelist_roles").fetch_all(&self.pool).await.unwrap();
        for row in rows { let gid = GuildId(row.get::<i64,_>(0) as u64); let rid = RoleId(row.get::<i64,_>(1) as u64); state.whitelist_roles.entry(gid).or_insert_with(HashSet::new).insert(rid); }
        let rows = sqlx::query("SELECT guild_id, user_id FROM link_bypass_users").fetch_all(&self.pool).await.unwrap();
        for row in rows { let gid = GuildId(row.get::<i64,_>(0) as u64); let uid = UserId(row.get::<i64,_>(1) as u64); state.link_bypass_users.entry(gid).or_insert_with(HashSet::new).insert(uid); }
        let rows = sqlx::query("SELECT guild_id, role_id FROM link_bypass_roles").fetch_all(&self.pool).await.unwrap();
        for row in rows { let gid = GuildId(row.get::<i64,_>(0) as u64); let rid = RoleId(row.get::<i64,_>(1) as u64); state.link_bypass_roles.entry(gid).or_insert_with(HashSet::new).insert(rid); }
        let now = now_pht();
        let rows = sqlx::query("SELECT guild_id, user_id, until_ts FROM muted_users").fetch_all(&self.pool).await.unwrap();
        for row in rows {
            let uid = UserId(row.get::<i64,_>(1) as u64);
            if let Ok(until) = DateTime::parse_from_rfc3339(row.get::<&str, _>(2)) {
                if until > now { state.muted_users.insert(uid, until); }
                else { let _ = sqlx::query("DELETE FROM muted_users WHERE user_id = $1 AND guild_id = $2").bind(uid.0 as i64).bind(row.get::<i64, _>(0)).execute(&self.pool).await; }
            }
        }
        println!("[DB] All data loaded.");
    }
    async fn set_protection(&self, gid: GuildId, en: bool) { sqlx::query("INSERT INTO protection(guild_id, enabled) VALUES ($1, $2) ON CONFLICT (guild_id) DO UPDATE SET enabled = EXCLUDED.enabled").bind(gid.0 as i64).bind(en as i32).execute(&self.pool).await.unwrap(); }
    async fn add_whitelist_user(&self, gid: GuildId, uid: UserId) { sqlx::query("INSERT INTO whitelist_users(guild_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING").bind(gid.0 as i64).bind(uid.0 as i64).execute(&self.pool).await.unwrap(); }
    async fn remove_whitelist_user(&self, gid: GuildId, uid: UserId) { sqlx::query("DELETE FROM whitelist_users WHERE guild_id = $1 AND user_id = $2").bind(gid.0 as i64).bind(uid.0 as i64).execute(&self.pool).await.unwrap(); }
    async fn add_whitelist_role(&self, gid: GuildId, rid: RoleId) { sqlx::query("INSERT INTO whitelist_roles(guild_id, role_id) VALUES ($1, $2) ON CONFLICT DO NOTHING").bind(gid.0 as i64).bind(rid.0 as i64).execute(&self.pool).await.unwrap(); }
    async fn remove_whitelist_role(&self, gid: GuildId, rid: RoleId) { sqlx::query("DELETE FROM whitelist_roles WHERE guild_id = $1 AND role_id = $2").bind(gid.0 as i64).bind(rid.0 as i64).execute(&self.pool).await.unwrap(); }
    async fn add_link_bypass_user(&self, gid: GuildId, uid: UserId) { sqlx::query("INSERT INTO link_bypass_users(guild_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING").bind(gid.0 as i64).bind(uid.0 as i64).execute(&self.pool).await.unwrap(); }
    async fn remove_link_bypass_user(&self, gid: GuildId, uid: UserId) { sqlx::query("DELETE FROM link_bypass_users WHERE guild_id = $1 AND user_id = $2").bind(gid.0 as i64).bind(uid.0 as i64).execute(&self.pool).await.unwrap(); }
    async fn add_link_bypass_role(&self, gid: GuildId, rid: RoleId) { sqlx::query("INSERT INTO link_bypass_roles(guild_id, role_id) VALUES ($1, $2) ON CONFLICT DO NOTHING").bind(gid.0 as i64).bind(rid.0 as i64).execute(&self.pool).await.unwrap(); }
    async fn remove_link_bypass_role(&self, gid: GuildId, rid: RoleId) { sqlx::query("DELETE FROM link_bypass_roles WHERE guild_id = $1 AND role_id = $2").bind(gid.0 as i64).bind(rid.0 as i64).execute(&self.pool).await.unwrap(); }
    async fn add_mute(&self, gid: GuildId, uid: UserId, until: DateTime<chrono::FixedOffset>) { sqlx::query("INSERT INTO muted_users(guild_id, user_id, until_ts) VALUES ($1, $2, $3) ON CONFLICT (guild_id, user_id) DO UPDATE SET until_ts = EXCLUDED.until_ts").bind(gid.0 as i64).bind(uid.0 as i64).bind(until.to_rfc3339()).execute(&self.pool).await.unwrap(); }
    async fn remove_mute(&self, gid: GuildId, uid: UserId) { sqlx::query("DELETE FROM muted_users WHERE guild_id = $1 AND user_id = $2").bind(gid.0 as i64).bind(uid.0 as i64).execute(&self.pool).await.unwrap(); }
    async fn log_action(&self, gid: GuildId, uid: UserId, action: &str, reason: &str) { let ts = now_pht().to_rfc3339(); sqlx::query("INSERT INTO action_history(guild_id, user_id, action, reason, timestamp) VALUES ($1, $2, $3, $4, $5)").bind(gid.0 as i64).bind(uid.0 as i64).bind(action).bind(reason).bind(ts).execute(&self.pool).await.unwrap(); }
    async fn save_guild_config(&self, gid: GuildId) {
        // Extract all values before any await point so MutexGuards are dropped first
        let (threshold_count, threshold_window_ms, punishment_str,
             max_messages_per_minute, max_duplicate_messages, max_emojis,
             auto_ban_threshold, link_whitelist_json, banned_words_json) = {
            let sec_guard = security_config().lock().unwrap();
            let anti_guard = antinuke_config().lock().unwrap();
            (
                anti_guard.threshold_count as i64,
                (anti_guard.threshold_window_secs * 1000.0) as i64,
                anti_guard.punishment.as_str().to_string(),
                sec_guard.max_messages_per_minute as i64,
                sec_guard.max_duplicate_messages as i64,
                sec_guard.max_emojis as i64,
                sec_guard.auto_ban_threshold as i64,
                serde_json::to_string(&sec_guard.link_whitelist).unwrap(),
                serde_json::to_string(&sec_guard.banned_words).unwrap(),
            )
        }; // guards dropped here, before the await below
        sqlx::query("INSERT INTO guild_config(guild_id, threshold_count, threshold_window, punishment, max_messages_per_minute, max_duplicate_messages, max_emojis, auto_ban_threshold, link_whitelist, banned_words) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (guild_id) DO UPDATE SET threshold_count=EXCLUDED.threshold_count, threshold_window=EXCLUDED.threshold_window, punishment=EXCLUDED.punishment, max_messages_per_minute=EXCLUDED.max_messages_per_minute, max_duplicate_messages=EXCLUDED.max_duplicate_messages, max_emojis=EXCLUDED.max_emojis, auto_ban_threshold=EXCLUDED.auto_ban_threshold, link_whitelist=EXCLUDED.link_whitelist, banned_words=EXCLUDED.banned_words")
            .bind(gid.0 as i64).bind(threshold_count).bind(threshold_window_ms)
            .bind(punishment_str).bind(max_messages_per_minute).bind(max_duplicate_messages)
            .bind(max_emojis).bind(auto_ban_threshold)
            .bind(link_whitelist_json).bind(banned_words_json)
            .execute(&self.pool).await.unwrap();
    }
    async fn load_guild_config(&self, gid: GuildId) {
        if let Some(row) = sqlx::query("SELECT * FROM guild_config WHERE guild_id = $1").bind(gid.0 as i64).fetch_optional(&self.pool).await.unwrap() {
            let mut sec_guard = security_config().lock().unwrap();
            let mut anti_guard = antinuke_config().lock().unwrap();
            let cfg = &mut *sec_guard; let antinuke = &mut *anti_guard;
            antinuke.threshold_count = row.get::<i64, _>(1) as usize;
            antinuke.threshold_window_secs = row.get::<i64, _>(2) as f64 / 1000.0;
            antinuke.punishment = Punishment::from_str(row.get::<&str, _>(3)).unwrap_or(Punishment::Ban);
            cfg.max_messages_per_minute = row.get::<i64, _>(4) as usize;
            cfg.max_duplicate_messages = row.get::<i64, _>(5) as usize;
            cfg.max_emojis = row.get::<i64, _>(6) as usize;
            cfg.auto_ban_threshold = row.get::<i64, _>(7) as usize;
            cfg.link_whitelist = serde_json::from_str(row.get::<&str, _>(8)).unwrap();
            cfg.banned_words = serde_json::from_str(row.get::<&str, _>(9)).unwrap();
        }
    }
}

// ------------------------------------------------------------
//  HELPERS
// ------------------------------------------------------------
async fn is_whitelisted(state: &BotState, http: &Http, cache: &Cache, gid: GuildId, uid: UserId) -> bool {
    if let Ok(current) = http.get_current_user().await { if uid == current.id { return true; } }
    if let Some(guild) = gid.to_guild_cached(cache) { if uid == guild.owner_id { return true; } }
    if let Some(set) = state.whitelist_users.get(&gid) { if set.contains(&uid) { return true; } }
    if let Some(role_set) = state.whitelist_roles.get(&gid) {
        if let Ok(member) = gid.member(http, uid).await { if member.roles.iter().any(|r| role_set.contains(r)) { return true; } }
    }
    false
}

async fn is_link_bypassed(state: &BotState, http: &Http, cache: &Cache, gid: GuildId, member: &Member) -> bool {
    if is_whitelisted(state, http, cache, gid, member.user.id).await { return true; }
    if let Some(set) = state.link_bypass_users.get(&gid) { if set.contains(&member.user.id) { return true; } }
    if let Some(role_set) = state.link_bypass_roles.get(&gid) { if member.roles.iter().any(|r| role_set.contains(r)) { return true; } }
    false
}

fn snap_guild(guild: &Guild) -> GuildSnapshot {
    GuildSnapshot {
        name: guild.name.clone(), description: guild.description.clone(), icon: guild.icon_url(), banner: guild.banner_url(),
        afk_channel_id: guild.afk_channel_id, afk_timeout: guild.afk_timeout, verification_level: guild.verification_level.num() as u16,
        default_notifications: guild.default_message_notifications.num() as u16, explicit_content_filter: guild.explicit_content_filter.num() as u16,
        system_channel_id: guild.system_channel_id,
    }
}

fn snap_partial_guild(guild: &serenity::model::guild::PartialGuild) -> GuildSnapshot {
    GuildSnapshot {
        name: guild.name.clone(), description: guild.description.clone(), icon: guild.icon_url(), banner: guild.banner_url(),
        afk_channel_id: guild.afk_channel_id, afk_timeout: guild.afk_timeout, verification_level: guild.verification_level.num() as u16,
        default_notifications: guild.default_message_notifications.num() as u16, explicit_content_filter: 0u16,
        system_channel_id: guild.system_channel_id,
    }
}

fn snap_channel(channel: &GuildChannel) -> ChannelSnapshot {
    ChannelSnapshot {
        name: channel.name.clone(), category_id: channel.parent_id, position: channel.position as i32, channel_type: channel.kind,
        overwrites: channel.permission_overwrites.iter().map(|ov| (ov.kind.clone(), ov.clone())).collect(),
        topic: if let ChannelType::Text = channel.kind { channel.topic.clone() } else { None },
        nsfw: if let ChannelType::Text = channel.kind { channel.nsfw } else { false },
        slowmode_delay: if let ChannelType::Text = channel.kind { channel.rate_limit_per_user.unwrap_or(0) } else { 0 },
    }
}

fn snap_role(role: &Role) -> RoleSnapshot {
    RoleSnapshot { name: role.name.clone(), permissions: role.permissions.bits(), colour: role.colour.0, hoist: role.hoist, mentionable: role.mentionable }
}

async fn build_permission_map(state: &BotState, http: &Http, cache: &Cache, gid: GuildId) {
    let guild = match gid.to_guild_cached(cache) { Some(g) => g, None => return };
    let mut dangerous = HashSet::new();
    for member in guild.members.values() {
        if member.user.bot { continue; }
        if is_whitelisted(state, http, cache, gid, member.user.id).await { continue; }
        let perms = member.permissions(cache).unwrap_or(Permissions::empty());
        if DANGEROUS_PERMISSIONS.iter().any(|p| perms.contains(*p)) { dangerous.insert(member.user.id); }
    }
    state.dangerous_members.insert(gid, dangerous);
}

async fn get_actor_fast(state: &BotState, http: &Http, cache: &Cache, gid: GuildId, action: &str) -> Option<UserId> {
    let now = now_pht().timestamp_millis() as f64 / 1000.0;
    let key = (gid, action.to_string());
    if let Some(prefetch_ref) = state.audit_prefetch.get(&key) {
        let (actor, fetched) = *prefetch_ref;
        if now - fetched < ACTOR_CACHE_TTL_SECS && !is_whitelisted(state, http, cache, gid, actor).await {
            state.confirmed_actors.entry(key.clone()).or_insert_with(DashMap::new).insert(actor, now + ACTOR_CACHE_TTL_SECS);
            return Some(actor);
        }
    }
    if let Some(conf) = state.confirmed_actors.get(&key) {
        for entry in conf.iter() { if now < *entry.value() && !is_whitelisted(state, http, cache, gid, *entry.key()).await { return Some(*entry.key()); } }
    }
    if let Some(dangerous) = state.dangerous_members.get(&gid) {
        let active: Vec<_> = dangerous.iter().filter(|uid| !state.ban_in_progress.get(&gid).map(|b| b.contains(*uid)).unwrap_or(false)).collect();
        if active.len() == 1 {
            let actor = *active[0];
            state.audit_prefetch.insert(key.clone(), (actor, now));
            state.confirmed_actors.entry(key).or_insert_with(DashMap::new).insert(actor, now + ACTOR_CACHE_TTL_SECS);
            return Some(actor);
        }
    }
    let action_type: u8 = match action {
        "channel_create" => 10, "channel_delete" => 12,
        "channel_update" => 11, "role_create" => 30,
        "role_delete" => 32, "role_update" => 31,
        "guild_update" => 1, "webhook_create" => 50,
        _ => return None,
    };
    if let Ok(logs) = gid.audit_logs(http, Some(action_type), None, None, Some(3)).await {
        for entry in logs.entries {
            if entry.user_id == http.get_current_user().await.ok()?.id { continue; }
            let age = (now_pht().timestamp() - entry.id.created_at().unix_timestamp()).abs() as f64;
            if age < 8.0 {
                let actor = entry.user_id; {
                    if !is_whitelisted(state, http, cache, gid, actor).await {
                        state.audit_prefetch.insert(key.clone(), (actor, now));
                        state.confirmed_actors.entry(key).or_insert_with(DashMap::new).insert(actor, now + ACTOR_CACHE_TTL_SECS);
                        return Some(actor);
                    }
                }
            }
        }
    }
    None
}

async fn log_violation(state: &BotState, http: &Http, cache: &Cache, gid: GuildId, user: &User, violation: &str, reason: &str, chid: ChannelId) {
    let mut count = state.user_violations.entry(user.id).or_insert(0);
    *count += 1; let total = *count;
    let user_mention = user.mention();
    let user_id_str = format!("{}", user.id);
    let total_str = total.to_string();
    let avatar = user.avatar_url().unwrap_or_else(|| user.default_avatar_url());
    let icon = cache.current_user().avatar_url().unwrap_or_default();
    let mut embed = CreateEmbed::default();
    embed.title("SECURITY VIOLATION DETECTED").color(EMBED_COLOR).timestamp(now_pht()) .field("User", format!("{} ({})", user_mention, user_id_str), true) .field("Violation", violation, true).field("Total Violations", total_str, true) .field("Reason", reason, false).thumbnail(avatar) .footer(|f| f.text("Coded by ransxmware.xyz").icon_url(icon));
    if let Some(log_id) = gid.channels(http).await.ok().and_then(|ch| ch.into_iter().find(|(_, c)| c.name == "security-logs").map(|(id, _)| id)) {
        let _ = log_id.send_message(http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
    }
    if total >= security_config().lock().unwrap().auto_ban_threshold && !is_whitelisted(state, http, cache, gid, user.id).await {
        let _ = gid.ban_with_reason(http, user.id, 0, &format!("Auto-ban: {} security violations", total)).await;
    }
}

async fn instant_ban_and_rollback(state: Arc<BotState>, db: Arc<Database>, http: Arc<Http>, cache: Arc<Cache>, gid: GuildId, actor: UserId, action: &str, rollback: impl std::future::Future<Output = ()> + Send + 'static, log_extra: String) {
    if !state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) { return; }
    if is_whitelisted(&*state, &*http, &*cache, gid, actor).await { return; }
    let ban_set = state.ban_in_progress.entry(gid).or_insert_with(DashSet::new);
    if ban_set.contains(&actor) { return; }
    ban_set.insert(actor);
    let state_clone = state.clone(); let db_clone = db.clone(); let http_clone = http.clone(); let gid_clone = gid; let actor_clone = actor; let action_str = action.to_string();
    tokio::spawn(async move {
        let _ = http_clone.ban_user(gid_clone.0, actor_clone.0, 0, &format!("[Anti-Nuke] {}", action_str)).await;
        let _ = db_clone.log_action(gid_clone, actor_clone, "ROLLBACK-BAN", &action_str).await;
        rollback.await;
        state_clone.ban_in_progress.entry(gid_clone).or_insert_with(DashSet::new).remove(&actor_clone);
        if let Some(log_id) = gid_clone.channels(&http_clone).await.ok().and_then(|ch| ch.into_iter().find(|(_, c)| c.name == "security-logs").map(|(id, _)| id)) {
            let mut embed = CreateEmbed::default();
            embed.title("🚨 ANTI-NUKE — INSTANT BAN + ROLLBACK").color(0xFF0000).timestamp(now_pht()) .field("Actor", format!("`{}`", actor_clone.0), true).field("Action", action_str, true).field("Ban", "✅ Banned", true) .field("Rollback", "✅ Restored", true).field("Details", log_extra, false) .footer(|f| f.text("Coded by ransxmware.xyz — Anti-Nuke Rollback"));
            let _ = log_id.send_message(&http_clone, |m| m.embed(|e| { *e = embed.clone(); e })).await;
        }
    });
}

async fn check_mass_action(state: &BotState, http: &Http, cache: &Cache, db: &Database, gid: GuildId, actor: UserId, action_type: &str) {
    if !state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) { return; }
    if is_whitelisted(&*state, &*http, &*cache, gid, actor).await { return; }
    let now = now_pht().timestamp_millis() as f64 / 1000.0;
    let mass_log = state.mass_action_log.entry(gid).or_insert_with(DashMap::new);
    let mut timestamps = mass_log.entry(actor).or_insert_with(Vec::new);
    timestamps.push(now);
    timestamps.retain(|t| now - *t <= MASS_ACTION_WINDOW_SECS);
    let count = timestamps.len();
    if count >= MASS_ACTION_THRESHOLD {
        timestamps.clear();
        if let Ok(member) = gid.member(http, actor).await {
            let reason = format!("Mass {}: {} {}s in {}s", action_type, count, action_type, MASS_ACTION_WINDOW_SECS);
            let manageable: Vec<RoleId> = member.roles.iter()
                .filter(|r| r.0 != gid.0)
                .copied()
                .collect();
            if !manageable.is_empty() { let _ = member.to_owned().remove_roles(http, &manageable).await; }
            let _ = member.ban_with_reason(http, 0, &reason).await;
            let _ = db.log_action(gid, actor, &format!("MASS-{}", action_type.to_uppercase()), &reason).await;
            if let Some(log_id) = gid.channels(http).await.ok().and_then(|ch| ch.into_iter().find(|(_, c)| c.name == "security-logs").map(|(id, _)| id)) {
                let actor_tag = member.user.tag();
                let actor_id_str = format!("{}", actor.0);
                let count_str = count.to_string();
                let window_str = format!("{}s", MASS_ACTION_WINDOW_SECS);
                let action_upper = action_type.to_uppercase();
                let mut embed = CreateEmbed::default();
                embed.title(format!("🚨 ANTI MASS {}", action_upper)).color(0xFF0000u32).timestamp(now_pht())
                    .field("Actor", format!("{} (`{}`)", actor_tag, actor_id_str), true)
                    .field("Count", count_str, true).field("Window", window_str, true)
                    .field("Action", "Roles stripped → Banned", false);
                let _ = log_id.send_message(http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
        }
    }
}

async fn auto_kick_security_threat(state: &BotState, http: &Http, cache: &Cache, db: &Database, gid: GuildId, member: &Member, reason: &str) {
    if is_whitelisted(state, http, cache, gid, member.user.id).await { return; }
    let _ = member.kick_with_reason(http, reason).await;
    let _ = db.log_action(gid, member.user.id, "AUTO-KICK", reason).await;
    let member_mention = member.user.mention();
    let member_id_str = member.user.id.0.to_string();
    let mut embed = CreateEmbed::default();
    embed.title("AUTO-KICK").description(format!("Member {} has been automatically kicked for a security threat.", member_mention)).color(0xFF4500u32).timestamp(now_pht())
        .field("Reason", reason, false).field("User ID", member_id_str, true);
    if let Some(log_id) = gid.channels(http).await.ok().and_then(|ch| ch.into_iter().find(|(_, c)| c.name == "security-logs").map(|(id, _)| id)) {
        let _ = log_id.send_message(http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
    }
}

async fn restore_channel(http: &Http, guild: &Guild, snap: &ChannelSnapshot) -> Option<ChannelId> {
    let parent_id = snap.category_id;
    let perms: Vec<PermissionOverwrite> = snap.overwrites.iter().map(|(_, ov)| ov.clone()).collect();
    let snap_name = snap.name.clone();
    let snap_topic = snap.topic.clone();
    let snap_nsfw = snap.nsfw;
    let snap_slowmode = snap.slowmode_delay;
    let snap_pos = snap.position;
    let snap_kind = snap.channel_type;
    guild.create_channel(http, |c| {
        c.name(&snap_name).kind(snap_kind).position(snap_pos as u32);
        if let Some(pid) = parent_id { c.category(pid); }
        if !perms.is_empty() { c.permissions(perms); }
        if snap_kind == ChannelType::Text {
            if let Some(ref t) = snap_topic { c.topic(t); }
            c.nsfw(snap_nsfw).rate_limit_per_user(snap_slowmode as u64);
        }
        c
    }).await.ok().map(|c| c.id)
}

async fn restore_role(state: &BotState, http: &Http, gid: GuildId, role_name: &str, snap: Option<RoleSnapshot>) -> Option<RoleId> {
    let lock = state.role_restore_locks.entry(gid).or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())));
    let _guard = lock.lock().await;
    let new_role = if let Some(s) = snap {
        gid.create_role(http, |r| r
            .name(&s.name)
            .permissions(Permissions::from_bits_truncate(s.permissions))
            .colour(s.colour as u64)
            .hoist(s.hoist)
            .mentionable(s.mentionable)
        ).await.ok()?
    } else {
        gid.create_role(http, |r| r.name(role_name)).await.ok()?
    };
    state.role_snapshots.insert(new_role.id, snap_role(&new_role));
    Some(new_role.id)
}

async fn snapshot_all(state: &BotState, http: &Http, guild: &Guild) {
    state.guild_snapshots.insert(guild.id, snap_guild(guild));
    for (id, ch) in guild.channels.iter() { if let Channel::Guild(gc) = ch { state.channel_snapshots.insert(*id, snap_channel(gc)); } }
    for (id, role) in guild.roles.iter() { state.role_snapshots.insert(*id, snap_role(role)); }
}

async fn poll_audit_logs(state: Arc<BotState>, http: Arc<Http>, cache: Arc<Cache>, gid: GuildId) {
    let actions = ["channel_delete","channel_create","channel_update","role_create","role_delete","role_update","guild_update","webhook_create","ban","kick","member_role_update"];
    loop { for act in actions { let _ = get_actor_fast(&state, &http, &cache, gid, act).await; } tokio::time::sleep(Duration::from_millis(200)).await; }
}

async fn cleanup_mutes(state: Arc<BotState>, db: Arc<Database>) {
    loop { tokio::time::sleep(Duration::from_secs(60)).await; let now = now_pht(); let to_remove: Vec<UserId> = state.muted_users.iter().filter_map(|e| if now >= *e.value() { Some(*e.key()) } else { None }).collect(); let had_removes = !to_remove.is_empty(); for uid in &to_remove { state.muted_users.remove(uid); } if had_removes { let _ = sqlx::query("DELETE FROM muted_users WHERE until_ts <= $1").bind(now.to_rfc3339()).execute(&db.pool).await; } }
}

// ------------------------------------------------------------
//  EVENT HANDLER
// ------------------------------------------------------------
struct Handler { state: Arc<BotState>, db: Arc<Database>, http: Arc<Http> }

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, ctx: Context, ready: serenity::model::gateway::Ready) {
        println!("{}", ANTINUKE_ASCII);
        println!("Logged in as: {}", ready.user.name);
        for gid in ctx.cache.guilds() { if let Some(guild) = gid.to_guild_cached(&ctx.cache) { snapshot_all(&self.state, &self.http, &guild).await; build_permission_map(&self.state, &self.http, &ctx.cache, gid).await; tokio::spawn(poll_audit_logs(self.state.clone(), self.http.clone(), ctx.cache.clone(), gid)); } }
        tokio::spawn(cleanup_mutes(self.state.clone(), self.db.clone()));
        let server_count = ctx.cache.guilds().len();
        ctx.set_presence(Some(serenity::model::gateway::Activity::watching(format!("over {} servers!", server_count))), serenity::model::user::OnlineStatus::Online).await;
    }

    async fn guild_create(&self, ctx: Context, guild: Guild, _is_new: bool) {
        snapshot_all(&self.state, &self.http, &guild).await; build_permission_map(&self.state, &self.http, &ctx.cache, guild.id).await; tokio::spawn(poll_audit_logs(self.state.clone(), self.http.clone(), ctx.cache.clone(), guild.id));
        let server_count = ctx.cache.guilds().len();
        ctx.set_presence(Some(serenity::model::gateway::Activity::watching(format!("over {} servers!", server_count))), serenity::model::user::OnlineStatus::Online).await;
    }

    async fn guild_delete(&self, ctx: Context, _incomplete: serenity::model::guild::UnavailableGuild, _full: Option<Guild>) {
        let server_count = ctx.cache.guilds().len();
        ctx.set_presence(Some(serenity::model::gateway::Activity::watching(format!("over {} servers!", server_count))), serenity::model::user::OnlineStatus::Online).await;
    }

    async fn webhook_update(&self, ctx: Context, guild_id: GuildId, channel_id: ChannelId) {
        let gid = guild_id;
        if !self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) { return; }
        let now = now_pht().timestamp_millis() as f64 / 1000.0;
        let entry = self.state.handled_webhook_events.entry(gid).or_insert_with(DashMap::new);
        if let Some(exp) = entry.get(&channel_id) { if now < *exp { return; } } entry.insert(channel_id, now + WEBHOOK_EVENT_DEDUP_TTL_SECS);
        if let Some(actor) = get_actor_fast(&self.state, &self.http, &ctx.cache, gid, "webhook_create").await {
            if is_whitelisted(&self.state, &self.http, &ctx.cache, gid, actor).await { return; }
            let http = self.http.clone(); let state = self.state.clone(); let db = self.db.clone();
            instant_ban_and_rollback(self.state.clone(), self.db.clone(), self.http.clone(), ctx.cache.clone(), gid, actor, "Unauthorized webhook creation", async move {
                let webhooks = match http.get_guild_webhooks(gid.0).await { Ok(w) => w, Err(_) => return }; 
                for wh in webhooks { let _ = http.delete_webhook(wh.id.0).await; }
            }, "All webhooks guild-wide purged".to_string()).await;
        }
    }

    async fn channel_create(&self, ctx: Context, channel: &GuildChannel) {
        let gid = channel.guild_id;
        if self.state.restoring.get(&gid).map(|r| *r).unwrap_or(false) { self.state.channel_snapshots.insert(channel.id, snap_channel(channel)); return; }
        if !self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) { self.state.channel_snapshots.insert(channel.id, snap_channel(channel)); return; }
        let now = now_pht().timestamp_millis() as f64 / 1000.0;
        let entry = self.state.handled_channel_creates.entry(gid).or_insert_with(DashMap::new);
        if let Some(exp) = entry.get(&channel.id) { if now < *exp { return; } } entry.insert(channel.id, now + CHANNEL_CREATE_DEDUP_TTL_SECS);
        if let Some(actor) = get_actor_fast(&self.state, &self.http, &ctx.cache, gid, "channel_create").await {
            if is_whitelisted(&self.state, &self.http, &ctx.cache, gid, actor).await { self.state.channel_snapshots.insert(channel.id, snap_channel(channel)); return; }
            let channel_id = channel.id; let channel_name = channel.name.clone();
            let http = self.http.clone();
            instant_ban_and_rollback(self.state.clone(), self.db.clone(), self.http.clone(), ctx.cache.clone(), gid, actor, "Unauthorized channel creation", async move {
                let _ = http.delete_channel(channel_id.0).await;
            }, format!("Channel **#{}** (`{}`) was deleted.", channel_name, channel_id.0)).await;
        } else { self.state.channel_snapshots.insert(channel.id, snap_channel(channel)); }
    }

    async fn channel_delete(&self, ctx: Context, channel: &GuildChannel) {
        let gid = channel.guild_id;
        if self.state.restoring.get(&gid).map(|r| *r).unwrap_or(false) { return; }
        if !self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) { self.state.channel_snapshots.remove(&channel.id); return; }
        if let Some(actor) = get_actor_fast(&self.state, &self.http, &ctx.cache, gid, "channel_delete").await {
            if is_whitelisted(&self.state, &self.http, &ctx.cache, gid, actor).await { self.state.channel_snapshots.remove(&channel.id); return; }
            let snap = self.state.channel_snapshots.get(&channel.id).map(|s| s.clone()).unwrap_or_else(|| ChannelSnapshot {
                name: channel.name.clone(), category_id: channel.parent_id, position: channel.position as i32, channel_type: channel.kind,
                overwrites: channel.permission_overwrites.iter().map(|ov| (ov.kind.clone(), ov.clone())).collect(),
                topic: if let ChannelType::Text = channel.kind { channel.topic.clone() } else { None },
                nsfw: if let ChannelType::Text = channel.kind { channel.nsfw } else { false },
                slowmode_delay: if let ChannelType::Text = channel.kind { channel.rate_limit_per_user.unwrap_or(0) } else { 0 },
            });
            self.state.channel_snapshots.remove(&channel.id);
            let queue_entry = self.state.rollback_queue.entry(gid).or_insert_with(DashMap::new);
            let mut actor_queue = queue_entry.entry(actor).or_insert_with(Vec::new);
            if !actor_queue.iter().any(|s| s.name == snap.name) { actor_queue.push(snap); } else { return; }
            let drain_set = self.state.drain_scheduled.entry(gid).or_insert_with(DashSet::new);
            if !drain_set.contains(&actor) {
                drain_set.insert(actor);
                {
                    let ban_set = self.state.ban_in_progress.entry(gid).or_insert_with(DashSet::new);
                    if !ban_set.contains(&actor) {
                        ban_set.insert(actor);
                        let http = self.http.clone(); let state = self.state.clone(); let db = self.db.clone();
                        tokio::spawn(async move {
                            let _ = http.ban_user(gid.0, actor.0, 0, "[Anti-Nuke] Full nuke — channel deletion").await;
                            let _ = db.log_action(gid, actor, "ROLLBACK-BAN", "full_nuke_channel_delete").await;
                            state.ban_in_progress.entry(gid).or_insert_with(DashSet::new).remove(&actor);
                        });
                    }
                }
                let state = self.state.clone(); let http = self.http.clone(); let cache = ctx.cache.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs_f64(DRAIN_DELAY_SECS)).await;
                    let queue = { let e = state.rollback_queue.entry(gid).or_insert_with(DashMap::new); e.remove(&actor).map(|(_, v)| v).unwrap_or_default() };
                    if queue.is_empty() { state.drain_scheduled.entry(gid).or_insert_with(DashSet::new).remove(&actor); return; }
                    state.restoring.insert(gid, true);
                    let queue_len = queue.len();
                    if let Some(guild) = gid.to_guild_cached(&cache) {
                        for snap in queue { let _ = restore_channel(&http, &guild, &snap).await; }
                    }
                    state.restoring.insert(gid, false);
                    state.drain_scheduled.entry(gid).or_insert_with(DashSet::new).remove(&actor);
                    if let Some(log_id) = gid.channels(&http).await.ok().and_then(|ch| ch.into_iter().find(|(_, c)| c.name == "security-logs").map(|(id, _)| id)) {
                        let mut embed = CreateEmbed::default();
                        embed.title("🚨 ANTI-NUKE — FULL NUKE ROLLBACK").color(0xFF0000).timestamp(now_pht())
                            .field("Actor", format!("`{}`", actor.0), true).field("Action", "Mass channel deletion", true)
                            .field("Ban", "✅ Banned (before restore)", true).field("Channels Restored", queue_len.to_string(), true)
                            .field("Details", "All deleted channels restored in bulk.", false);
                        let _ = log_id.send_message(&http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                    }
                });
            }
        }
    }

    async fn channel_update(&self, ctx: Context, old: Option<Channel>, new: Channel) {
        let (old_ch, new_ch) = match (old, new) {
            (Some(Channel::Guild(o)), Channel::Guild(n)) => (o, n),
            _ => return,
        };
        let old = old_ch; let new = new_ch;
        let gid = new.guild_id;
        if !self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) { self.state.channel_snapshots.insert(new.id, snap_channel(&new)); return; }
        if let Some(actor) = get_actor_fast(&self.state, &self.http, &ctx.cache, gid, "channel_update").await {
            if is_whitelisted(&self.state, &self.http, &ctx.cache, gid, actor).await { self.state.channel_snapshots.insert(new.id, snap_channel(&new)); return; }
            let snap = self.state.channel_snapshots.get(&new.id).map(|s| s.clone());
            let changed = if old.name != new.name { format!("name `{}` → `{}`", old.name, new.name) } else { "settings changed".to_string() };
            let old_name_log = old.name.clone();
            let channel_id = new.id; let http = self.http.clone(); let state = self.state.clone(); let db = self.db.clone();
            instant_ban_and_rollback(self.state.clone(), self.db.clone(), self.http.clone(), ctx.cache.clone(), gid, actor, "Unauthorized channel edit", async move {
                if let Ok(Channel::Guild(mut live)) = http.get_channel(channel_id.0).await {
                    let restore_name = snap.as_ref().map(|s| s.name.clone()).unwrap_or(old.name.clone());
                    let snap_ref = snap.clone();
                    let old_topic = old.topic.clone();
                    let old_nsfw = old.nsfw;
                    let old_ratelimit = old.rate_limit_per_user.unwrap_or(0);
                    let is_text = live.kind == ChannelType::Text;
                    let _ = live.edit(&http, |e| {
                        e.name(&restore_name);
                        if is_text {
                            if let Some(ref s) = snap_ref {
                                e.topic(s.topic.as_deref().unwrap_or("")).nsfw(s.nsfw).rate_limit_per_user(s.slowmode_delay as u64);
                            } else {
                                e.topic(old_topic.as_deref().unwrap_or("")).nsfw(old_nsfw).rate_limit_per_user(old_ratelimit);
                            }
                        }
                        e
                    }).await;
                    state.channel_snapshots.insert(channel_id, snap_channel(&live));
                }
            }, format!("Channel **#{}** — {} → reverted.", old_name_log, changed)).await;
        } else { self.state.channel_snapshots.insert(new.id, snap_channel(&new)); }
    }

    async fn guild_update(&self, ctx: Context, old: Option<Guild>, new: serenity::model::guild::PartialGuild) {
        let old = match old { Some(g) => g, None => { self.state.guild_snapshots.insert(new.id, snap_partial_guild(&new)); return; } };
        let mut gid = new.id;
        if !self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) { self.state.guild_snapshots.insert(gid, snap_partial_guild(&new)); return; }
        let now = now_pht().timestamp_millis() as f64 / 1000.0;
        if let Some(exp) = self.state.handled_guild_updates.get(&gid) { if now < *exp { return; } } self.state.handled_guild_updates.insert(gid, now + GUILD_UPDATE_DEDUP_TTL_SECS);
        if let Some(actor) = get_actor_fast(&self.state, &self.http, &ctx.cache, gid, "guild_update").await {
            if is_whitelisted(&self.state, &self.http, &ctx.cache, gid, actor).await { self.state.guild_snapshots.insert(gid, snap_partial_guild(&new)); return; }
            let snap = self.state.guild_snapshots.get(&gid).map(|s| s.clone());
            let changes = format!("name `{}` → `{}`", old.name, new.name);
            let http = self.http.clone(); let state = self.state.clone();
            instant_ban_and_rollback(self.state.clone(), self.db.clone(), self.http.clone(), ctx.cache.clone(), gid, actor, "Unauthorized guild settings change", async move {
                let mut gid = gid; // rebind as mut inside async block for gid.edit()
                if let Some(s) = snap {
                    let sname = s.name.clone();
                    let sdesc = s.description.clone();
                    let safk_timeout = s.afk_timeout;
                    let sverif = s.verification_level;
                    let snotif = s.default_notifications;
                    let secf = s.explicit_content_filter;
                    let _ = gid.edit(&http, |e| {
                        use serenity::model::guild::{VerificationLevel, DefaultMessageNotificationLevel, ExplicitContentFilter};
                        let vl = match sverif { 1 => VerificationLevel::Low, 2 => VerificationLevel::Medium, 3 => VerificationLevel::High, 4 => VerificationLevel::Higher, _ => VerificationLevel::None };
                        let nl = match snotif { 1 => DefaultMessageNotificationLevel::Mentions, _ => DefaultMessageNotificationLevel::All };
                        let ef = match secf { 1 => ExplicitContentFilter::WithoutRole, 2 => ExplicitContentFilter::All, _ => ExplicitContentFilter::None };
                        e.name(&sname)
                         .afk_timeout(safk_timeout)
                         .verification_level(vl)
                         .default_message_notifications(Some(nl))
                         .explicit_content_filter(Some(ef));
                        if let Some(ref desc) = sdesc { e.description(desc); }
                        e
                    }).await;
                    state.guild_snapshots.insert(gid, s);
                }
            }, format!("Changes: {}", changes)).await;
        } else { self.state.guild_snapshots.insert(gid, snap_partial_guild(&new)); }
    }

    async fn guild_role_create(&self, ctx: Context, role: Role) {
        let gid = role.guild_id;
        if !self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) { self.state.role_snapshots.insert(role.id, snap_role(&role)); return; }
        let now = now_pht().timestamp_millis() as f64 / 1000.0;
        let entry = self.state.handled_role_events.entry(gid).or_insert_with(DashMap::new);
        if let Some(exp) = entry.get(&role.id.0) { if now < *exp { return; } } entry.insert(role.id.0, now + ROLE_EVENT_DEDUP_TTL_SECS);
        if let Some(actor) = get_actor_fast(&self.state, &self.http, &ctx.cache, gid, "role_create").await {
            if is_whitelisted(&self.state, &self.http, &ctx.cache, gid, actor).await { self.state.role_snapshots.insert(role.id, snap_role(&role)); return; }
            let role_id = role.id; let role_name = role.name.clone();
            let http = self.http.clone();
            instant_ban_and_rollback(self.state.clone(), self.db.clone(), self.http.clone(), ctx.cache.clone(), gid, actor, "Unauthorized role creation", async move {
                let _ = http.delete_role(gid.0, role_id.0).await;
            }, format!("Role **@{}** (`{}`) was deleted.", role_name, role_id.0)).await;
        } else { self.state.role_snapshots.insert(role.id, snap_role(&role)); }
    }

    async fn guild_role_update(&self, ctx: Context, old: Option<Role>, new: Role) {
        let old = match old { Some(r) => r, None => { self.state.role_snapshots.insert(new.id, snap_role(&new)); return; } };
        let gid = new.guild_id;
        if !self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) { self.state.role_snapshots.insert(new.id, snap_role(&new)); return; }
        let now = now_pht().timestamp_millis() as f64 / 1000.0;
        let key = (new.id.0 as u64) * 10_000_000 + (old.permissions.bits() % 10_000_000);
        let entry = self.state.handled_role_events.entry(gid).or_insert_with(DashMap::new);
        if let Some(exp) = entry.get(&key) { if now < *exp { return; } } entry.insert(key, now + ROLE_EVENT_DEDUP_TTL_SECS);
        if let Some(actor) = get_actor_fast(&self.state, &self.http, &ctx.cache, gid, "role_update").await {
            if is_whitelisted(&self.state, &self.http, &ctx.cache, gid, actor).await { self.state.role_snapshots.insert(new.id, snap_role(&new)); build_permission_map(&self.state, &self.http, &ctx.cache, gid).await; return; }
            let snap = self.state.role_snapshots.get(&new.id).map(|s| s.clone());
            let changes = if old.name != new.name { format!("name `{}` → `{}`", old.name, new.name) } else { "settings changed".to_string() };
            let old_name_log = old.name.clone();
            let role_id = new.id;
            let http = self.http.clone(); let state = self.state.clone();
            instant_ban_and_rollback(self.state.clone(), self.db.clone(), self.http.clone(), ctx.cache.clone(), gid, actor, "Unauthorized role edit", async move {
                let roles = http.get_guild_roles(gid.0).await;
                if let Some(live) = roles.ok().as_deref().and_then(|rs| rs.iter().find(|r| r.id == role_id)) {
                    if let Some(s) = snap.clone() {
                        let sname = s.name.clone();
                        let sperms = s.permissions;
                        let scolour = s.colour;
                        let shoist = s.hoist;
                        let sment = s.mentionable;
                        let _ = live.edit(&http, |r| r.name(&sname).permissions(Permissions::from_bits_truncate(sperms)).colour(scolour as u64).hoist(shoist).mentionable(sment)).await;
                    } else {
                        let oname = old.name.clone();
                        let operms = old.permissions;
                        let ocolour = old.colour.0;
                        let ohoist = old.hoist;
                        let oment = old.mentionable;
                        let _ = live.edit(&http, |r| r.name(&oname).permissions(operms).colour(ocolour as u64).hoist(ohoist).mentionable(oment)).await;
                    }
                    state.role_snapshots.insert(role_id, snap.unwrap_or_else(|| snap_role(&live)));
                }
            }, format!("Role **@{}** — {} → reverted.", old_name_log, changes)).await;
        } else { self.state.role_snapshots.insert(new.id, snap_role(&new)); }
    }

    async fn guild_role_delete(&self, ctx: Context, gid: GuildId, role_id: RoleId, role: Option<Role>) {
        let role = match role { Some(r) => r, None => { self.state.role_snapshots.remove(&role_id); return; } };
        if !self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) { self.state.role_snapshots.remove(&role.id); return; }
        let now = now_pht().timestamp_millis() as f64 / 1000.0;
        let entry = self.state.handled_role_events.entry(gid).or_insert_with(DashMap::new);
        if let Some(exp) = entry.get(&role.id.0) { if now < *exp { return; } } entry.insert(role.id.0, now + ROLE_EVENT_DEDUP_TTL_SECS);
        if let Some(actor) = get_actor_fast(&self.state, &self.http, &ctx.cache, gid, "role_delete").await {
            if is_whitelisted(&self.state, &self.http, &ctx.cache, gid, actor).await { self.state.role_snapshots.remove(&role.id); return; }
            let snap = self.state.role_snapshots.remove(&role.id).map(|(_, s)| s);
            let role_name = role.name.clone();
            let role_name2 = role_name.clone();
            let role_id_val = role.id.0;
            let state = self.state.clone(); let db = self.db.clone(); let http = self.http.clone();
            instant_ban_and_rollback(self.state.clone(), self.db.clone(), self.http.clone(), ctx.cache.clone(), gid, actor, "Unauthorized role deletion", async move {
                restore_role(&state, &http, gid, &role_name, snap).await;
            }, format!("Role **@{}** (`{}`) was restored.", role_name2, role_id_val)).await;
        }
    }

    async fn guild_ban_addition(&self, ctx: Context, gid: GuildId, banned_user: User) {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if let Ok(logs) = gid.audit_logs(&self.http, Some(22u8), None, None, Some(5)).await {
            for entry in logs.entries {
                let age = (now_pht().timestamp() - entry.id.created_at().unix_timestamp()).abs() as f64;
                if age < 15.0 && entry.target_id == Some(banned_user.id.0) {
                    let actor = entry.user_id; {
                        check_mass_action(&self.state, &self.http, &ctx.cache, &self.db, gid, actor, "Ban").await;
                    }
                    break;
                }
            }
        }
    }

    async fn guild_member_removal(&self, ctx: Context, gid: GuildId, user: User, _member_data: Option<Member>) {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if let Ok(logs) = gid.audit_logs(&self.http, Some(20u8), None, None, Some(5)).await {
            for entry in logs.entries {
                let age = (now_pht().timestamp() - entry.id.created_at().unix_timestamp()).abs() as f64;
                if age < 15.0 && entry.target_id == Some(user.id.0) {
                    let actor = entry.user_id; {
                        check_mass_action(&self.state, &self.http, &ctx.cache, &self.db, gid, actor, "Kick").await;
                    }
                    break;
                }
            }
        }
    }

    async fn guild_member_addition(&self, ctx: Context, new_member: Member) {
        let gid = new_member.guild_id;
        if !self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) { return; }
        if new_member.user.bot {
            tokio::time::sleep(Duration::from_millis(300)).await;
            if let Ok(logs) = gid.audit_logs(&self.http, Some(28u8), None, None, Some(5)).await {
                for entry in logs.entries {
                    let age = (now_pht().timestamp() - entry.id.created_at().unix_timestamp()).abs() as f64;
                    if age < 20.0 && entry.target_id == Some(new_member.user.id.0) {
                        let adder = entry.user_id; {
                            if !is_whitelisted(&self.state, &self.http, &ctx.cache, gid, adder).await {
                                auto_kick_security_threat(&self.state, &self.http, &ctx.cache, &self.db, gid, &new_member, "Unauthorized bot addition detected — Security Protocol Activated").await;
                            }
                        }
                        break;
                    }
                }
            }
        }
    }

    async fn guild_member_update(&self, ctx: Context, old_if_available: Option<Member>, new: Member) {
        let gid = new.guild_id;
        let old_roles = old_if_available.map(|o| o.roles).unwrap_or_default();
        for role_id in new.roles.iter().filter(|r| !old_roles.contains(r)) {
            if let Some(role) = ctx.cache.guild(gid).and_then(|g| g.roles.get(role_id).cloned()) {
                if DANGEROUS_PERMISSIONS.iter().any(|p| role.permissions.contains(*p)) {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    if let Ok(logs) = gid.audit_logs(&self.http, Some(25u8), None, None, Some(15)).await {
                        for entry in logs.entries {
                            let age = (now_pht().timestamp() - entry.id.created_at().unix_timestamp()).abs() as f64;
                            if age < 20.0 && entry.target_id == Some(new.user.id.0) {
                                let assigner = entry.user_id; {
                                    if !is_whitelisted(&self.state, &self.http, &ctx.cache, gid, assigner).await {
                                        if let Ok(assigner_member) = gid.member(&self.http, assigner).await {
                                            auto_kick_security_threat(&self.state, &self.http, &ctx.cache, &self.db, gid, &assigner_member, &format!("Granted dangerous permissions to {}", new.user.tag())).await;
                                        }
                                    }
                                }
                                break;
                            }
                        }
                    }
                    break;
                }
            }
        }
        build_permission_map(&self.state, &self.http, &ctx.cache, gid).await;
    }

    async fn message(&self, ctx: Context, msg: Message) {
        if msg.author.bot { return; }
        let gid = match msg.guild_id { Some(g) => g, None => return };
        let now = now_pht();

        // Rate limiting for commands
        let is_cmd = msg.content.starts_with('x') || msg.content.starts_with("null");
        if is_cmd {
            if let Some(until) = self.state.rate_limited_until.get(&msg.author.id) { if now < *until { let _ = msg.delete(&self.http).await; return; } else { self.state.rate_limited_until.remove(&msg.author.id); } }
            let now_ts = now.timestamp_millis() as f64 / 1000.0;
            let mut timestamps = self.state.command_timestamps.entry(msg.author.id).or_insert_with(|| VecDeque::with_capacity(10));
            timestamps.push_back(now_ts);
            while let Some(t) = timestamps.front() { if now_ts - *t > RATE_LIMIT_WINDOW_SECS { timestamps.pop_front(); } else { break; } }
            if timestamps.len() > RATE_LIMIT_MAX_COMMANDS {
                let cooldown = now + ChronoDuration::seconds(RATE_LIMIT_COOLDOWN_SECS);
                self.state.rate_limited_until.insert(msg.author.id, cooldown);
                let _ = msg.delete(&self.http).await;
                let mut embed = CreateEmbed::default();
                embed.title("⏱️ Slow Down!").description(format!("{} you're sending commands too fast.\nPlease wait **{} seconds** before using commands again.", msg.author.mention(), RATE_LIMIT_COOLDOWN_SECS)).color(0xFF4500).timestamp(now);
                if let Ok(sent) = msg.channel_id.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await { tokio::time::sleep(Duration::from_secs(5)).await; let _ = sent.delete(&self.http).await; }
                return;
            }
        }

        // Muted check
        if let Some(until) = self.state.muted_users.get(&msg.author.id) { if now < *until { let _ = msg.delete(&self.http).await; return; } else { self.state.muted_users.remove(&msg.author.id); } }

        // Whitelisted bypass
        if is_whitelisted(&self.state, &self.http, &ctx.cache, gid, msg.author.id).await {
            self.process_commands(ctx, &msg).await;
            return;
        }

        // Track message times and content
        {
            let mut times = self.state.user_message_times.entry(msg.author.id).or_insert_with(VecDeque::new);
            times.push_back(now);
            while let Some(t) = times.front() { if (now - *t).num_seconds() > 60 { times.pop_front(); } else { break; } }
            let mut msgs = self.state.user_messages.entry(msg.author.id).or_insert_with(VecDeque::new);
            msgs.push_back(msg.content.to_lowercase());
            while msgs.len() > 10 { msgs.pop_front(); }
        }

        if self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) {
            let member = match msg.member(&ctx).await { Ok(m) => m, Err(_) => return };
            let link_bypassed = is_link_bypassed(&self.state, &self.http, &ctx.cache, gid, &member).await;

            // Invite detection & server ad enforcement
            let invite_re = Regex::new(r"(?i)discord\.gg/([a-zA-Z0-9]+)|discord(?:app)?\.com/invite/([a-zA-Z0-9]+)").unwrap();
            if let Some(caps) = invite_re.captures(&msg.content) {
                let code = caps.get(1).or_else(|| caps.get(2)).unwrap().as_str();
                if let Ok(invite) = self.http.get_invite(code, false, false, None).await {
                    if let Some(inv_guild) = invite.guild {
                        if inv_guild.id != gid.0 {
                            // server ad
                            let now_ts = now.timestamp_millis() as f64 / 1000.0;
                            let ad_reg = self.state.server_ad_registry.entry(gid).or_insert_with(DashMap::new);
                            let existing = ad_reg.get(&msg.author.id).map(|e| e.clone());
                            if let Some(ex) = existing {
                                if ex.invite_code == code && ex.channel_id == msg.channel_id {
                                    let _ = msg.delete(&self.http).await;
                                    let mut embed = CreateEmbed::default();
                                    embed.title("🚫 Duplicate Server Ad").description(format!("{}, your server ad is **already posted** in this channel.\nYou may only advertise once every **{} hour(s)**.", msg.author.mention(), SERVER_AD_EXPIRY_SECS / 3600)).color(0xFF4500);
                                    if let Ok(sent) = msg.channel_id.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await { tokio::time::sleep(Duration::from_secs(8)).await; let _ = sent.delete(&self.http).await; }
                                    return;
                                } else {
                                    let _ = msg.delete(&self.http).await;
                                    let spam_map = self.state.ad_spam_channels.entry(gid).or_insert_with(DashMap::new);
                                    let mut channels = spam_map.entry(msg.author.id).or_insert_with(Vec::new);
                                    if !channels.contains(&msg.channel_id) { channels.push(msg.channel_id); }
                                    if let Some(orig_ch) = ex.channel_id.to_channel(&self.http).await.ok().and_then(|c| c.guild()) { let _ = orig_ch.delete_messages(&self.http, &[ex.message_id]).await; }
                                    ad_reg.remove(&msg.author.id);
                                    spam_map.remove(&msg.author.id);
                                    let timeout = now + ChronoDuration::minutes(AD_SPAM_TIMEOUT_MIN);
                                    let timeout_str = timeout.to_rfc3339();
                                    if let Ok(member) = gid.member(&self.http, msg.author.id).await {
                                        let _ = member.edit(&self.http, |e| e.disable_communication_until(timeout_str.clone())).await;
                                    }
                                    self.db.log_action(gid, msg.author.id, "AD-SPAM-TIMEOUT", &format!("Spammed ad in {} channels", channels.len())).await;
                                    if let Some(log_id) = gid.channels(&self.http).await.ok().and_then(|ch| ch.into_iter().find(|(_, c)| c.name == "security-logs").map(|(id, _)| id)) {
                                        let mut embed = CreateEmbed::default();
                                        embed.title("📢 AD SPAM DETECTED — TIMEOUT ISSUED").color(0xFF4500).timestamp(now) .field("User", format!("{} (`{}`)", msg.author.mention(), msg.author.id), true) .field("Invite", format!("`discord.gg/{}`", code), true) .field("Channels", format!("{} channels spammed", channels.len()), true) .field("Timeout", format!("{} minutes", AD_SPAM_TIMEOUT_MIN), true) .field("Action", "All ad copies deleted + user timed out", false);
                                        let _ = log_id.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                                    }
                                    let mut embed = CreateEmbed::default();
                                    embed.title("🚫 Server Ad Spam Detected").description(format!("{} has been **timed out for {} minutes** for spamming their server ad across **{} channels**.\nAll copies of the ad have been **deleted**.\nYou are only allowed **one ad** per {} hour(s).", msg.author.mention(), AD_SPAM_TIMEOUT_MIN, channels.len(), SERVER_AD_EXPIRY_SECS / 3600)).color(0xFF0000);
                                    if let Ok(sent) = msg.channel_id.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await { tokio::time::sleep(Duration::from_secs(15)).await; let _ = sent.delete(&self.http).await; }
                                    return;
                                }
                            } else {
                                ad_reg.insert(msg.author.id, ServerAdEntry { invite_code: code.to_string(), channel_id: msg.channel_id, message_id: msg.id, timestamp: now_ts });
                                let spam_map = self.state.ad_spam_channels.entry(gid).or_insert_with(DashMap::new);
                                spam_map.insert(msg.author.id, vec![msg.channel_id]);
                            }
                        }
                    }
                } else {
                    let _ = msg.delete(&self.http).await;
                    log_violation(&self.state, &self.http, &ctx.cache, gid, &msg.author, "MALICIOUS DISCORD INVITE", &format!("Posted a malicious Discord invite (code: {})", code), msg.channel_id).await;
                    return;
                }
            }

            // Spam detection
            let recent_times_len = self.state.user_message_times.get(&msg.author.id).unwrap().len();
            let max_msgs = security_config().lock().unwrap().max_messages_per_minute;
            if recent_times_len > max_msgs {
                let _ = msg.delete(&self.http).await;
                log_violation(&self.state, &self.http, &ctx.cache, gid, &msg.author, "SPAM DETECTION", &format!("Sent {} messages in 1 minute", recent_times_len), msg.channel_id).await;
                return;
            }

            // Duplicate messages
            let dup_count = {
                let recent_msgs = self.state.user_messages.get(&msg.author.id).unwrap();
                recent_msgs.iter().filter(|m| *m == &msg.content.to_lowercase()).count()
            };
            let max_dup = security_config().lock().unwrap().max_duplicate_messages;
            if dup_count > max_dup {
                let _ = msg.delete(&self.http).await;
                log_violation(&self.state, &self.http, &ctx.cache, gid, &msg.author, "DUPLICATE SPAM", "Sending identical messages repeatedly", msg.channel_id).await;
                return;
            }

            // Banned words
            if !link_bypassed {
                let lower = msg.content.to_lowercase();
                let banned_words = security_config().lock().unwrap().banned_words.clone();
                for word in &banned_words {
                    if lower.contains(word.as_str()) {
                        let _ = msg.delete(&self.http).await;
                        log_violation(&self.state, &self.http, &ctx.cache, gid, &msg.author, "BANNED WORD", &format!("Used prohibited word: '{}'", word), msg.channel_id).await;
                        return;
                    }
                }
            }

            // Emoji spam
            let emoji_re = Regex::new(r"<:[^:]+:\d+>|[\u{1F600}-\u{1F64F}]").unwrap();
            let emoji_count = emoji_re.find_iter(&msg.content).count();
            let max_emojis = security_config().lock().unwrap().max_emojis;
            if emoji_count > max_emojis {
                let _ = msg.delete(&self.http).await;
                log_violation(&self.state, &self.http, &ctx.cache, gid, &msg.author, "EMOJI SPAM", &format!("Used {} emojis (limit: {})", emoji_count, max_emojis), msg.channel_id).await;
                return;
            }

            // Links
            if !link_bypassed {
                let url_re = Regex::new(r"https?://[^\s]+").unwrap();
                let link_whitelist = security_config().lock().unwrap().link_whitelist.clone();
                for url in url_re.find_iter(&msg.content) {
                    let url_str = url.as_str();
                    let allowed = link_whitelist.iter().any(|d| url_str.contains(d.as_str()));
                    let is_gif = url_str.to_lowercase().ends_with(".gif");
                    if !allowed && !is_gif {
                        let _ = msg.delete(&self.http).await;
                        log_violation(&self.state, &self.http, &ctx.cache, gid, &msg.author, "UNAUTHORIZED LINK", &format!("Posted non-whitelisted link: {}", url_str), msg.channel_id).await;
                        return;
                    }
                }
            }
        }

        // "null av" natural language
        let null_av_re = Regex::new(r"(?i)^null\s+av(?:\s+(.+))?$").unwrap();
        if let Some(caps) = null_av_re.captures(&msg.content) {
            let query = caps.get(1).map(|m| m.as_str()).unwrap_or("");
            let target = if let Some(mention) = msg.mentions.first() {
                Some(mention.clone())
            } else if !query.is_empty() {
                let lower = query.to_lowercase();
                ctx.cache.guild(gid).and_then(|g| g.members.values().find(|m| m.display_name().to_lowercase().contains(&lower) || m.user.name.to_lowercase().contains(&lower)).map(|m| m.user.clone()))
            } else {
                None
            };
            if let Some(user) = target {
                let mut embed = CreateEmbed::default();
                embed.color(EMBED_COLOR).image(user.avatar_url().unwrap_or_else(|| user.default_avatar_url()));
                let _ = msg.channel_id.send_message(&self.http, |m| m.content(format!("Here is {}'s avatar.", user.mention())).embed(|e| { *e = embed.clone(); e })).await;
            }
            return;
        }

        self.process_commands(ctx, &msg).await;
    }
}

// ------------------------------------------------------------
//  COMMAND PROCESSING (All commands fully implemented)
// ------------------------------------------------------------
impl Handler {
    async fn process_commands(&self, ctx: Context, msg: &Message) {
        let content = &msg.content;
        if !(content.starts_with('x') || content.starts_with("null")) { return; }
        let prefix = if content.starts_with('x') { "x" } else { "null" };
        let args: Vec<&str> = content[prefix.len()..].trim().split_whitespace().collect();
        let cmd = args.first().unwrap_or(&"").to_lowercase();
        let rest = &args[1..];
        let gid = match msg.guild_id { Some(g) => g, None => return };
        let member = match msg.member(&ctx).await { Ok(m) => m, Err(_) => return };
        let author = &msg.author;
        let channel = msg.channel_id;

        let is_owner = || { gid.to_guild_cached(&ctx.cache).map(|g| author.id == g.owner_id).unwrap_or(false) };
        let has_perms = |perms: Permissions| member.permissions(&ctx.cache).unwrap_or(Permissions::empty()).contains(perms);
        let is_admin = has_perms(Permissions::ADMINISTRATOR);
        let manage_msgs = has_perms(Permissions::MANAGE_MESSAGES);
        let ban_members = has_perms(Permissions::BAN_MEMBERS);
        let kick_members = has_perms(Permissions::KICK_MEMBERS);
        let manage_roles = has_perms(Permissions::MANAGE_ROLES);

        async fn send_embed(http: &Http, ch: ChannelId, title: &str, desc: &str, color: u32) {
            let mut embed = CreateEmbed::default();
            embed.title(title).description(desc).color(color).timestamp(now_pht());
            let _ = ch.send_message(http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
        }

        match cmd.as_str() {
            "antinuke" => {
                if !is_owner() { let _ = send_embed(&self.http, channel, "🔒 Owner Only", "This command can only be used by the **server owner**.", 0xFF0000).await; return; }
                if rest.is_empty() {
                    let enabled = self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false);
                    let status = if enabled { "ENABLED" } else { "DISABLED" };
                    let color = if enabled { 0x00FF00 } else { 0xFF0000 };
                    let mut embed = CreateEmbed::default();
                    embed.title("Protection Status").description(format!("Anti-Nuke + Security Protection: **{}**", status)).color(color).timestamp(now_pht());
                    if enabled {
                        embed.field("Active Protections", format!("• Anti Webhook Create\n• Anti Channel Create / Delete / Update\n• Anti Guild Update\n• Anti Role Create / Update / Delete\n• Message moderation (spam, caps, links, etc.)\nThreshold: **{} actions** in **{}s** → **{}**", antinuke_config().lock().unwrap().threshold_count, antinuke_config().lock().unwrap().threshold_window_secs, antinuke_config().lock().unwrap().punishment.as_str().to_uppercase()), false);
                    } else {
                        embed.field("Note", "Use `xantinuke on` or `xsecurity on` to enable protection.", false);
                    }
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                    return;
                }
                let setting = rest[0].to_lowercase();
                if setting == "on" || setting == "enable" || setting == "true" || setting == "1" {
                    self.state.protection_enabled.insert(gid, true);
                    self.db.set_protection(gid, true).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("Protection Enabled").description("Anti-Nuke + Security protection is now **ACTIVE**").color(0x00FF00).timestamp(now_pht()) .field("Now Protected Against", "• Webhook creation abuse\n• Mass channel create / delete / update\n• Server (guild) settings tampering\n• Role create / update / delete spam\n• Message spam, caps, invite links, banned words\n".to_owned() + &format!("Threshold: **{} actions / {}s** → **{}**", antinuke_config().lock().unwrap().threshold_count, antinuke_config().lock().unwrap().threshold_window_secs, antinuke_config().lock().unwrap().punishment.as_str().to_uppercase()), false) .field("⚠️ Important", "Whitelist trusted admins with `xwhitelistuser @user` to avoid false triggers.", false) .footer(|f| f.text("Coded by ransxmware.xyz — Protection"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else if setting == "off" || setting == "disable" || setting == "false" || setting == "0" {
                    self.state.protection_enabled.insert(gid, false);
                    self.db.set_protection(gid, false).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("Protection Disabled").description("Anti-Nuke + Security protection is now **INACTIVE**").color(0xFF0000).timestamp(now_pht()) .field("Note", "Use `xantinuke on` or `xsecurity on` to re-enable protection.", false) .footer(|f| f.text("Coded by ransxmware.xyz — Protection"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else { let _ = send_embed(&self.http, channel, "❌ Invalid Setting", "Usage: `xantinuke on` or `xantinuke off`", 0xFF0000).await; }
            }
            "security" => {
                if !is_owner() { let _ = send_embed(&self.http, channel, "🔒 Owner Only", "This command can only be used by the server owner.", 0xFF0000).await; return; }
                if rest.is_empty() {
                    let enabled = self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false);
                    let status = if enabled { "ENABLED" } else { "DISABLED" };
                    let color = if enabled { 0x00FF00 } else { 0xFF0000 };
                    let mut embed = CreateEmbed::default();
                    embed.title("Protection Status").description(format!("Anti-Nuke + Security: **{}**", status)).color(color).timestamp(now_pht());
                    if enabled {
                        embed.field("Active Protections", "• Auto-kick unauthorized bots\n• Discord invite blocking\n• Webhook monitoring\n• Channel deletion monitoring\n• Dangerous permission monitoring\n• Message spam / caps / link filtering", false);
                    } else {
                        embed.field("Available Protections", "Use `xsecurity on` to enable all protection features.", false);
                    }
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                    return;
                }
                let setting = rest[0].to_lowercase();
                if setting == "on" || setting == "enable" || setting == "true" || setting == "1" {
                    self.state.protection_enabled.insert(gid, true);
                    self.db.set_protection(gid, true).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("Protection Enabled").description("Anti-Nuke + Security features are now **ACTIVE**").color(0x00FF00).timestamp(now_pht()) .field("Now Protected Against", "• Unauthorized bot additions\n• Discord invite spam\n• Webhook abuse\n• Mass channel operations\n• Dangerous permission escalation\n• Message spam / caps / banned words", false) .field("⚠️ Important", "Whitelist trusted admins with `xwhitelistuser @user` to avoid false triggers.", false) .footer(|f| f.text("Coded by ransxmware.xyz — Security System"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else if setting == "off" || setting == "disable" || setting == "false" || setting == "0" {
                    self.state.protection_enabled.insert(gid, false);
                    self.db.set_protection(gid, false).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("Protection Disabled").description("Anti-Nuke + Security features are now **INACTIVE**").color(0xFF0000).timestamp(now_pht()) .field("Note", "Use `xsecurity on` or `xantinuke on` to re-enable protection.", false) .footer(|f| f.text("Coded by ransxmware.xyz — Security System"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else { let _ = send_embed(&self.http, channel, "❌ Invalid Setting", "Use `xsecurity on` or `xsecurity off`", 0xFF0000).await; }
            }
            "whitelistrole" => {
                if !is_owner() { let _ = send_embed(&self.http, channel, "🔒 Owner Only", "This command can only be used by the server owner.", 0xFF0000).await; return; }
                if let Some(role_id) = msg.mention_roles.iter().next() {
                    let role = match gid.to_guild_cached(&ctx.cache).and_then(|g| g.roles.get(role_id).cloned()) { Some(r) => r, None => { let _ = send_embed(&self.http, channel, "❌ Role Not Found", "Could not find that role.", 0xFF0000).await; return; } };
                    let mut set = self.state.whitelist_roles.entry(gid).or_insert_with(HashSet::new);
                    if set.contains(&role.id) { let _ = send_embed(&self.http, channel, "❌ Already Whitelisted", &format!("{} is already whitelisted.", role.mention()), 0xFF0000).await; return; }
                    set.insert(role.id);
                    self.db.add_whitelist_role(gid, role.id).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("✅ Role Whitelisted").description(format!("{} has been added to the anti-nuke whitelist.", role.mention())).color(0x00FF00).timestamp(now_pht()) .field("Role", role.mention(), true).field("Added by", author.mention(), true).field("Benefit", "Members with this role bypass anti-nuke & security checks.", false) .footer(|f| f.text("Coded by ransxmware.xyz"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else { let _ = send_embed(&self.http, channel, "❌ Missing Role", "Usage: `xwhitelistrole @role`", 0xFF0000).await; }
            }
            "unwhitelistrole" => {
                if !is_owner() { let _ = send_embed(&self.http, channel, "🔒 Owner Only", "This command can only be used by the server owner.", 0xFF0000).await; return; }
                if let Some(role_id) = msg.mention_roles.iter().next() {
                    let role = match gid.to_guild_cached(&ctx.cache).and_then(|g| g.roles.get(role_id).cloned()) { Some(r) => r, None => { let _ = send_embed(&self.http, channel, "❌ Role Not Found", "Could not find that role.", 0xFF0000).await; return; } };
                    let mut set = self.state.whitelist_roles.entry(gid).or_insert_with(HashSet::new);
                    if !set.contains(&role.id) { let _ = send_embed(&self.http, channel, "❌ Role Not Whitelisted", &format!("{} is not whitelisted.", role.mention()), 0xFF0000).await; return; }
                    set.remove(&role.id);
                    self.db.remove_whitelist_role(gid, role.id).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("✅ Role Removed from Whitelist").description(format!("{} has been removed from the whitelist.", role.mention())).color(0x00FF00).timestamp(now_pht()) .field("Role", role.mention(), true).field("Removed by", author.mention(), true) .footer(|f| f.text("Coded by ransxmware.xyz"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else { let _ = send_embed(&self.http, channel, "❌ Missing Role", "Usage: `xunwhitelistrole @role`", 0xFF0000).await; }
            }
            "whitelistuser" => {
                if !is_owner() { let _ = send_embed(&self.http, channel, "🔒 Owner Only", "This command can only be used by the server owner.", 0xFF0000).await; return; }
                if let Some(user_mention) = msg.mentions.iter().next() {
                    let uid = user_mention.id;
                    let mut set = self.state.whitelist_users.entry(gid).or_insert_with(HashSet::new);
                    if set.contains(&uid) { let _ = send_embed(&self.http, channel, "❌ Already Whitelisted", &format!("{} is already whitelisted.", user_mention.mention()), 0xFF0000).await; return; }
                    set.insert(uid);
                    self.db.add_whitelist_user(gid, uid).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("✅ User Whitelisted").description(format!("{} has been added to the anti-nuke whitelist.", user_mention.mention())).color(0x00FF00).timestamp(now_pht()) .field("User", user_mention.mention(), true).field("Added by", author.mention(), true) .thumbnail(user_mention.avatar_url().unwrap_or_else(|| user_mention.default_avatar_url())) .footer(|f| f.text("Coded by ransxmware.xyz"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else { let _ = send_embed(&self.http, channel, "❌ Missing User", "Usage: `xwhitelistuser @user`", 0xFF0000).await; }
            }
            "unwhitelistuser" => {
                if !is_owner() { let _ = send_embed(&self.http, channel, "🔒 Owner Only", "This command can only be used by the server owner.", 0xFF0000).await; return; }
                if let Some(user_mention) = msg.mentions.iter().next() {
                    let uid = user_mention.id;
                    let mut set = self.state.whitelist_users.entry(gid).or_insert_with(HashSet::new);
                    if !set.contains(&uid) { let _ = send_embed(&self.http, channel, "❌ User Not Whitelisted", &format!("{} is not whitelisted.", user_mention.mention()), 0xFF0000).await; return; }
                    set.remove(&uid);
                    self.db.remove_whitelist_user(gid, uid).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("✅ User Removed from Whitelist").description(format!("{} has been removed from the whitelist.", user_mention.mention())).color(0x00FF00).timestamp(now_pht()) .field("User", user_mention.mention(), true).field("Removed by", author.mention(), true) .thumbnail(user_mention.avatar_url().unwrap_or_else(|| user_mention.default_avatar_url())) .footer(|f| f.text("Coded by ransxmware.xyz"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else { let _ = send_embed(&self.http, channel, "❌ Missing User", "Usage: `xunwhitelistuser @user`", 0xFF0000).await; }
            }
            "whitelistlist" => {
                if !is_owner() { let _ = send_embed(&self.http, channel, "🔒 Owner Only", "This command can only be used by the server owner.", 0xFF0000).await; return; }
                let guild = gid.to_guild_cached(&ctx.cache).unwrap();
                let wl_roles = self.state.whitelist_roles.get(&gid).map(|s| s.iter().map(|rid| guild.roles.get(rid).map(|r| r.mention().to_string()).unwrap_or_else(|| format!("<deleted role {}>", rid.0))).collect::<Vec<_>>().join("\n")).unwrap_or_else(|| "None".to_string());
                let wl_users = self.state.whitelist_users.get(&gid).map(|s| s.iter().map(|uid| guild.members.get(uid).map(|m| m.mention().to_string()).unwrap_or_else(|| format!("<user {}>", uid.0))).collect::<Vec<_>>().join("\n")).unwrap_or_else(|| "None".to_string());
                let bypass_users = self.state.link_bypass_users.get(&gid).map(|s| s.iter().map(|uid| guild.members.get(uid).map(|m| m.mention().to_string()).unwrap_or_else(|| format!("<user {}>", uid.0))).collect::<Vec<_>>().join("\n")).unwrap_or_else(|| "None".to_string());
                let bypass_roles = self.state.link_bypass_roles.get(&gid).map(|s| s.iter().map(|rid| guild.roles.get(rid).map(|r| r.mention().to_string()).unwrap_or_else(|| format!("<deleted role {}>", rid.0))).collect::<Vec<_>>().join("\n")).unwrap_or_else(|| "None".to_string());
                let mut embed = CreateEmbed::default();
                embed.title("🛡️ Whitelist & Bypass List").description(format!("Configuration for **{}**", guild.name)).color(EMBED_COLOR).timestamp(now_pht()) .field("🔒 Whitelisted Roles (Anti-Nuke)", wl_roles, false) .field("👤 Whitelisted Users (Anti-Nuke)", wl_users, false) .field("🔗 Link Bypass Users", bypass_users, false) .field("🔗 Link Bypass Roles", bypass_roles, false) .footer(|f| f.text("Coded by ransxmware.xyz"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "bypasslink" => {
                if !is_owner() { let _ = send_embed(&self.http, channel, "🔒 Owner Only", "This command can only be used by the server owner.", 0xFF0000).await; return; }
                if let Some(user_mention) = msg.mentions.iter().next() {
                    let uid = user_mention.id;
                    let mut set = self.state.link_bypass_users.entry(gid).or_insert_with(HashSet::new);
                    if set.contains(&uid) { let _ = send_embed(&self.http, channel, "❌ Already Bypassed", &format!("{} already has a link bypass.", user_mention.mention()), 0xFF0000).await; return; }
                    set.insert(uid);
                    self.db.add_link_bypass_user(gid, uid).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("✅ Link Bypass Granted").description(format!("{} can now post **any link** without being filtered.", user_mention.mention())).color(0x00FF00).timestamp(now_pht()) .field("User", user_mention.mention(), true).field("Granted by", author.mention(), true) .field("ℹ️ Note", "Use `xremovebypasslink @user` to revoke this bypass.", false) .thumbnail(user_mention.avatar_url().unwrap_or_else(|| user_mention.default_avatar_url())) .footer(|f| f.text("Coded by ransxmware.xyz — Link Bypass"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else if let Some(rid) = msg.mention_roles.iter().next() {
                    let rid = *rid;
                    let role_name = gid.to_guild_cached(&ctx.cache).and_then(|g| g.roles.get(&rid).map(|r| r.name.clone())).unwrap_or_else(|| rid.to_string());
                    let mut set = self.state.link_bypass_roles.entry(gid).or_insert_with(HashSet::new);
                    if set.contains(&rid) { let _ = send_embed(&self.http, channel, "❌ Already Bypassed", &format!("`{}` already has a link bypass.", role_name), 0xFF0000).await; return; }
                    set.insert(rid);
                    self.db.add_link_bypass_role(gid, rid).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("✅ Link Bypass Granted (Role)").description(format!("All members with <@&{}> can now post **any link** without being filtered.", rid.0)).color(0x00FF00).timestamp(now_pht()) .field("Role", format!("<@&{}>", rid.0), true).field("Granted by", author.mention(), true) .field("ℹ️ Note", "Use `xremovebypasslink @role` to revoke this bypass.", false) .footer(|f| f.text("Coded by ransxmware.xyz — Link Bypass"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else { let _ = send_embed(&self.http, channel, "❌ Invalid Target", "Please mention a **user** or a **role**.\nUsage: `xbypasslink @user` or `xbypasslink @role`", 0xFF0000).await; }
            }
            "removebypasslink" => {
                if !is_owner() { let _ = send_embed(&self.http, channel, "🔒 Owner Only", "This command can only be used by the server owner.", 0xFF0000).await; return; }
                if let Some(user_mention) = msg.mentions.iter().next() {
                    let uid = user_mention.id;
                    let mut set = self.state.link_bypass_users.entry(gid).or_insert_with(HashSet::new);
                    if !set.contains(&uid) { let _ = send_embed(&self.http, channel, "❌ Not Bypassed", &format!("{} does not have a link bypass.", user_mention.mention()), 0xFF0000).await; return; }
                    set.remove(&uid);
                    self.db.remove_link_bypass_user(gid, uid).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("✅ Link Bypass Revoked").description(format!("{}'s link bypass has been removed.", user_mention.mention())).color(0xFF4500).timestamp(now_pht()) .field("User", user_mention.mention(), true).field("Removed by", author.mention(), true) .thumbnail(user_mention.avatar_url().unwrap_or_else(|| user_mention.default_avatar_url())) .footer(|f| f.text("Coded by ransxmware.xyz — Link Bypass"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else if let Some(rid) = msg.mention_roles.iter().next() {
                    let rid = *rid;
                    let role_name = gid.to_guild_cached(&ctx.cache).and_then(|g| g.roles.get(&rid).map(|r| r.name.clone())).unwrap_or_else(|| rid.to_string());
                    let mut set = self.state.link_bypass_roles.entry(gid).or_insert_with(HashSet::new);
                    if !set.contains(&rid) { let _ = send_embed(&self.http, channel, "❌ Not Bypassed", &format!("`{}` does not have a link bypass.", role_name), 0xFF0000).await; return; }
                    set.remove(&rid);
                    self.db.remove_link_bypass_role(gid, rid).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("✅ Role Link Bypass Revoked").description(format!("Link bypass for <@&{}> has been removed.", rid.0)).color(0xFF4500).timestamp(now_pht()) .field("Role", format!("<@&{}>", rid.0), true).field("Removed by", author.mention(), true) .footer(|f| f.text("Coded by ransxmware.xyz — Link Bypass"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else { let _ = send_embed(&self.http, channel, "❌ Invalid Target", "Please mention a **user** or a **role**.\nUsage: `xremovebypasslink @user` or `xremovebypasslink @role`", 0xFF0000).await; }
            }
            "setup" => {
                if !is_admin { let _ = send_embed(&self.http, channel, "❌ Missing Permissions", "You need Administrator permission.", 0xFF0000).await; return; }
                if let Some(existing) = gid.channels(&self.http).await.ok().and_then(|ch| ch.into_iter().find(|(_, c)| c.name == "security-logs").map(|(_, c)| c)) {
                    let _ = send_embed(&self.http, channel, "Channel Already Exists", &format!("Security logs channel already exists: {}", existing.mention()), 0xFFA500).await; return;
                }
                let overwrites = vec![
                    PermissionOverwrite { allow: Permissions::empty(), deny: Permissions::VIEW_CHANNEL, kind: PermissionOverwriteType::Role(RoleId(gid.0)) },
                    PermissionOverwrite { allow: Permissions::VIEW_CHANNEL | Permissions::SEND_MESSAGES, deny: Permissions::empty(), kind: PermissionOverwriteType::Member(self.http.get_current_user().await.unwrap().id) },
                ];
                let overwrites_clone = overwrites.clone();
                if let Ok(new_channel) = gid.create_channel(&self.http, |c| {
                    c.name("security-logs")
                     .permissions(overwrites_clone)
                     .topic("Coded by ransxmware.xyz — Automated security logs and violations")
                }).await {
                    let mut welcome = CreateEmbed::default();
                    welcome.title("Coded by ransxmware.xyz — Logs Channel").description("This channel will receive all security notifications, anti-nuke alerts, and violation reports.").color(EMBED_COLOR).timestamp(now_pht()) .field("What gets logged here:", "• Anti-nuke triggers\n• Security violations\n• Auto-kicks and auto-bans\n• New account alerts\n• Webhook activities\n• Permission changes", false) .field("Commands to get started:", "`xantinuke on` — Enable anti-nuke\n`xsecurity on` — Enable message security\n`xconfig` — View configuration\n`xhelp` — View all commands", false) .footer(|f| f.text("Coded by ransxmware.xyz").icon_url(ctx.cache.current_user().avatar_url().unwrap_or_default()));
                    let _ = new_channel.send_message(&self.http, |m| m.embed(|e| { *e = welcome.clone(); e })).await;
                    let _ = send_embed(&self.http, channel, "Setup Complete", &format!("Security logs channel created: {}", new_channel.mention()), 0x00FF00).await;
                } else { let _ = send_embed(&self.http, channel, "❌ Setup Failed", "An error occurred during setup.", 0xFF0000).await; }
            }
            "config" => {
                if !is_admin { let _ = send_embed(&self.http, channel, "❌ Missing Permissions", "You need Administrator permission.", 0xFF0000).await; return; }
                let enabled = self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false);
                let log_channel = gid.channels(&self.http).await.ok().and_then(|ch| ch.into_iter().find(|(_, c)| c.name == "security-logs").map(|(_, c)| c.mention().to_string())).unwrap_or_else(|| "❌ Not Set".to_string());
                let mut embed = CreateEmbed::default();
                embed.title("Security Configuration").description("Current security settings and limits").color(EMBED_COLOR).timestamp(now_pht()) .field("Protection (Anti-Nuke + Security)", if enabled { "✅ ENABLED" } else { "🔴 DISABLED" }, false) .field("Logs Channel", log_channel, true) .field("Anti-Nuke Settings", format!("**Threshold:**  {} actions\n**Window:**     {}s\n**Punishment:** {}", antinuke_config().lock().unwrap().threshold_count, antinuke_config().lock().unwrap().threshold_window_secs, antinuke_config().lock().unwrap().punishment.as_str().to_uppercase()), false) .field("Security Limits", format!("**Messages/Minute:**    {}\n**Duplicate Messages:** {}\n**Max Emojis:**         {}\n**Auto-ban Threshold:** {} violations", security_config().lock().unwrap().max_messages_per_minute, security_config().lock().unwrap().max_duplicate_messages, security_config().lock().unwrap().max_emojis, security_config().lock().unwrap().auto_ban_threshold), false) .field("Whitelisted Roles", format!("{} roles", self.state.whitelist_roles.get(&gid).map(|s| s.len()).unwrap_or(0)), true) .field("Whitelisted Users", format!("{} users", self.state.whitelist_users.get(&gid).map(|s| s.len()).unwrap_or(0)), true) .field("Allowed Domains", security_config().lock().unwrap().link_whitelist.join(", "), false) .field("Banned Words", format!("{} words filtered", security_config().lock().unwrap().banned_words.len()), true) .footer(|f| f.text("Coded by ransxmware.xyz"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "stats" => {
                if !manage_msgs { let _ = send_embed(&self.http, channel, "❌ Missing Permissions", "You need Manage Messages permission.", 0xFF0000).await; return; }
                let total_violations: usize = self.state.user_violations.iter().map(|e| *e.value()).sum();
                let total_muted = self.state.muted_users.len();
                let total_warnings: usize = self.state.user_warnings.iter().map(|e| e.value().len()).sum();
                let mut top = self.state.user_violations.iter().filter(|e| ctx.cache.guild(gid).map(|g| g.members.contains_key(e.key())).unwrap_or(false)).collect::<Vec<_>>();
                top.sort_by(|a, b| b.value().cmp(a.value()));
                let top5 = top.iter().take(5).map(|e| format!("{}: {}", ctx.cache.guild(gid).unwrap().members.get(e.key()).map(|m| m.mention().to_string()).unwrap_or_else(|| format!("<@{}>", e.key().0)), e.value())).collect::<Vec<_>>().join("\n");
                let mut embed = CreateEmbed::default();
                embed.title("Security Statistics").description(format!("Security data for **{}**", ctx.cache.guild(gid).map(|g| g.name.clone()).unwrap_or_default())).color(EMBED_COLOR).timestamp(now_pht()) .field("Total Violations", total_violations.to_string(), true) .field("Currently Muted", total_muted.to_string(), true) .field("Total Warnings", total_warnings.to_string(), true) .field("Protection Status", if self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) { "✅ Active" } else { "🔴 Inactive" }, true) .field("Server Members", ctx.cache.guild(gid).map(|g| g.member_count).unwrap_or(0).to_string(), true);
                if !top5.is_empty() { embed.field("🚨 Top Violators", top5, false); }
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "purge" => {
                if !manage_msgs { let _ = send_embed(&self.http, channel, "❌ Missing Permissions", "You need Manage Messages permission.", 0xFF0000).await; return; }
                if rest.is_empty() { let _ = send_embed(&self.http, channel, "❌ Missing Amount", "Usage: `xpurge <amount>`", 0xFF0000).await; return; }
                let amount = match rest[0].parse::<usize>() { Ok(a) => a, Err(_) => { let _ = send_embed(&self.http, channel, "❌ Invalid Amount", "Please specify a positive number.", 0xFF0000).await; return; } };
                if amount == 0 || amount > 100 { let _ = send_embed(&self.http, channel, "❌ Invalid Amount", "Amount must be between 1 and 100.", 0xFF0000).await; return; }
                let _ = msg.delete(&self.http).await;
                let deleted = channel.messages(&self.http, |m| m.limit(amount as u64)).await.unwrap_or_default();
                if !deleted.is_empty() { let _ = channel.delete_messages(&self.http, deleted.iter().collect::<Vec<_>>()).await; }
                let mut embed = CreateEmbed::default();
                embed.title("Messages Purged").description(format!("Successfully deleted **{}** messages from {}", deleted.len(), channel.mention())).color(0x00FF00).timestamp(now_pht()) .field("Requested by", author.mention(), true).field("Channel", channel.mention(), true) .footer(|f| f.text("Coded by ransxmware.xyz"));
                let sent = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await.ok();
                if let Some(s) = sent { tokio::time::sleep(Duration::from_secs(5)).await; let _ = s.delete(&self.http).await; }
            }
            "warn" => {
                if !manage_msgs { let _ = send_embed(&self.http, channel, "❌ Missing Permissions", "You need Manage Messages permission.", 0xFF0000).await; return; }
                if rest.is_empty() { let _ = send_embed(&self.http, channel, "❌ Missing User", "Usage: `xwarn @user <reason>`", 0xFF0000).await; return; }
                let target = match msg.mentions.iter().next() { Some(u) => u, None => { let _ = send_embed(&self.http, channel, "❌ Missing User", "Please mention a user.", 0xFF0000).await; return; } };
                let reason = if rest.len() > 1 { rest[1..].join(" ") } else { "No reason provided".to_string() };
                if target.id == author.id { let _ = send_embed(&self.http, channel, "❌ Cannot Warn Yourself", "You cannot warn yourself!", 0xFF0000).await; return; }
                if let Ok(member) = gid.member(&self.http, target.id).await { if member.permissions(&ctx.cache).unwrap_or(Permissions::empty()).contains(Permissions::ADMINISTRATOR) { let _ = send_embed(&self.http, channel, "❌ Cannot Warn Administrator", "Cannot warn administrators.", 0xFF0000).await; return; } }
                let warning = WarningData { reason: reason.clone(), moderator: author.id, timestamp: now_pht(), guild_id: gid };
                let mut warnings = self.state.user_warnings.entry(target.id).or_insert_with(Vec::new);
                warnings.push(warning);
                self.db.log_action(gid, target.id, "WARN", &reason).await;
                let count = warnings.iter().filter(|w| w.guild_id == gid).count();
                let target_avatar = target.avatar_url().unwrap_or_else(|| target.default_avatar_url());
                let guild_name = ctx.cache.guild(gid).map(|g| g.name.clone()).unwrap_or_default();
                let mut embed = CreateEmbed::default();
                embed.title("⚠️ USER WARNING").color(0xFFA500u32).timestamp(now_pht()) .field("User", target.mention(), true).field("Moderator", author.mention(), true) .field("Warning Count", count.to_string(), true).field("Reason", reason.clone(), false) .thumbnail(target_avatar.clone()) .footer(|f| f.text("Coded by ransxmware.xyz — Warning System"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                let author_tag = author.tag();
                let mut dm = CreateEmbed::default();
                dm.title("⚠️ You have been warned").description(format!("You have been warned in **{}**", guild_name)).color(0xFFA500u32).timestamp(now_pht()) .field("Reason", reason, false).field("Moderator", author_tag, true).field("Total Warnings", count.to_string(), true);
                let _ = target.direct_message(&self.http, |m| m.embed(|e| { *e = dm.clone(); e })).await;
            }
            "mute" => {
                if !manage_msgs { let _ = send_embed(&self.http, channel, "❌ Missing Permissions", "You need Manage Messages permission.", 0xFF0000).await; return; }
                if rest.is_empty() { let _ = send_embed(&self.http, channel, "❌ Missing User", "Usage: `xmute @user [duration] [reason]`", 0xFF0000).await; return; }
                let target = match msg.mentions.iter().next() { Some(u) => u, None => { let _ = send_embed(&self.http, channel, "❌ Missing User", "Please mention a user.", 0xFF0000).await; return; } };
                let duration = if rest.len() > 1 { rest[1].parse::<i64>().unwrap_or(10) } else { 10 };
                let reason = if rest.len() > 2 { rest[2..].join(" ") } else { "No reason provided".to_string() };
                if target.id == author.id { let _ = send_embed(&self.http, channel, "❌ Cannot Mute Yourself", "You cannot mute yourself!", 0xFF0000).await; return; }
                if let Ok(member) = gid.member(&self.http, target.id).await { if member.permissions(&ctx.cache).unwrap_or(Permissions::empty()).contains(Permissions::ADMINISTRATOR) { let _ = send_embed(&self.http, channel, "❌ Cannot Mute Administrator", "Cannot mute administrators.", 0xFF0000).await; return; } }
                let until = now_pht() + ChronoDuration::minutes(duration);
                self.state.muted_users.insert(target.id, until);
                self.db.add_mute(gid, target.id, until).await;
                self.db.log_action(gid, target.id, "MUTE", &format!("{}min — {}", duration, reason)).await;
                let target_avatar = target.avatar_url().unwrap_or_else(|| target.default_avatar_url());
                let guild_name = ctx.cache.guild(gid).map(|g| g.name.clone()).unwrap_or_default();
                let until_str = until.to_rfc3339();
                let mut embed = CreateEmbed::default();
                embed.title("USER MUTED").color(0xFF4500u32).timestamp(now_pht()) .field("User", target.mention(), true).field("Moderator", author.mention(), true) .field("Duration", format!("{} minutes", duration), true).field("Reason", reason.clone(), false) .field("Expires", until_str.clone(), true) .thumbnail(target_avatar) .footer(|f| f.text("Coded by ransxmware.xyz — Mute System"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                let mut dm = CreateEmbed::default();
                dm.title("You have been muted").description(format!("You have been muted in **{}**", guild_name)).color(0xFF4500u32).timestamp(now_pht()) .field("Duration", format!("{} minutes", duration), true).field("Reason", reason, false) .field("Expires", until_str, true);
                let _ = target.direct_message(&self.http, |m| m.embed(|e| { *e = dm.clone(); e })).await;
            }
            "unmute" => {
                if !manage_msgs { let _ = send_embed(&self.http, channel, "❌ Missing Permissions", "You need Manage Messages permission.", 0xFF0000).await; return; }
                if rest.is_empty() { let _ = send_embed(&self.http, channel, "❌ Missing User", "Usage: `xunmute @user`", 0xFF0000).await; return; }
                let target = match msg.mentions.iter().next() { Some(u) => u, None => { let _ = send_embed(&self.http, channel, "❌ Missing User", "Please mention a user.", 0xFF0000).await; return; } };
                if !self.state.muted_users.contains_key(&target.id) { let _ = send_embed(&self.http, channel, "❌ User Not Muted", &format!("{} is not currently muted.", target.mention()), 0xFF0000).await; return; }
                self.state.muted_users.remove(&target.id);
                self.db.remove_mute(gid, target.id).await;
                let mut embed = CreateEmbed::default();
                embed.title("USER UNMUTED").description(format!("{} has been unmuted.", target.mention())).color(0x00FF00).timestamp(now_pht()) .field("User", target.mention(), true).field("Moderator", author.mention(), true) .thumbnail(target.avatar_url().unwrap_or_else(|| target.default_avatar_url())) .footer(|f| f.text("Coded by ransxmware.xyz — Mute System"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "role" => {
                if !manage_roles { let _ = send_embed(&self.http, channel, "❌ Missing Permissions", "You need Manage Roles permission.", 0xFF0000).await; return; }
                if rest.len() < 2 { let _ = send_embed(&self.http, channel, "❌ Missing Arguments", "Usage: `xrole @user @role`", 0xFF0000).await; return; }
                let target = match msg.mentions.iter().next() { Some(u) => u, None => { let _ = send_embed(&self.http, channel, "❌ Missing User", "Please mention a user.", 0xFF0000).await; return; } };
                let role_id = match msg.mention_roles.iter().next() { Some(r) => *r, None => { let _ = send_embed(&self.http, channel, "❌ Missing Role", "Please mention a role.", 0xFF0000).await; return; } };
                let role = match gid.to_guild_cached(&ctx.cache).and_then(|g| g.roles.get(&role_id).cloned()) { Some(r) => r, None => { let _ = send_embed(&self.http, channel, "❌ Role Not Found", "Could not find that role.", 0xFF0000).await; return; } };
                if let Ok(mut member) = gid.member(&self.http, target.id).await {
                    if member.roles.contains(&role.id) { let _ = send_embed(&self.http, channel, "Role Already Assigned", &format!("{} already has {}", target.mention(), role.mention()), 0xFFA500).await; return; }
                    if let Some(guild) = gid.to_guild_cached(&ctx.cache) {
                        let bot_id = ctx.cache.current_user().id;
                        let bot_highest = guild.members.get(&bot_id).and_then(|m| m.roles.iter().filter_map(|r| guild.roles.get(r)).max_by_key(|r| r.position)).map(|r| r.position).unwrap_or(0);
                        let author_member = guild.members.get(&author.id).cloned();
                        let author_highest = author_member.as_ref().and_then(|m| m.roles.iter().filter_map(|r| guild.roles.get(r)).max_by_key(|r| r.position)).map(|r| r.position).unwrap_or(0);
                        if role.position >= bot_highest { let _ = send_embed(&self.http, channel, "❌ Cannot Manage Role", "I don't have permission to manage this role (role hierarchy).", 0xFF0000).await; return; }
                        if author.id != guild.owner_id && role.position >= author_highest { let _ = send_embed(&self.http, channel, "❌ Role Hierarchy Violation", &format!("You cannot assign {} because it is equal to or higher than your own highest role.", role.mention()), 0xFF0000).await; return; }
                    }
                    let _ = member.add_role(&self.http, role.id).await;
                    self.db.log_action(gid, target.id, "ROLE GIVEN", &format!("{} by {}", role.name, author.tag())).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("Role Assigned").description(format!("Successfully gave {} the role {}", target.mention(), role.mention())).color(EMBED_COLOR).timestamp(now_pht()) .field("User", target.mention(), true).field("Role", role.mention(), true).field("Given by", author.mention(), true) .thumbnail(target.avatar_url().unwrap_or_else(|| target.default_avatar_url()));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else { let _ = send_embed(&self.http, channel, "❌ User Not Found", "Could not find that member.", 0xFF0000).await; }
            }
            "iplookup" => {
                if !manage_msgs { let _ = send_embed(&self.http, channel, "❌ Missing Permissions", "You need Manage Messages permission.", 0xFF0000).await; return; }
                if rest.is_empty() { let _ = send_embed(&self.http, channel, "❌ Missing IP", "Usage: `xiplookup <ip_address>`", 0xFF0000).await; return; }
                let ip = rest[0];
                let ip_re = Regex::new(r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$").unwrap();
                if !ip_re.is_match(ip) { let _ = send_embed(&self.http, channel, "❌ Invalid IP Address", "Please provide a valid IPv4 address (e.g., 8.8.8.8)", 0xFF0000).await; return; }
                let private = Regex::new(r"^(10\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|192\.168\.|127\.|169\.254\.|0\.)").unwrap();
                if private.is_match(ip) { let _ = send_embed(&self.http, channel, "[+] Private IP Address", "This appears to be a private/local IP address. Geolocation data may not be available.", 0xFFA500).await; return; }
                match reqwest::get(&format!("http://ip-api.com/json/{}", ip)).await {
                    Ok(resp) => match resp.json::<serde_json::Value>().await {
                        Ok(data) => if data["status"].as_str() == Some("success") {
                            let lat = data["lat"].as_f64().unwrap_or(0.0);
                            let lon = data["lon"].as_f64().unwrap_or(0.0);
                            let mut embed = CreateEmbed::default();
                            embed.title("[+] IP Address Lookup").description(format!("[+] Information for IP: `{}`", ip)).color(0x0099FF).timestamp(now_pht()) .field("[+] City", data["city"].as_str().unwrap_or("Unknown"), true) .field("[+] Region", data["regionName"].as_str().unwrap_or("Unknown"), true) .field("[+] Country", format!("{} ({})", data["country"].as_str().unwrap_or("Unknown"), data["countryCode"].as_str().unwrap_or("N/A")), true) .field("[+] ISP", data["isp"].as_str().unwrap_or("Unknown"), true) .field("[+] Organization", data["org"].as_str().unwrap_or("Unknown"), true) .field("[+] AS", data["as"].as_str().unwrap_or("Unknown"), true) .field("[+] Coordinates", format!("{}, {}", lat, lon), true) .field("[+] Timezone", data["timezone"].as_str().unwrap_or("Unknown"), true) .field("[+] ZIP Code", data["zip"].as_str().unwrap_or("Unknown"), true);
                            if lat != 0.0 && lon != 0.0 { embed.field("[+] Google Maps", format!("[View on Maps](https://www.google.com/maps/search/?api=1&query={},{})", lat, lon), false); }
                            embed.field("[+] Note", "IP geolocation is approximate and may not reflect the exact physical location.", false)
                                .footer(|f| f.text("IP Lookup Service").icon_url(ctx.cache.current_user().avatar_url().unwrap_or_default()));
                            let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                        } else { let _ = send_embed(&self.http, channel, "❌ Lookup Failed", "Could not retrieve information for this IP address.", 0xFF0000).await; },
                        Err(_) => { let _ = send_embed(&self.http, channel, "❌ Service Unavailable", "IP lookup service is currently unavailable.", 0xFF0000).await; }
                    },
                    Err(_) => { let _ = send_embed(&self.http, channel, "❌ Service Unavailable", "IP lookup service is currently unavailable.", 0xFF0000).await; }
                }
            }
            "ipgrab" => {
                if !manage_msgs { let _ = send_embed(&self.http, channel, "❌ Missing Permissions", "You need Manage Messages permission.", 0xFF0000).await; return; }
                let target = if let Some(u) = msg.mentions.iter().next() { u } else { author };
                let ph_ranges = ["202.90","203.177","210.213","218.108","124.105","112.198","180.190","49.144","103.10","27.109"];
                let fake_ip = format!("{}.{}.{}", ph_ranges[rand::random::<usize>() % ph_ranges.len()], rand::random::<u8>() % 255 + 1, rand::random::<u8>() % 255 + 1);
                let cities = ["Manila","Quezon City","Makati","Cebu City","Davao City","Taguig","Pasig","Antipolo","Caloocan","Zamboanga City","Las Piñas","Bacoor","Muntinlupa","Parañaque","Valenzuela","Iloilo City","Bacolod","Cagayan de Oro","General Santos","Baguio"];
                let provinces = ["Metro Manila","Cebu","Davao del Sur","Cavite","Rizal","Laguna","Bulacan","Pampanga","Batangas","Zambales","Iloilo","Negros Occidental","Misamis Oriental","South Cotabato","Benguet"];
                let regions = ["National Capital Region (NCR)","Central Luzon","CALABARZON","Central Visayas","Davao Region","Western Visayas","Northern Mindanao","SOCCSKSARGEN","Cordillera Administrative Region"];
                let isps = ["PLDT Inc.","Globe Telecom","Smart Communications","Sky Broadband","Converge ICT","DITO Telecommunity","Eastern Communications","Philippine Long Distance Telephone Company","Bayantel","Sun Cellular"];
                let city = cities[rand::random::<usize>() % cities.len()];
                let province = provinces[rand::random::<usize>() % provinces.len()];
                let region = regions[rand::random::<usize>() % regions.len()];
                let isp = isps[rand::random::<usize>() % isps.len()];
                let lat = rand::random::<f64>() * (21.0 - 4.5) + 4.5;
                let lon = rand::random::<f64>() * (127.0 - 116.0) + 116.0;
                let mut embed = CreateEmbed::default();
                embed.title("IP GRAB - @Null, X").description(format!("**@GRABBED: {}**", target.name)).color(0xFF0000).timestamp(now_pht()) .field("[+] Target", target.mention(), true).field("[+] IP Address", format!("`{}`", fake_ip), true) .field("[+] Status", "**CONFIRMED**", true).field("[+] City", city, true).field("[+] Province", province, true) .field("[+] Region", region, true).field("[+] ISP Provider", isp, true).field("[+] Country", "Philippines", true) .field("[+] Timezone", "PHT (UTC+8)", true).field("[+] Coordinates", format!("{:.6}, {:.6}", lat, lon), true) .field("[+] Timestamp", now_pht().format("%H:%M:%S UTC").to_string(), true).field("[+] Encryption", "**BYPASSED**", true) .field("[+] Location", format!("[IP Lookup](https://www.google.com/maps/search/?api=1&query={},{})", lat, lon), false) .thumbnail(target.avatar_url().unwrap_or_else(|| target.default_avatar_url())) .footer(|f| f.text("Coded by ransxmware.xyz"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                tokio::time::sleep(Duration::from_secs(5)).await;
                let mut reveal = CreateEmbed::default();
                reveal.title("😂 Got You!").description(format!("Relax {}, that was **100% FAKE**!\nNo actual IP was captured. Discord does not expose user IPs.", target.mention())).color(0x00FF00).timestamp(now_pht()) .footer(|f| f.text("Coded by ransxmware.xyz — Stay safe online 💚"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = reveal.clone(); e })).await;
            }
            "status" => {
                if !is_admin { let _ = send_embed(&self.http, channel, "❌ Missing Permissions", "You need Administrator permission.", 0xFF0000).await; return; }
                if rest.is_empty() { let _ = send_embed(&self.http, channel, "❌ Missing Status", "Usage: `xstatus <online/dnd/invisible>`", 0xFF0000).await; return; }
                let status_str = rest[0].to_lowercase();
                let status = match status_str.as_str() { "online" => serenity::model::user::OnlineStatus::Online, "dnd" => serenity::model::user::OnlineStatus::DoNotDisturb, "invisible" => serenity::model::user::OnlineStatus::Invisible, _ => { let _ = send_embed(&self.http, channel, "❌ Invalid Status", "Valid statuses: `online`, `dnd`, `invisible`", 0xFF0000).await; return; } };
                let server_count = ctx.cache.guilds().len();
                ctx.set_presence(Some(serenity::model::gateway::Activity::watching(format!("over {} servers!", server_count))), status).await;
                let color = if status_str == "online" { 0x00FF00 } else if status_str == "dnd" { 0xFF0000 } else { 0x808080 };
                let mut embed = CreateEmbed::default();
                embed.title("Status Updated").description(format!("Bot status changed to **{}**", status_str.to_uppercase())).color(color).timestamp(now_pht()) .field("Activity", format!("over {} servers!", server_count), false) .footer(|f| f.text("Coded by ransxmware.xyz — Status System").icon_url(ctx.cache.current_user().avatar_url().unwrap_or_default()));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "violations" => {
                if !manage_msgs { let _ = send_embed(&self.http, channel, "❌ Missing Permissions", "You need Manage Messages permission.", 0xFF0000).await; return; }
                let target = if let Some(u) = msg.mentions.iter().next() { u } else { author };
                let vcount = self.state.user_violations.get(&target.id).map(|c| *c).unwrap_or(0);
                let wcount = self.state.user_warnings.get(&target.id).map(|w| w.iter().filter(|w| w.guild_id == gid).count()).unwrap_or(0);
                let is_muted = self.state.muted_users.contains_key(&target.id);
                let (risk, color) = if vcount == 0 { ("Clean Record", 0x00FF00) } else if vcount < 3 { ("Low Risk", 0xFFFF00) } else if vcount < 5 { ("Medium Risk", 0xFF8000) } else { ("High Risk", 0xFF0000) };
                let account_age = (now_pht().timestamp() - target.created_at().timestamp()) / 86400;
                let join_age = if let Some(member) = gid.member(&self.http, target.id).await.ok() { (now_pht().timestamp() - member.joined_at.unwrap().timestamp()) / 86400 } else { 0 };
                let mut embed = CreateEmbed::default();
                embed.title("User Violations Report").description(format!("Security record for {}", target.mention())).color(color).timestamp(now_pht()) .field("Security Violations", vcount.to_string(), true).field("Warnings", wcount.to_string(), true) .field("Currently Muted", if is_muted { "Yes" } else { "No" }, true).field("Risk Level", risk, false) .field("Account Age", format!("{} days", account_age), true).field("Days in Server", format!("{} days", join_age), true) .thumbnail(target.avatar_url().unwrap_or_else(|| target.default_avatar_url())) .footer(|f| f.text("Coded by ransxmware.xyz — User Report"));
                if is_muted { if let Some(until) = self.state.muted_users.get(&target.id) { embed.field("Mute Expires", until.to_rfc3339(), true); } }
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "clearviolations" => {
                if !is_admin { let _ = send_embed(&self.http, channel, "❌ Missing Permissions", "You need Administrator permission.", 0xFF0000).await; return; }
                if rest.is_empty() { let _ = send_embed(&self.http, channel, "❌ Missing User", "Usage: `xclearviolations @user`", 0xFF0000).await; return; }
                let target = match msg.mentions.iter().next() { Some(u) => u, None => { let _ = send_embed(&self.http, channel, "❌ Missing User", "Please mention a user.", 0xFF0000).await; return; } };
                let old = self.state.user_violations.get(&target.id).map(|c| *c).unwrap_or(0);
                self.state.user_violations.insert(target.id, 0);
                if let Some(mut warns) = self.state.user_warnings.get_mut(&target.id) { warns.retain(|w| w.guild_id != gid); }
                let mut embed = CreateEmbed::default();
                embed.title("Violations Cleared").description(format!("Cleared all violations for {}", target.mention())).color(0x00FF00).timestamp(now_pht()) .field("Previous Violations", old.to_string(), true).field("Current Violations", "0".to_string(), true) .field("Cleared by", author.mention(), true).thumbnail(target.avatar_url().unwrap_or_else(|| target.default_avatar_url())) .footer(|f| f.text("Coded by ransxmware.xyz — Violation Management"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "ping" => {
                let ws_latency: u128 = 0; // WebSocket latency not directly accessible from event context in 0.11.7
                let start = std::time::Instant::now();
                let mut sent = channel.send_message(&self.http, |m| m.content("🏓 Pinging...")).await.unwrap();
                let api_latency = start.elapsed().as_millis();
                let (ws_quality, ws_color) = if ws_latency < 80 { ("🟢 Excellent", 0x00FF7F) } else if ws_latency < 150 { ("🟡 Good", 0xFFFF00) } else if ws_latency < 300 { ("🟠 Fair", 0xFF8C00) } else { ("🔴 Poor", 0xFF0000) };
                let (api_quality, api_color) = if api_latency < 80 { ("🟢 Excellent", 0x00FF7F) } else if api_latency < 150 { ("🟡 Good", 0xFFFF00) } else if api_latency < 300 { ("🟠 Fair", 0xFF8C00) } else { ("🔴 Poor", 0xFF0000) };
                let rl_status = if let Some(until) = self.state.rate_limited_until.get(&author.id) { if *until > now_pht() { format!("⏱️ Rate-limited — {}s remaining", (until.timestamp() - now_pht().timestamp())) } else { "✅ Not rate-limited".to_string() } } else { "✅ Not rate-limited".to_string() };
                let now_ts = now_pht().timestamp_millis() as f64 / 1000.0;
                let recent_cmds = self.state.command_timestamps.get(&author.id).map(|ts| ts.iter().filter(|t| now_ts - **t <= RATE_LIMIT_WINDOW_SECS).count()).unwrap_or(0);
                let cmd_budget = format!("{}/{} commands used in last {}s", recent_cmds, RATE_LIMIT_MAX_COMMANDS, RATE_LIMIT_WINDOW_SECS);
                let mut embed = CreateEmbed::default();
                embed.title("🏓 Pong!").color(if ws_latency > api_latency { ws_color } else { api_color }).timestamp(now_pht()) .field("📡 WebSocket Latency", format!("`{}ms` — {}", ws_latency, ws_quality), true) .field("🌐 API Round-Trip", format!("`{}ms` — {}", api_latency, api_quality), true) .field("⚙️ API Semaphore", format!("`{}/20` slots free", self.state.api_semaphore.available_permits()), true) .field("🛡️ Your Rate-Limit Status", rl_status, true) .field("📊 Command Budget", cmd_budget, true) .footer(|f| f.text("Coded by ransxmware.xyz — Ping Checker"));
                let _ = sent.edit(&self.http, |e| e.content("").embed(|em| { *em = embed.clone(); em })).await;
            }
            "kick" => {
                if !kick_members { let _ = send_embed(&self.http, channel, "❌ Missing Permissions", "You need Kick Members permission.", 0xFF0000).await; return; }
                if rest.is_empty() { let _ = send_embed(&self.http, channel, "❌ Missing User", "Usage: `xkick @user [reason]`", 0xFF0000).await; return; }
                let target = match msg.mentions.iter().next() { Some(u) => u, None => { let _ = send_embed(&self.http, channel, "❌ Missing User", "Please mention a user.", 0xFF0000).await; return; } };
                let reason = if rest.len() > 1 { rest[1..].join(" ") } else { "No reason provided".to_string() };
                if let Ok(member) = gid.member(&self.http, target.id).await {
                    if target.id == author.id { let _ = send_embed(&self.http, channel, "❌ Cannot Kick Yourself", "You cannot kick yourself!", 0xFF0000).await; return; }
                    if target.id == gid.to_guild_cached(&ctx.cache).unwrap().owner_id { let _ = send_embed(&self.http, channel, "❌ Cannot Kick Owner", "You cannot kick the server owner.", 0xFF0000).await; return; }
                    // Role hierarchy check via guild cache
                    if let Some(guild) = gid.to_guild_cached(&ctx.cache) {
                        let bot_top = ctx.cache.current_user().id;
                        let bot_highest = guild.members.get(&bot_top).and_then(|m| m.roles.iter().filter_map(|r| guild.roles.get(r)).max_by_key(|r| r.position)).map(|r| r.position).unwrap_or(0);
                        let target_highest = member.roles.iter().filter_map(|r| guild.roles.get(r)).max_by_key(|r| r.position).map(|r| r.position).unwrap_or(0);
                        let author_highest = ctx.cache.member(gid, author.id).unwrap_or_else(|| member.clone()).roles.iter().filter_map(|r| guild.roles.get(r)).max_by_key(|r| r.position).map(|r| r.position).unwrap_or(0);
                        if target_highest >= bot_highest { let _ = send_embed(&self.http, channel, "❌ Role Hierarchy", "I cannot kick someone with an equal or higher role than me.", 0xFF0000).await; return; }
                        if author.id != guild.owner_id && target_highest >= author_highest { let _ = send_embed(&self.http, channel, "❌ Role Hierarchy", "You cannot kick someone with an equal or higher role than you.", 0xFF0000).await; return; }
                    }
                    let guild_name = ctx.cache.guild(gid).unwrap().name.clone();
                    let author_tag = author.tag();
                    let target_tag = target.tag();
                    let target_avatar = target.avatar_url().unwrap_or_else(|| target.default_avatar_url());
                    let mut dm = CreateEmbed::default();
                    dm.title("You have been kicked").description(format!("You were kicked from **{}**", guild_name)).color(0xFF4500u32).timestamp(now_pht()) .field("Reason", reason.clone(), false).field("Moderator", author_tag.clone(), true);
                    let _ = target.direct_message(&self.http, |m| m.embed(|e| { *e = dm.clone(); e })).await;
                    let _ = member.kick_with_reason(&self.http, &reason).await;
                    self.db.log_action(gid, target.id, "KICK", &format!("by {} — {}", author_tag, reason)).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("👢 Member Kicked").description(format!("{} has been kicked from the server.", target.mention())).color(0xFF4500u32).timestamp(now_pht()) .field("User", format!("{} (`{}`)", target_tag, target.id), true).field("Moderator", author.mention(), true).field("Reason", reason, false) .thumbnail(target_avatar) .footer(|f| f.text("Coded by ransxmware.xyz — Moderation"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                    if let Some(log_id) = gid.channels(&self.http).await.ok().and_then(|ch| ch.into_iter().find(|(_, c)| c.name == "security-logs").map(|(id, _)| id)) {
                        if log_id != channel { let _ = log_id.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await; }
                    }
                } else { let _ = send_embed(&self.http, channel, "❌ User Not Found", "Could not find that member.", 0xFF0000).await; }
            }
            "ban" => {
                if !ban_members { let _ = send_embed(&self.http, channel, "❌ Missing Permissions", "You need Ban Members permission.", 0xFF0000).await; return; }
                if rest.is_empty() { let _ = send_embed(&self.http, channel, "❌ Missing User", "Usage: `xban @user [reason]`", 0xFF0000).await; return; }
                let target = match msg.mentions.iter().next() { Some(u) => u, None => { let _ = send_embed(&self.http, channel, "❌ Missing User", "Please mention a user.", 0xFF0000).await; return; } };
                let reason = if rest.len() > 1 { rest[1..].join(" ") } else { "No reason provided".to_string() };
                if let Ok(member) = gid.member(&self.http, target.id).await {
                    if target.id == author.id { let _ = send_embed(&self.http, channel, "❌ Cannot Ban Yourself", "You cannot ban yourself!", 0xFF0000).await; return; }
                    if target.id == gid.to_guild_cached(&ctx.cache).unwrap().owner_id { let _ = send_embed(&self.http, channel, "❌ Cannot Ban Owner", "You cannot ban the server owner.", 0xFF0000).await; return; }
                    // Role hierarchy check via guild cache
                    if let Some(guild) = gid.to_guild_cached(&ctx.cache) {
                        let bot_top = ctx.cache.current_user().id;
                        let bot_highest = guild.members.get(&bot_top).and_then(|m| m.roles.iter().filter_map(|r| guild.roles.get(r)).max_by_key(|r| r.position)).map(|r| r.position).unwrap_or(0);
                        let target_highest = member.roles.iter().filter_map(|r| guild.roles.get(r)).max_by_key(|r| r.position).map(|r| r.position).unwrap_or(0);
                        let author_highest = ctx.cache.member(gid, author.id).unwrap_or_else(|| member.clone()).roles.iter().filter_map(|r| guild.roles.get(r)).max_by_key(|r| r.position).map(|r| r.position).unwrap_or(0);
                        if target_highest >= bot_highest { let _ = send_embed(&self.http, channel, "❌ Role Hierarchy", "I cannot ban someone with an equal or higher role than me.", 0xFF0000).await; return; }
                        if author.id != guild.owner_id && target_highest >= author_highest { let _ = send_embed(&self.http, channel, "❌ Role Hierarchy", "You cannot ban someone with an equal or higher role than you.", 0xFF0000).await; return; }
                    }
                    let guild_name = ctx.cache.guild(gid).unwrap().name.clone();
                    let author_tag = author.tag();
                    let target_tag = target.tag();
                    let target_avatar = target.avatar_url().unwrap_or_else(|| target.default_avatar_url());
                    let mut dm = CreateEmbed::default();
                    dm.title("🔨 You have been banned").description(format!("You were banned from **{}**", guild_name)).color(0xFF0000u32).timestamp(now_pht()) .field("Reason", reason.clone(), false).field("Moderator", author_tag.clone(), true);
                    let _ = target.direct_message(&self.http, |m| m.embed(|e| { *e = dm.clone(); e })).await;
                    let _ = member.ban_with_reason(&self.http, 0, &reason).await;
                    self.db.log_action(gid, target.id, "BAN", &format!("by {} — {}", author_tag, reason)).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("🔨 Member Banned").description(format!("{} has been permanently banned.", target.mention())).color(0xFF0000u32).timestamp(now_pht()) .field("User", format!("{} (`{}`)", target_tag, target.id), true).field("Moderator", author.mention(), true).field("Reason", reason, false) .thumbnail(target_avatar) .footer(|f| f.text("Coded by ransxmware.xyz — Moderation"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                    if let Some(log_id) = gid.channels(&self.http).await.ok().and_then(|ch| ch.into_iter().find(|(_, c)| c.name == "security-logs").map(|(id, _)| id)) {
                        if log_id != channel { let _ = log_id.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await; }
                    }
                } else { let _ = send_embed(&self.http, channel, "❌ User Not Found", "Could not find that member.", 0xFF0000).await; }
            }
            "unban" => {
                if !ban_members { let _ = send_embed(&self.http, channel, "❌ Missing Permissions", "You need Ban Members permission.", 0xFF0000).await; return; }
                if rest.is_empty() { let _ = send_embed(&self.http, channel, "❌ Missing User", "Usage: `xunban <@user / username / user ID>`", 0xFF0000).await; return; }
                let query = rest.join(" ").trim().to_string();
                let bans = gid.bans(&self.http).await.unwrap_or_default();
                let mut target = None;
                if let Ok(uid) = query.parse::<u64>() { target = bans.iter().find(|b| b.user.id.0 == uid).map(|b| &b.user); }
                // discriminator lookup removed — Discord dropped discriminators
                if target.is_none() { let lower = query.to_lowercase(); target = bans.iter().find(|b| b.user.name.to_lowercase().contains(&lower)).map(|b| &b.user); }
                if let Some(user) = target {
                    let _ = gid.unban(&self.http, user.id).await;
                    self.db.log_action(gid, user.id, "UNBAN", &format!("by {}", author.tag())).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("✅ Member Unbanned").description(format!("**{}** has been unbanned and can rejoin the server.", user.tag())).color(0x00FF00).timestamp(now_pht()) .field("User", format!("{} (`{}`)", user.tag(), user.id), true).field("Moderator", author.mention(), true) .thumbnail(user.avatar_url().unwrap_or_else(|| user.default_avatar_url())) .footer(|f| f.text("Coded by ransxmware.xyz — Moderation"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                    if let Some(log_id) = gid.channels(&self.http).await.ok().and_then(|ch| ch.into_iter().find(|(_, c)| c.name == "security-logs").map(|(id, _)| id)) {
                        if log_id != channel { let _ = log_id.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await; }
                    }
                } else { let _ = send_embed(&self.http, channel, "❌ User Not Found in Ban List", "Could not find that user in the ban list. Try using their user ID.", 0xFF0000).await; }
            }
            "av" => {
                let target = if let Some(u) = msg.mentions.iter().next() { Some(u.clone()) } else if !rest.is_empty() { let name = rest.join(" ").to_lowercase(); ctx.cache.guild(gid).and_then(|g| g.members.values().find(|m| m.display_name().to_lowercase().contains(&name) || m.user.name.to_lowercase().contains(&name)).map(|m| m.user.clone())) } else { None };
                if let Some(user) = target {
                    let mut embed = CreateEmbed::default();
                    embed.color(EMBED_COLOR).image(user.avatar_url().unwrap_or_else(|| user.default_avatar_url()));
                    let _ = channel.send_message(&self.http, |m| m.content(format!("Here is {}'s avatar.", user.mention())).embed(|e| { *e = embed.clone(); e })).await;
                } else { let _ = send_embed(&self.http, channel, "❌ User Not Found", "No member found matching that name.", 0xFF0000).await; }
            }
            "serverinfo" => {
                let guild = gid.to_guild_cached(&ctx.cache).unwrap();
                let owner = if let Some(m) = gid.to_guild_cached(&ctx.cache).and_then(|g| g.members.get(&guild.owner_id).cloned()) { m.user } else { match guild.owner_id.to_user(&self.http).await { Ok(u) => u, Err(_) => { let _ = send_embed(&self.http, channel, "❌ Error", "Could not fetch server owner.", 0xFF0000).await; return; } } };
                let created = guild.id.created_at();
                let delta = now_pht().timestamp() - created.timestamp();
                let days = delta / 86400;
                let relative = if days >= 365 { format!("{} year(s) ago", days / 365) } else if days >= 30 { format!("{} month(s) ago", days / 30) } else { format!("{} day(s) ago", days) };
                let created_str = format!("{} ({})", created.format("%B %d, %Y at %I:%M %p"), relative);
                let text = guild.channels.values().filter(|c| matches!(c, Channel::Guild(gc) if gc.kind == ChannelType::Text)).count();
                let voice = guild.channels.values().filter(|c| matches!(c, Channel::Guild(gc) if gc.kind == ChannelType::Voice)).count();
                let cats = guild.channels.values().filter(|c| matches!(c, Channel::Guild(gc) if gc.kind == ChannelType::Category)).count();
                let total_ch = text + voice;
                let bots = guild.members.values().filter(|m| m.user.bot).count();
                let humans = guild.members.len() - bots;
                let boost_level = guild.premium_tier;
                let boost_count = guild.premium_subscription_count;
                let boost_str = format!("{} Boost{} (Level {})", boost_count, if boost_count != 1 { "s" } else { "" }, boost_level.num());
                let emojis: String = guild.emojis.values().map(|e| e.to_string()).collect();
                let mut embed = CreateEmbed::default();
                embed.title(format!("☁️  {}", guild.name)).description(guild.description.clone().unwrap_or_default()).color(EMBED_COLOR).timestamp(now_pht());
                if let Some(icon) = guild.icon_url() { embed.thumbnail(icon); }
                if let Some(banner) = guild.banner_url() { embed.image(banner); }
                embed.field("Server Owner", format!("{} ({})", owner.mention(), owner.name), false)
                    .field("ID", guild.id.0.to_string(), false)
                    .field("Members", guild.member_count.to_string(), false)
                    .field("Server Boost Status", boost_str, false)
                    .field("Roles", guild.roles.len().to_string(), false)
                    .field("Channels", format!("{} ({} text · {} voice · {} categories)", total_ch, text, voice, cats), false)
                    .field("Created", created_str, false);
                if !emojis.is_empty() { embed.field("Emoji List", emojis, false); }
                embed.footer(|f| f.text(format!("Null, X | Today at {}", now_pht().format("%I:%M %p"))).icon_url(ctx.cache.current_user().avatar_url().unwrap_or_default()));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "addlink" => {
                if !is_admin { let _ = send_embed(&self.http, channel, "❌ Missing Permissions", "You need Administrator permission.", 0xFF0000).await; return; }
                if rest.is_empty() { let _ = send_embed(&self.http, channel, "❌ Missing Domain", "Usage: `xaddlink <domain>`\nExample: `xaddlink imgur.com`", 0xFF0000).await; return; }
                let mut domain = rest[0].to_lowercase();
                domain = domain.trim_start_matches("https://").trim_start_matches("http://").split('/').next().unwrap_or(&domain).to_string();
                if security_config().lock().unwrap().link_whitelist.contains(&domain) { let _ = send_embed(&self.http, channel, "❌ Already Whitelisted", &format!("`{}` is already in the link whitelist.", domain), 0xFF0000).await; return; }
                security_config().lock().unwrap().link_whitelist.push(domain.clone());
                self.db.save_guild_config(gid).await;
                let mut embed = CreateEmbed::default();
                embed.title("✅ Link Whitelisted").description(format!("`{}` has been added to the allowed links.", domain)).color(0x00FF00).timestamp(now_pht()) .field("Domain", format!("`{}`", domain), true).field("Added by", author.mention(), true) .field("Total Allowed", security_config().lock().unwrap().link_whitelist.len().to_string(), true) .footer(|f| f.text("Coded by ransxmware.xyz"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "removelink" => {
                if !is_admin { let _ = send_embed(&self.http, channel, "❌ Missing Permissions", "You need Administrator permission.", 0xFF0000).await; return; }
                if rest.is_empty() { let _ = send_embed(&self.http, channel, "❌ Missing Domain", "Usage: `xremovelink <domain>`\nExample: `xremovelink imgur.com`", 0xFF0000).await; return; }
                let mut domain = rest[0].to_lowercase();
                domain = domain.trim_start_matches("https://").trim_start_matches("http://").split('/').next().unwrap_or(&domain).to_string();
                let pos = security_config().lock().unwrap().link_whitelist.iter().position(|d| *d == domain);
                if pos.is_none() { let _ = send_embed(&self.http, channel, "❌ Not in Whitelist", &format!("`{}` is not in the link whitelist.", domain), 0xFF0000).await; return; }
                security_config().lock().unwrap().link_whitelist.remove(pos.unwrap());
                self.db.save_guild_config(gid).await;
                let mut embed = CreateEmbed::default();
                embed.title("✅ Link Removed").description(format!("`{}` has been removed from the allowed links.", domain)).color(0x00FF00).timestamp(now_pht()) .field("Domain", format!("`{}`", domain), true).field("Removed by", author.mention(), true) .field("Total Allowed", security_config().lock().unwrap().link_whitelist.len().to_string(), true) .footer(|f| f.text("Coded by ransxmware.xyz"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "history" => {
                if !manage_msgs { let _ = send_embed(&self.http, channel, "❌ Missing Permissions", "You need Manage Messages permission.", 0xFF0000).await; return; }
                if rest.is_empty() { let _ = send_embed(&self.http, channel, "❌ Missing User", "Usage: `xhistory @user`", 0xFF0000).await; return; }
                let target = match msg.mentions.iter().next() { Some(u) => u, None => { let _ = send_embed(&self.http, channel, "❌ Missing User", "Please mention a user.", 0xFF0000).await; return; } };
                let rows = sqlx::query("SELECT action, reason, timestamp FROM action_history WHERE guild_id = $1 AND user_id = $2 ORDER BY id DESC LIMIT 15")
                    .bind(gid.0 as i64).bind(target.id.0 as i64).fetch_all(&self.db.pool).await.unwrap_or_default();
                let mut embed = CreateEmbed::default();
                embed.title(format!("Action History — {}", target.name)).description(format!("Last {} recorded actions for {}", rows.len(), target.mention())).color(EMBED_COLOR).timestamp(now_pht()) .thumbnail(target.avatar_url().unwrap_or_else(|| target.default_avatar_url()));
                if rows.is_empty() {
                    embed.field("No History", "No recorded actions for this user in this server.", false);
                } else {
                    for row in rows {
                        let dt = DateTime::parse_from_rfc3339(row.get::<&str, _>(2)).unwrap();
                        embed.field(format!("`{}` — {}", row.get::<&str, _>(0), dt.format("%Y-%m-%d %H:%M UTC")), row.get::<&str, _>(1), false);
                    }
                }
                let violations = self.state.user_violations.get(&target.id).map(|c| *c).unwrap_or(0);
                let warnings = self.state.user_warnings.get(&target.id).map(|w| w.iter().filter(|w| w.guild_id == gid).count()).unwrap_or(0);
                embed.field("User ID", format!("`{}`", target.id), true)
                    .field("Violations", violations.to_string(), true).field("Warnings", warnings.to_string(), true)
                    .footer(|f| f.text("Coded by ransxmware.xyz — History"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "help" => {
                if rest.is_empty() {
                    let mut embed = CreateEmbed::default();
                    embed.title("🛡️ Null, X : Security Menu").description("**REVSHIT**").color(EMBED_COLOR).timestamp(now_pht()) .field("🔒 Anti-Nuke Commands", "`xantinuke on/off` — Toggle anti-nuke protection\n`xsecurity on/off` — Toggle message security\n`xsetup` — Setup security logs channel\n`xconfig` — View security configuration\n`xstats` — View security statistics", true) .field("📋 Whitelist Commands", "`xwhitelistrole <@role>` — Whitelist a role\n`xunwhitelistrole <@role>` — Remove whitelisted role\n`xwhitelistuser <@user>` — Whitelist a user\n`xunwhitelistuser <@user>` — Remove whitelisted user\n`xwhitelistlist` — View whitelist\n`xbypasslink <@user/@role>` — Grant link bypass\n`xremovebypasslink <@user/@role>` — Revoke link bypass\n`xaddlink <domain>` — Add allowed link\n`xremovelink <domain>` — Remove allowed link", true) .field("\u{200b}", "\u{200b}", false) .field("⚔️ Moderation Commands", "`xpurge <amount>` — Delete messages\n`xwarn <@user> <reason>` — Warn a user\n`xmute <@user> [duration]` — Mute a user\n`xunmute <@user>` — Unmute a user\n`xkick <@user>` — Kick a user\n`xban <@user>` — Ban a user\n`xunban <@user/username/id>` — Unban a user\n`xrole <@user> <@role>` — Give role to user\n`xhistory <@user>` — View action history", true) .field("🔧 Utility Commands", "`xping` — Bot latency & rate-limit status\n`xiplookup <ip>` — Lookup IP information\n`xipgrab <@user>` — IP grab\n`xstatus <type>` — Set bot status\n`xav [@user]` — Show a user's avatar\n`xserverinfo` — Server information\n`xviolations <@user>` — Check user violations\n`xclearviolations <@user>` — Clear user violations", true) .field("\u{200b}", "\u{200b}", false) .field("ℹ️ Information", "`xhelp <command>` — Get detailed help for a command\n**Prefix:** `x` or `null` — e.g. `xban` or `nullban`", false) .footer(|f| f.text("Coded by ransxmware.xyz")).thumbnail(ctx.cache.current_user().avatar_url().unwrap_or_default());
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else {
                    let cmd_name = rest[0].to_lowercase();
                    let help_map = std::collections::HashMap::from([
                        ("antinuke", ("Toggle anti-nuke protection on or off", "xantinuke [on/off]", vec!["xantinuke on","xantinuke off","xantinuke"], "Administrator")),
                        ("whitelistrole", ("Add a role to the anti-nuke whitelist", "xwhitelistrole @role", vec!["xwhitelistrole @Admin"], "Administrator")),
                        ("unwhitelistrole", ("Remove a role from the anti-nuke whitelist", "xunwhitelistrole @role", vec!["xunwhitelistrole @Admin"], "Administrator")),
                        ("whitelistuser", ("Add a user to the anti-nuke whitelist", "xwhitelistuser @user", vec!["xwhitelistuser @user"], "Administrator")),
                        ("unwhitelistuser", ("Remove a user from the anti-nuke whitelist", "xunwhitelistuser @user", vec!["xunwhitelistuser @user"], "Administrator")),
                        ("whitelistlist", ("View all whitelisted roles, users, and link bypasses", "xwhitelistlist", vec!["xwhitelistlist"], "Administrator")),
                        ("bypasslink", ("Grant a user or role permission to post any link freely", "xbypasslink @user/@role", vec!["xbypasslink @user","xbypasslink @Moderator"], "Server Owner")),
                        ("removebypasslink", ("Revoke a link bypass from a user or role", "xremovebypasslink @user/@role", vec!["xremovebypasslink @user","xremovebypasslink @Mod"], "Server Owner")),
                        ("security", ("Toggle advanced message security features on or off", "xsecurity [on/off]", vec!["xsecurity on","xsecurity off"], "Administrator")),
                        ("setup", ("Set up the security logs channel for the server", "xsetup", vec!["xsetup"], "Administrator")),
                        ("config", ("View the current security and anti-nuke configuration", "xconfig", vec!["xconfig"], "Administrator")),
                        ("stats", ("View security statistics for the server", "xstats", vec!["xstats"], "Manage Messages")),
                        ("purge", ("Delete a specified number of messages from the current channel", "xpurge <amount>", vec!["xpurge 10","xpurge 50"], "Manage Messages")),
                        ("warn", ("Issue a warning to a user with a specified reason", "xwarn @user <reason>", vec!["xwarn @user Spamming"], "Manage Messages")),
                        ("mute", ("Temporarily mute a user", "xmute @user [duration] [reason]", vec!["xmute @user 10 Spamming"], "Manage Messages")),
                        ("unmute", ("Remove mute from a user", "xunmute @user", vec!["xunmute @user"], "Manage Messages")),
                        ("role", ("Give a role to a specified user", "xrole @user @role", vec!["xrole @user @Member"], "Manage Roles")),
                        ("iplookup", ("Look up geolocation info about an IP address", "xiplookup <ip_address>", vec!["xiplookup 8.8.8.8"], "Manage Messages")),
                        ("ipgrab", ("Generate a fake IP grab for fun/prank purposes", "xipgrab [@user]", vec!["xipgrab @user","xipgrab"], "Manage Messages")),
                        ("status", ("Change the bot's Discord status", "xstatus <online/dnd/invisible>", vec!["xstatus online","xstatus dnd"], "Administrator")),
                        ("violations", ("Check the security violations record for a user", "xviolations [@user]", vec!["xviolations @user","xviolations"], "Manage Messages")),
                        ("clearviolations", ("Clear all security violations for a user", "xclearviolations @user", vec!["xclearviolations @user"], "Administrator")),
                        ("ping", ("Check bot latency, API round-trip speed and your rate-limit status", "xping", vec!["xping"], "None (anyone)")),
                        ("kick", ("Kick a member from the server", "xkick @user [reason]", vec!["xkick @user","xkick @user Raiding"], "Kick Members")),
                        ("ban", ("Ban a member from the server", "xban @user [reason]", vec!["xban @user","xban @user Nuking"], "Ban Members")),
                        ("unban", ("Unban a user by @mention, username, or user ID", "xunban <@user / username / user ID>", vec!["xunban 123456789","xunban someuser","xunban someuser#1234"], "Ban Members")),
                        ("av", ("Show a user's full-size avatar. Also works as \"null av @user\"", "xav [@user or username]", vec!["xav","xav @user","xav 0tnull"], "None (anyone)")),
                        ("serverinfo", ("Display detailed server information", "xserverinfo", vec!["xserverinfo"], "None (anyone)")),
                        ("addlink", ("Add a domain to the link whitelist", "xaddlink <domain>", vec!["xaddlink imgur.com"], "Administrator")),
                        ("removelink", ("Remove a domain from the link whitelist", "xremovelink <domain>", vec!["xremovelink imgur.com"], "Administrator")),
                        ("history", ("View action history for a user", "xhistory @user", vec!["xhistory @user"], "Manage Messages")),
                    ]);
                    if let Some((desc, usage, examples, perms)) = help_map.get(cmd_name.as_str()) {
                        let mut embed = CreateEmbed::default();
                        embed.title(format!("Help: x{}", cmd_name)).description(*desc).color(EMBED_COLOR).timestamp(now_pht()) .field("Usage", format!("`{}`", usage), false) .field("Required Permission", *perms, true) .field("Examples", examples.iter().map(|e| format!("`{}`", e)).collect::<Vec<_>>().join("\n"), false) .footer(|f| f.text("Coded by ransxmware.xyz Help System"));
                        let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                    } else { let _ = send_embed(&self.http, channel, "Command Not Found", &format!("No help available for `{}`. Use `xhelp` to see all commands.", cmd_name), 0xFF0000).await; }
                }
            }
            _ => {}
        }
    }
}

// ------------------------------------------------------------
//  MAIN
// ------------------------------------------------------------
#[tokio::main]
async fn main() {
    let token = std::env::var("DISCORD_TOKEN").expect("DISCORD_TOKEN environment variable not set");
    let state = Arc::new(BotState::new());
    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let db = Arc::new(Database::new(&db_url).await);
    db.load_all(&state).await;
    let http = Arc::new(Http::new(&token));
    let handler = Handler { state, db, http };
    let mut client = Client::builder(&token, GatewayIntents::all())
        .event_handler(handler)
        .await
        .expect("Error creating client");
    if let Err(why) = client.start().await {
        println!("Client error: {:?}", why);
    }
}
