// ============================================================
//  REXZ-STYLE ANTI-NUKE BOT – FULLY FEATURED (FIXED)
// ============================================================
#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused_imports)]
#![recursion_limit = "256"]

use serenity::{
    async_trait,
    builder::CreateEmbed,
    cache::Cache,
    client::{Client, Context, EventHandler},
    http::Http,
    model::{
        channel::{Channel, ChannelType, GuildChannel, PermissionOverwrite, PermissionOverwriteType},
        gateway::GatewayIntents,
        guild::{Guild, Member, Role, UnavailableGuild},
        id::{ChannelId, GuildId, MessageId, RoleId, UserId},
        permissions::Permissions,
        prelude::*,
        user::User,
        Timestamp,
    },
};
use tokio::sync::Semaphore;
use dashmap::{DashMap, DashSet};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use regex::Regex;
use sqlx::{PgPool, postgres::PgPoolOptions};
use reqwest;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashSet, VecDeque},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use poise::serenity_prelude as serenity_poise;
use poise::serenity_prelude::Mentionable;
use rand::Rng;

// ------------------------------------------------------------
//  ENUM CONVERSION HELPERS
// ------------------------------------------------------------
fn verification_level_to_u16(vl: serenity::model::guild::VerificationLevel) -> u16 {
    use serenity::model::guild::VerificationLevel::*;
    match vl {
        None => 0, Low => 1, Medium => 2, High => 3, Higher => 4, _ => 0,
    }
}
fn notification_level_to_u16(n: serenity::model::guild::DefaultMessageNotificationLevel) -> u16 {
    use serenity::model::guild::DefaultMessageNotificationLevel::*;
    match n { All => 0, Mentions => 1, _ => 0 }
}
fn explicit_filter_to_u16(e: serenity::model::guild::ExplicitContentFilter) -> u16 {
    use serenity::model::guild::ExplicitContentFilter::*;
    match e { None => 0, WithoutRole => 1, All => 2, _ => 0 }
}
fn premium_tier_num(t: serenity::model::guild::PremiumTier) -> u8 {
    use serenity::model::guild::PremiumTier::*;
    match t { Tier1 => 1, Tier2 => 2, Tier3 => 3, _ => 0 }
}

// ------------------------------------------------------------
//  CONSTANTS
// ------------------------------------------------------------
fn pht_offset() -> chrono::FixedOffset {
    chrono::FixedOffset::east_opt(8 * 3600).unwrap()
}
fn now_pht() -> DateTime<chrono::FixedOffset> {
    Utc::now().with_timezone(&pht_offset())
}
fn now_ts() -> Timestamp {
    Timestamp::from_unix_timestamp(Utc::now().timestamp())
        .expect("system clock produced out-of-range timestamp")
}

const DANGEROUS_PERMISSIONS: [Permissions; 7] = [
    Permissions::ADMINISTRATOR, Permissions::MANAGE_GUILD,
    Permissions::MANAGE_ROLES, Permissions::MANAGE_CHANNELS,
    Permissions::MANAGE_WEBHOOKS, Permissions::BAN_MEMBERS,
    Permissions::KICK_MEMBERS,
];

const EMBED_COLOR: u32 = 0x000000;
const ANTINUKE_ASCII: &str = r#"
  _   _       _ _   ____        _
 | \ | |_   _| | | | __ )  ___ | |_
 |  \| | | | | | | |  _ \ / _ \| __|
 | |\  | |_| | | | | |_) | (_) | |_
 |_| \_|\__,_|_|_| |____/ \___/ \__|
"#;

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

// ------------------------------------------------------------
//  TYPES & SNAPSHOTS
// ------------------------------------------------------------
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Punishment { Ban, Kick, Strip }
impl Punishment {
    fn as_str(&self) -> &'static str {
        match self {
            Punishment::Ban => "ban",
            Punishment::Kick => "kick",
            Punishment::Strip => "strip",
        }
    }
}
impl FromStr for Punishment {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, ()> {
        match s {
            "ban" => Ok(Punishment::Ban),
            "kick" => Ok(Punishment::Kick),
            "strip" => Ok(Punishment::Strip),
            _ => Err(()),
        }
    }
}

#[derive(Clone)]
struct GuildSecurityConfig {
    pub mass_ban_threshold: usize,
    pub mass_ban_window_secs: f64,
    pub mass_kick_threshold: usize,
    pub mass_kick_window_secs: f64,
    pub mass_channel_create_threshold: usize,
    pub mass_channel_create_window_secs: f64,
    pub mass_role_create_threshold: usize,
    pub mass_role_create_window_secs: f64,
    pub punishment: Punishment,
    pub max_messages_per_minute: usize,
    pub max_duplicate_messages: usize,
    pub max_emojis: usize,
    pub auto_ban_threshold: usize,
    pub link_whitelist: Vec<String>,
    pub banned_words: Vec<String>,
    pub second_owner_id: Option<UserId>,
}
impl Default for GuildSecurityConfig {
    fn default() -> Self {
        Self {
            mass_ban_threshold: 5,
            mass_ban_window_secs: 10.0,
            mass_kick_threshold: 5,
            mass_kick_window_secs: 10.0,
            mass_channel_create_threshold: 5,
            mass_channel_create_window_secs: 10.0,
            mass_role_create_threshold: 5,
            mass_role_create_window_secs: 10.0,
            punishment: Punishment::Ban,
            max_messages_per_minute: 15,
            max_duplicate_messages: 3,
            max_emojis: 5,
            auto_ban_threshold: 5,
            link_whitelist: vec![
                "youtube.com".to_string(), "youtu.be".to_string(),
                "github.com".to_string(), "open.spotify.com".to_string(),
                "tenor.com".to_string(), "giphy.com".to_string(),
            ],
            banned_words: vec![
                "spam".to_string(), "hack".to_string(), "cheat".to_string(),
                "discord.gg".to_string(),
            ],
            second_owner_id: None,
        }
    }
}

#[derive(Clone)]
struct WarningData {
    reason: String,
    moderator: UserId,
    timestamp: DateTime<chrono::FixedOffset>,
    guild_id: GuildId,
}

#[derive(Clone)]
struct ChannelSnapshot {
    name: String,
    category_id: Option<ChannelId>,
    position: i32,
    channel_type: ChannelType,
    overwrites: Vec<PermissionOverwrite>,
    topic: Option<String>,
    nsfw: bool,
    slowmode_delay: u64,
}
#[derive(Clone)]
struct GuildSnapshot {
    name: String,
    description: Option<String>,
    icon: Option<String>,
    banner: Option<String>,
    afk_channel_id: Option<ChannelId>,
    afk_timeout: u64,
    verification_level: u16,
    default_notifications: u16,
    explicit_content_filter: u16,
    system_channel_id: Option<ChannelId>,
}
#[derive(Clone)]
struct RoleSnapshot {
    name: String,
    permissions: u64,
    colour: u32,
    hoist: bool,
    mentionable: bool,
}
#[derive(Clone)]
struct ServerAdEntry {
    invite_code: String,
    channel_id: ChannelId,
    message_id: MessageId,
    timestamp: f64,
}

// ------------------------------------------------------------
//  GLOBAL STATE
// ------------------------------------------------------------
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
    guild_configs: DashMap<GuildId, GuildSecurityConfig>,
}
impl BotState {
    fn new() -> Self {
        Self {
            protection_enabled: DashMap::new(),
            whitelist_roles: DashMap::new(),
            whitelist_users: DashMap::new(),
            link_bypass_users: DashMap::new(),
            link_bypass_roles: DashMap::new(),
            muted_users: DashMap::new(),
            user_violations: DashMap::new(),
            user_message_times: DashMap::new(),
            user_messages: DashMap::new(),
            user_warnings: DashMap::new(),
            action_log: DashMap::new(),
            mass_action_log: DashMap::new(),
            confirmed_actors: DashMap::new(),
            ban_in_progress: DashMap::new(),
            rollback_queue: DashMap::new(),
            drain_scheduled: DashMap::new(),
            restoring: DashMap::new(),
            edit_logged: DashMap::new(),
            handled_channel_creates: DashMap::new(),
            handled_guild_updates: DashMap::new(),
            handled_webhook_events: DashMap::new(),
            handled_role_events: DashMap::new(),
            role_restore_locks: DashMap::new(),
            dangerous_members: DashMap::new(),
            command_timestamps: DashMap::new(),
            rate_limited_until: DashMap::new(),
            audit_prefetch: DashMap::new(),
            guild_snapshots: DashMap::new(),
            channel_snapshots: DashMap::new(),
            role_snapshots: DashMap::new(),
            server_ad_registry: DashMap::new(),
            ad_spam_channels: DashMap::new(),
            api_semaphore: Arc::new(Semaphore::new(20)),
            guild_configs: DashMap::new(),
        }
    }
}

// ------------------------------------------------------------
//  DATABASE
// ------------------------------------------------------------
struct Database {
    pool: PgPool,
}
impl Database {
    async fn new(url: &str) -> Self {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .expect("Failed to connect to Postgres");
        let queries = vec![
            "CREATE TABLE IF NOT EXISTS protection ( guild_id BIGINT PRIMARY KEY, enabled INTEGER NOT NULL DEFAULT 0 )",
            "CREATE TABLE IF NOT EXISTS whitelist_users ( guild_id BIGINT NOT NULL, user_id BIGINT NOT NULL, PRIMARY KEY (guild_id, user_id) )",
            "CREATE TABLE IF NOT EXISTS whitelist_roles ( guild_id BIGINT NOT NULL, role_id BIGINT NOT NULL, PRIMARY KEY (guild_id, role_id) )",
            "CREATE TABLE IF NOT EXISTS muted_users ( guild_id BIGINT NOT NULL, user_id BIGINT NOT NULL, until_ts TEXT NOT NULL, PRIMARY KEY (guild_id, user_id) )",
            r#"CREATE TABLE IF NOT EXISTS guild_config (
                guild_id BIGINT PRIMARY KEY,
                mass_ban_threshold BIGINT NOT NULL DEFAULT 5,
                mass_ban_window_secs FLOAT NOT NULL DEFAULT 10.0,
                mass_kick_threshold BIGINT NOT NULL DEFAULT 5,
                mass_kick_window_secs FLOAT NOT NULL DEFAULT 10.0,
                mass_channel_create_threshold BIGINT NOT NULL DEFAULT 5,
                mass_channel_create_window_secs FLOAT NOT NULL DEFAULT 10.0,
                mass_role_create_threshold BIGINT NOT NULL DEFAULT 5,
                mass_role_create_window_secs FLOAT NOT NULL DEFAULT 10.0,
                punishment TEXT NOT NULL DEFAULT 'ban',
                max_messages_per_minute BIGINT NOT NULL DEFAULT 15,
                max_duplicate_messages BIGINT NOT NULL DEFAULT 3,
                max_emojis BIGINT NOT NULL DEFAULT 5,
                auto_ban_threshold BIGINT NOT NULL DEFAULT 5,
                link_whitelist TEXT NOT NULL DEFAULT '["youtube.com","github.com"]',
                banned_words TEXT NOT NULL DEFAULT '["spam","hack","cheat","discord.gg"]',
                second_owner_id BIGINT
            )"#,
            "CREATE TABLE IF NOT EXISTS link_bypass_users ( guild_id BIGINT NOT NULL, user_id BIGINT NOT NULL, PRIMARY KEY (guild_id, user_id) )",
            "CREATE TABLE IF NOT EXISTS link_bypass_roles ( guild_id BIGINT NOT NULL, role_id BIGINT NOT NULL, PRIMARY KEY (guild_id, role_id) )",
            "CREATE TABLE IF NOT EXISTS action_history ( id SERIAL PRIMARY KEY, guild_id BIGINT NOT NULL, user_id BIGINT NOT NULL, action TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '', timestamp TEXT NOT NULL )",
        ];
        for stmt in queries {
            if let Err(e) = sqlx::query(stmt).execute(&pool).await {
                println!("[DB INIT ERROR] {}", e);
            }
        }
        Self { pool }
    }

    async fn load_all(&self, state: &BotState) {
        // protection
        {
            let rows = sqlx::query("SELECT guild_id, enabled FROM protection")
                .fetch_all(&self.pool).await.unwrap_or_default();
            for row in rows {
                use sqlx::Row;
                let gid = GuildId(row.get::<i64, _>("guild_id") as u64);
                let enabled: i32 = row.get("enabled");
                state.protection_enabled.insert(gid, enabled != 0);
            }
        }
        // whitelist_users
        {
            let rows = sqlx::query("SELECT guild_id, user_id FROM whitelist_users")
                .fetch_all(&self.pool).await.unwrap_or_default();
            for row in rows {
                use sqlx::Row;
                let gid = GuildId(row.get::<i64, _>("guild_id") as u64);
                let uid = UserId(row.get::<i64, _>("user_id") as u64);
                state.whitelist_users.entry(gid).or_insert_with(HashSet::new).insert(uid);
            }
        }
        // whitelist_roles
        {
            let rows = sqlx::query("SELECT guild_id, role_id FROM whitelist_roles")
                .fetch_all(&self.pool).await.unwrap_or_default();
            for row in rows {
                use sqlx::Row;
                let gid = GuildId(row.get::<i64, _>("guild_id") as u64);
                let rid = RoleId(row.get::<i64, _>("role_id") as u64);
                state.whitelist_roles.entry(gid).or_insert_with(HashSet::new).insert(rid);
            }
        }
        // link_bypass_users
        {
            let rows = sqlx::query("SELECT guild_id, user_id FROM link_bypass_users")
                .fetch_all(&self.pool).await.unwrap_or_default();
            for row in rows {
                use sqlx::Row;
                let gid = GuildId(row.get::<i64, _>("guild_id") as u64);
                let uid = UserId(row.get::<i64, _>("user_id") as u64);
                state.link_bypass_users.entry(gid).or_insert_with(HashSet::new).insert(uid);
            }
        }
        // link_bypass_roles
        {
            let rows = sqlx::query("SELECT guild_id, role_id FROM link_bypass_roles")
                .fetch_all(&self.pool).await.unwrap_or_default();
            for row in rows {
                use sqlx::Row;
                let gid = GuildId(row.get::<i64, _>("guild_id") as u64);
                let rid = RoleId(row.get::<i64, _>("role_id") as u64);
                state.link_bypass_roles.entry(gid).or_insert_with(HashSet::new).insert(rid);
            }
        }
        // muted_users
        {
            let now = now_pht();
            let rows = sqlx::query("SELECT guild_id, user_id, until_ts FROM muted_users")
                .fetch_all(&self.pool).await.unwrap_or_default();
            for row in rows {
                use sqlx::Row;
                let gid_raw: i64 = row.get("guild_id");
                let uid = UserId(row.get::<i64, _>("user_id") as u64);
                let until_str: String = row.get("until_ts");
                if let Ok(until) = DateTime::parse_from_rfc3339(&until_str) {
                    if until > now {
                        state.muted_users.insert(uid, until);
                    } else {
                        let _ = sqlx::query("DELETE FROM muted_users WHERE user_id = $1 AND guild_id = $2")
                            .bind(row.get::<i64, _>("user_id"))
                            .bind(gid_raw)
                            .execute(&self.pool).await;
                    }
                }
            }
        }
        // guild_config
        {
            let rows = sqlx::query("SELECT guild_id, mass_ban_threshold, mass_ban_window_secs, mass_kick_threshold, mass_kick_window_secs, mass_channel_create_threshold, mass_channel_create_window_secs, mass_role_create_threshold, mass_role_create_window_secs, punishment, max_messages_per_minute, max_duplicate_messages, max_emojis, auto_ban_threshold, link_whitelist, banned_words, second_owner_id FROM guild_config")
                .fetch_all(&self.pool).await.unwrap_or_default();
            for row in rows {
                use sqlx::Row;
                let config = GuildSecurityConfig {
                    mass_ban_threshold:           row.get::<i64, _>("mass_ban_threshold") as usize,
                    mass_ban_window_secs:         row.get::<f64, _>("mass_ban_window_secs"),
                    mass_kick_threshold:          row.get::<i64, _>("mass_kick_threshold") as usize,
                    mass_kick_window_secs:        row.get::<f64, _>("mass_kick_window_secs"),
                    mass_channel_create_threshold: row.get::<i64, _>("mass_channel_create_threshold") as usize,
                    mass_channel_create_window_secs: row.get::<f64, _>("mass_channel_create_window_secs"),
                    mass_role_create_threshold:   row.get::<i64, _>("mass_role_create_threshold") as usize,
                    mass_role_create_window_secs: row.get::<f64, _>("mass_role_create_window_secs"),
                    punishment: Punishment::from_str(row.get::<&str, _>("punishment")).unwrap_or(Punishment::Ban),
                    max_messages_per_minute:      row.get::<i64, _>("max_messages_per_minute") as usize,
                    max_duplicate_messages:       row.get::<i64, _>("max_duplicate_messages") as usize,
                    max_emojis:                   row.get::<i64, _>("max_emojis") as usize,
                    auto_ban_threshold:           row.get::<i64, _>("auto_ban_threshold") as usize,
                    link_whitelist: serde_json::from_str(row.get::<&str, _>("link_whitelist")).unwrap_or_default(),
                    banned_words:   serde_json::from_str(row.get::<&str, _>("banned_words")).unwrap_or_default(),
                    second_owner_id: row.get::<Option<i64>, _>("second_owner_id").map(|id| UserId(id as u64)),
                };
                state.guild_configs.insert(GuildId(row.get::<i64, _>("guild_id") as u64), config);
            }
        }
        println!("[DB] All data loaded.");
    }

    async fn save_guild_config(&self, gid: GuildId, cfg: &GuildSecurityConfig) {
        let link_wl = serde_json::to_string(&cfg.link_whitelist).unwrap();
        let banned_w = serde_json::to_string(&cfg.banned_words).unwrap();
        let second_owner = cfg.second_owner_id.map(|id| id.0 as i64);
        let _ = sqlx::query(
            r#"INSERT INTO guild_config (
                guild_id, mass_ban_threshold, mass_ban_window_secs,
                mass_kick_threshold, mass_kick_window_secs,
                mass_channel_create_threshold, mass_channel_create_window_secs,
                mass_role_create_threshold, mass_role_create_window_secs,
                punishment, max_messages_per_minute, max_duplicate_messages,
                max_emojis, auto_ban_threshold, link_whitelist, banned_words, second_owner_id
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
            ON CONFLICT (guild_id) DO UPDATE SET
                mass_ban_threshold = EXCLUDED.mass_ban_threshold,
                mass_ban_window_secs = EXCLUDED.mass_ban_window_secs,
                mass_kick_threshold = EXCLUDED.mass_kick_threshold,
                mass_kick_window_secs = EXCLUDED.mass_kick_window_secs,
                mass_channel_create_threshold = EXCLUDED.mass_channel_create_threshold,
                mass_channel_create_window_secs = EXCLUDED.mass_channel_create_window_secs,
                mass_role_create_threshold = EXCLUDED.mass_role_create_threshold,
                mass_role_create_window_secs = EXCLUDED.mass_role_create_window_secs,
                punishment = EXCLUDED.punishment,
                max_messages_per_minute = EXCLUDED.max_messages_per_minute,
                max_duplicate_messages = EXCLUDED.max_duplicate_messages,
                max_emojis = EXCLUDED.max_emojis,
                auto_ban_threshold = EXCLUDED.auto_ban_threshold,
                link_whitelist = EXCLUDED.link_whitelist,
                banned_words = EXCLUDED.banned_words,
                second_owner_id = EXCLUDED.second_owner_id"#
        )
        .bind(gid.0 as i64)
        .bind(cfg.mass_ban_threshold as i64)
        .bind(cfg.mass_ban_window_secs)
        .bind(cfg.mass_kick_threshold as i64)
        .bind(cfg.mass_kick_window_secs)
        .bind(cfg.mass_channel_create_threshold as i64)
        .bind(cfg.mass_channel_create_window_secs)
        .bind(cfg.mass_role_create_threshold as i64)
        .bind(cfg.mass_role_create_window_secs)
        .bind(cfg.punishment.as_str())
        .bind(cfg.max_messages_per_minute as i64)
        .bind(cfg.max_duplicate_messages as i64)
        .bind(cfg.max_emojis as i64)
        .bind(cfg.auto_ban_threshold as i64)
        .bind(&link_wl)
        .bind(&banned_w)
        .bind(second_owner)
        .execute(&self.pool).await;
    }

    async fn set_protection(&self, gid: GuildId, en: bool) {
        let _ = sqlx::query(
            "INSERT INTO protection(guild_id, enabled) VALUES ($1, $2) ON CONFLICT (guild_id) DO UPDATE SET enabled = EXCLUDED.enabled"
        )
        .bind(gid.0 as i64)
        .bind(en as i32)
        .execute(&self.pool).await;
    }

    async fn add_whitelist_user(&self, gid: GuildId, uid: UserId) {
        let _ = sqlx::query(
            "INSERT INTO whitelist_users(guild_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING"
        ).bind(gid.0 as i64).bind(uid.0 as i64).execute(&self.pool).await;
    }
    async fn remove_whitelist_user(&self, gid: GuildId, uid: UserId) {
        let _ = sqlx::query(
            "DELETE FROM whitelist_users WHERE guild_id = $1 AND user_id = $2"
        ).bind(gid.0 as i64).bind(uid.0 as i64).execute(&self.pool).await;
    }
    async fn add_whitelist_role(&self, gid: GuildId, rid: RoleId) {
        let _ = sqlx::query(
            "INSERT INTO whitelist_roles(guild_id, role_id) VALUES ($1, $2) ON CONFLICT DO NOTHING"
        ).bind(gid.0 as i64).bind(rid.0 as i64).execute(&self.pool).await;
    }
    async fn remove_whitelist_role(&self, gid: GuildId, rid: RoleId) {
        let _ = sqlx::query(
            "DELETE FROM whitelist_roles WHERE guild_id = $1 AND role_id = $2"
        ).bind(gid.0 as i64).bind(rid.0 as i64).execute(&self.pool).await;
    }
    async fn add_link_bypass_user(&self, gid: GuildId, uid: UserId) {
        let _ = sqlx::query(
            "INSERT INTO link_bypass_users(guild_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING"
        ).bind(gid.0 as i64).bind(uid.0 as i64).execute(&self.pool).await;
    }
    async fn remove_link_bypass_user(&self, gid: GuildId, uid: UserId) {
        let _ = sqlx::query(
            "DELETE FROM link_bypass_users WHERE guild_id = $1 AND user_id = $2"
        ).bind(gid.0 as i64).bind(uid.0 as i64).execute(&self.pool).await;
    }
    async fn add_link_bypass_role(&self, gid: GuildId, rid: RoleId) {
        let _ = sqlx::query(
            "INSERT INTO link_bypass_roles(guild_id, role_id) VALUES ($1, $2) ON CONFLICT DO NOTHING"
        ).bind(gid.0 as i64).bind(rid.0 as i64).execute(&self.pool).await;
    }
    async fn remove_link_bypass_role(&self, gid: GuildId, rid: RoleId) {
        let _ = sqlx::query(
            "DELETE FROM link_bypass_roles WHERE guild_id = $1 AND role_id = $2"
        ).bind(gid.0 as i64).bind(rid.0 as i64).execute(&self.pool).await;
    }
    async fn add_mute(&self, gid: GuildId, uid: UserId, until: DateTime<chrono::FixedOffset>) {
        let _ = sqlx::query(
            "INSERT INTO muted_users(guild_id, user_id, until_ts) VALUES ($1, $2, $3) ON CONFLICT (guild_id, user_id) DO UPDATE SET until_ts = EXCLUDED.until_ts"
        ).bind(gid.0 as i64).bind(uid.0 as i64).bind(until.to_rfc3339()).execute(&self.pool).await;
    }
    async fn remove_mute(&self, gid: GuildId, uid: UserId) {
        let _ = sqlx::query(
            "DELETE FROM muted_users WHERE guild_id = $1 AND user_id = $2"
        ).bind(gid.0 as i64).bind(uid.0 as i64).execute(&self.pool).await;
    }
    async fn log_action(&self, gid: GuildId, uid: UserId, action: &str, reason: &str) {
        let ts = now_pht().to_rfc3339();
        let _ = sqlx::query(
            "INSERT INTO action_history(guild_id, user_id, action, reason, timestamp) VALUES ($1,$2,$3,$4,$5)"
        )
        .bind(gid.0 as i64).bind(uid.0 as i64).bind(action).bind(reason).bind(ts)
        .execute(&self.pool).await;
    }
}

// ------------------------------------------------------------
//  HELPERS
// ------------------------------------------------------------
async fn is_whitelisted(state: &BotState, http: &Http, cache: &Cache, gid: GuildId, uid: UserId) -> bool {
    if let Ok(current) = http.get_current_user().await {
        if uid == current.id { return true; }
    }
    if let Some(guild) = gid.to_guild_cached(cache) {
        if uid == guild.owner_id { return true; }
    }
    if let Some(cfg) = state.guild_configs.get(&gid) {
        if cfg.second_owner_id == Some(uid) { return true; }
    }
    if let Some(set) = state.whitelist_users.get(&gid) {
        if set.contains(&uid) { return true; }
    }
    if let Some(role_set) = state.whitelist_roles.get(&gid) {
        if let Ok(member) = gid.member(http, uid).await {
            if member.roles.iter().any(|r| role_set.contains(r)) { return true; }
        }
    }
    false
}

async fn is_link_bypassed(state: &BotState, http: &Http, cache: &Cache, gid: GuildId, member: &Member) -> bool {
    if is_whitelisted(state, http, cache, gid, member.user.id).await { return true; }
    if let Some(set) = state.link_bypass_users.get(&gid) {
        if set.contains(&member.user.id) { return true; }
    }
    if let Some(role_set) = state.link_bypass_roles.get(&gid) {
        if member.roles.iter().any(|r| role_set.contains(r)) { return true; }
    }
    false
}

fn snap_guild(guild: &Guild) -> GuildSnapshot {
    GuildSnapshot {
        name: guild.name.clone(),
        description: guild.description.clone(),
        icon: guild.icon_url(),
        banner: guild.banner_url(),
        afk_channel_id: guild.afk_channel_id,
        afk_timeout: guild.afk_timeout,
        verification_level: verification_level_to_u16(guild.verification_level),
        default_notifications: notification_level_to_u16(guild.default_message_notifications),
        explicit_content_filter: explicit_filter_to_u16(guild.explicit_content_filter),
        system_channel_id: guild.system_channel_id,
    }
}

fn snap_partial_guild(guild: &serenity::model::guild::PartialGuild) -> GuildSnapshot {
    GuildSnapshot {
        name: guild.name.clone(),
        description: guild.description.clone(),
        icon: guild.icon_url(),
        banner: guild.banner_url(),
        afk_channel_id: guild.afk_channel_id,
        afk_timeout: guild.afk_timeout,
        verification_level: verification_level_to_u16(guild.verification_level),
        default_notifications: notification_level_to_u16(guild.default_message_notifications),
        explicit_content_filter: 0u16,
        system_channel_id: guild.system_channel_id,
    }
}

fn snap_channel(channel: &GuildChannel) -> ChannelSnapshot {
    ChannelSnapshot {
        name: channel.name.clone(),
        category_id: channel.parent_id,
        position: channel.position as i32,
        channel_type: channel.kind,
        overwrites: channel.permission_overwrites.clone(),
        topic: if channel.kind == ChannelType::Text { channel.topic.clone() } else { None },
        nsfw: if channel.kind == ChannelType::Text { channel.nsfw } else { false },
        slowmode_delay: channel.rate_limit_per_user.unwrap_or(0),
    }
}

fn snap_role(role: &Role) -> RoleSnapshot {
    RoleSnapshot {
        name: role.name.clone(),
        permissions: role.permissions.bits(),
        colour: role.colour.0,
        hoist: role.hoist,
        mentionable: role.mentionable,
    }
}

async fn build_permission_map(state: &BotState, http: &Http, cache: &Cache, gid: GuildId) {
    let guild = match gid.to_guild_cached(cache) { Some(g) => g, None => return };
    let mut dangerous = HashSet::new();
    for member in guild.members.values() {
        if member.user.bot { continue; }
        if is_whitelisted(state, http, cache, gid, member.user.id).await { continue; }
        let mut perms = Permissions::empty();
        if let Some(everyone) = guild.roles.get(&RoleId(gid.0)) {
            perms |= everyone.permissions;
        }
        for role_id in &member.roles {
            if let Some(role) = guild.roles.get(role_id) {
                perms |= role.permissions;
            }
        }
        if DANGEROUS_PERMISSIONS.iter().any(|p| perms.contains(*p)) {
            dangerous.insert(member.user.id);
        }
    }
    state.dangerous_members.insert(gid, dangerous);
}

async fn get_actor_fast(
    state: &BotState, http: &Http, cache: &Cache, gid: GuildId, action: &str,
) -> Option<UserId> {
    let now = now_pht().timestamp_millis() as f64 / 1000.0;
    let key = (gid, action.to_string());
    if let Some(prefetch_ref) = state.audit_prefetch.get(&key) {
        let (actor, fetched) = *prefetch_ref;
        if now - fetched < ACTOR_CACHE_TTL_SECS
            && !is_whitelisted(state, http, cache, gid, actor).await
        {
            state.confirmed_actors.entry(key.clone())
                .or_insert_with(DashMap::new)
                .insert(actor, now + ACTOR_CACHE_TTL_SECS);
            return Some(actor);
        }
    }
    if let Some(conf) = state.confirmed_actors.get(&key) {
        for entry in conf.iter() {
            if now < *entry.value()
                && !is_whitelisted(state, http, cache, gid, *entry.key()).await
            {
                return Some(*entry.key());
            }
        }
    }
    if let Some(dangerous) = state.dangerous_members.get(&gid) {
        let active: Vec<_> = dangerous.iter()
            .filter(|uid| !state.ban_in_progress.get(&gid)
                .map(|b| b.contains(*uid)).unwrap_or(false))
            .collect();
        if active.len() == 1 {
            let actor = *active[0];
            state.audit_prefetch.insert(key.clone(), (actor, now));
            state.confirmed_actors.entry(key).or_insert_with(DashMap::new)
                .insert(actor, now + ACTOR_CACHE_TTL_SECS);
            return Some(actor);
        }
    }
    let action_type: u8 = match action {
        "channel_create" => 10, "channel_delete" => 12,
        "channel_update" => 11, "role_create" => 30,
        "role_delete" => 32, "role_update" => 31,
        "guild_update" => 1, "webhook_create" => 50,
        "member_unban" => 23,
        _ => return None,
    };
    if let Ok(logs) = gid.audit_logs(http, Some(action_type), None, None, Some(3)).await {
        for entry in logs.entries {
            if entry.user_id == http.get_current_user().await.ok()?.id { continue; }
            let age = (now_pht().timestamp() - entry.id.created_at().unix_timestamp()).abs() as f64;
            if age < 8.0 {
                let actor = entry.user_id;
                if !is_whitelisted(state, http, cache, gid, actor).await {
                    state.audit_prefetch.insert(key.clone(), (actor, now));
                    state.confirmed_actors.entry(key).or_insert_with(DashMap::new)
                        .insert(actor, now + ACTOR_CACHE_TTL_SECS);
                    return Some(actor);
                }
            }
        }
    }
    None
}

async fn log_violation(
    state: &BotState, http: &Http, cache: &Cache,
    gid: GuildId, user: &User, violation: &str, reason: &str, chid: ChannelId,
) {
    let mut count = state.user_violations.entry(user.id).or_insert(0);
    *count += 1;
    let total = *count;
    let cfg = state.guild_configs.get(&gid).map(|c| c.clone()).unwrap_or_default();
    let auto_ban_threshold = cfg.auto_ban_threshold;

    let user_mention = user.mention();
    let user_id_str = format!("{}", user.id);
    let total_str = total.to_string();
    let avatar = user.avatar_url().unwrap_or_else(|| user.default_avatar_url());
    let icon = cache.current_user().avatar_url().unwrap_or_default();
    let mut embed = CreateEmbed::default();
    embed.title("SECURITY VIOLATION DETECTED").color(EMBED_COLOR).timestamp(now_ts())
        .field("User", format!("{} ({})", user_mention, user_id_str), true)
        .field("Violation", violation, true)
        .field("Total Violations", total_str, true)
        .field("Reason", reason, false)
        .thumbnail(avatar)
        .footer(|f| f.text("Coded by ransxmware.xyz").icon_url(icon));
    if let Some(log_id) = gid.channels(http).await.ok()
        .and_then(|ch| ch.into_iter().find(|(_, c)| c.name == "security-logs").map(|(id, _)| id))
    {
        let _ = log_id.send_message(http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
    }
    if total >= auto_ban_threshold && !is_whitelisted(state, http, cache, gid, user.id).await {
        let _ = gid.ban_with_reason(http, user.id, 0,
            &format!("Auto-ban: {} security violations", total)).await;
    }
}

async fn instant_ban_and_rollback(
    state: Arc<BotState>, db: Arc<Database>, http: Arc<Http>, cache: Arc<Cache>,
    gid: GuildId, actor: UserId, action: &str,
    rollback: impl std::future::Future<Output = ()> + Send + 'static,
    log_extra: String,
) {
    if !state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) { return; }
    if is_whitelisted(&state, &http, &cache, gid, actor).await { return; }
    let ban_set = state.ban_in_progress.entry(gid).or_insert_with(DashSet::new);
    if ban_set.contains(&actor) { return; }
    ban_set.insert(actor);
    let state_clone = state.clone();
    let db_clone = db.clone();
    let http_clone = http.clone();
    let action_str = action.to_string();
    tokio::spawn(async move {
        let _ = http_clone.ban_user(gid.0, actor.0, 0,
            &format!("[Anti-Nuke] {}", action_str)).await;
        let _ = db_clone.log_action(gid, actor, "ROLLBACK-BAN", &action_str).await;
        rollback.await;
        state_clone.ban_in_progress.entry(gid).or_insert_with(DashSet::new).remove(&actor);
        if let Some(log_id) = gid.channels(&http_clone).await.ok()
            .and_then(|ch| ch.into_iter().find(|(_, c)| c.name == "security-logs").map(|(id, _)| id))
        {
            let mut embed = CreateEmbed::default();
            embed.title("🚨 ANTI-NUKE — INSTANT BAN + ROLLBACK").color(0xFF0000u32).timestamp(now_ts())
                .field("Actor", format!("`{}`", actor.0), true)
                .field("Action", action_str, true)
                .field("Ban", "✅ Banned", true)
                .field("Rollback", "✅ Restored", true)
                .field("Details", log_extra, false)
                .footer(|f| f.text("Coded by ransxmware.xyz — Anti-Nuke Rollback"));
            let _ = log_id.send_message(&http_clone, |m| m.embed(|e| { *e = embed.clone(); e })).await;
        }
    });
}

async fn check_mass_action(
    state: &BotState, http: &Http, cache: &Cache, db: &Database,
    gid: GuildId, actor: UserId, action_type: &str,
) {
    if !state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) { return; }
    if is_whitelisted(state, http, cache, gid, actor).await { return; }
    let cfg = state.guild_configs.get(&gid).map(|c| c.clone()).unwrap_or_default();
    let (threshold, window_secs) = match action_type {
        "Ban" => (cfg.mass_ban_threshold, cfg.mass_ban_window_secs),
        "Kick" => (cfg.mass_kick_threshold, cfg.mass_kick_window_secs),
        "ChannelCreate" => (cfg.mass_channel_create_threshold, cfg.mass_channel_create_window_secs),
        "RoleCreate" => (cfg.mass_role_create_threshold, cfg.mass_role_create_window_secs),
        _ => return,
    };
    let now = now_pht().timestamp_millis() as f64 / 1000.0;
    let mass_log = state.mass_action_log.entry(gid).or_insert_with(DashMap::new);
    let mut timestamps = mass_log.entry(actor).or_insert_with(Vec::new);
    timestamps.push(now);
    timestamps.retain(|t| now - *t <= window_secs);
    let count = timestamps.len();
    if count >= threshold {
        timestamps.clear();
        if let Ok(member) = gid.member(http, actor).await {
            let reason = format!("Mass {}: {} {}s in {}s", action_type, count, action_type, window_secs);
            let manageable: Vec<RoleId> = member.roles.iter()
                .filter(|r| r.0 != gid.0).copied().collect();
            if !manageable.is_empty() {
                let _ = member.to_owned().remove_roles(http, &manageable).await;
            }
            let _ = member.ban_with_reason(http, 0, &reason).await;
            let _ = db.log_action(gid, actor, &format!("MASS-{}", action_type.to_uppercase()), &reason).await;
            if let Some(log_id) = gid.channels(http).await.ok()
                .and_then(|ch| ch.into_iter().find(|(_, c)| c.name == "security-logs").map(|(id, _)| id))
            {
                let actor_tag = member.user.tag();
                let actor_id_str = format!("{}", actor.0);
                let count_str = count.to_string();
                let window_str = format!("{}s", window_secs);
                let action_upper = action_type.to_uppercase();
                let mut embed = CreateEmbed::default();
                embed.title(format!("🚨 ANTI MASS {}", action_upper)).color(0xFF0000u32).timestamp(now_ts())
                    .field("Actor", format!("{} (`{}`)", actor_tag, actor_id_str), true)
                    .field("Count", count_str, true)
                    .field("Window", window_str, true)
                    .field("Action", "Roles stripped → Banned", false);
                let _ = log_id.send_message(http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
        }
    }
}

async fn restore_channel(http: &Http, guild: &Guild, snap: &ChannelSnapshot) -> Option<ChannelId> {
    let parent_id = snap.category_id;
    let perms = snap.overwrites.clone();
    let snap_name = snap.name.clone();
    let snap_topic = snap.topic.clone();
    let snap_nsfw = snap.nsfw;
    let snap_slowmode = snap.slowmode_delay;
    let snap_pos = snap.position;
    let snap_kind = snap.channel_type;
    guild.create_channel(http, |c| {
        c.name(&snap_name).kind(snap_kind).position(snap_pos.unsigned_abs() as u32);
        if let Some(pid) = parent_id { c.category(pid); }
        if !perms.is_empty() { c.permissions(perms); }
        if snap_kind == ChannelType::Text {
            if let Some(ref t) = snap_topic { c.topic(t); }
            c.nsfw(snap_nsfw).rate_limit_per_user(snap_slowmode);
        }
        c
    }).await.ok().map(|c| c.id)
}

async fn restore_role(
    state: &BotState, http: &Http, gid: GuildId,
    role_name: &str, snap: Option<RoleSnapshot>,
) -> Option<RoleId> {
    let lock = state.role_restore_locks.entry(gid)
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())));
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
    for (id, ch) in guild.channels.iter() {
        if let Some(guild_ch) = ch.clone().guild() {
            state.channel_snapshots.insert(*id, snap_channel(&guild_ch));
        }
    }
    for (id, role) in guild.roles.iter() {
        state.role_snapshots.insert(*id, snap_role(role));
    }
}

async fn poll_audit_logs(state: Arc<BotState>, http: Arc<Http>, cache: Arc<Cache>, gid: GuildId) {
    let actions = [
        "channel_delete", "channel_create", "channel_update",
        "role_create", "role_delete", "role_update",
        "guild_update", "webhook_create", "ban", "kick", "member_role_update",
        "member_unban",
    ];
    loop {
        for act in actions {
            let _ = get_actor_fast(&state, &http, &cache, gid, act).await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn cleanup_mutes(state: Arc<BotState>, db: Arc<Database>) {
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
        let now = now_pht();
        let to_remove: Vec<UserId> = state.muted_users.iter()
            .filter_map(|e| if now >= *e.value() { Some(*e.key()) } else { None })
            .collect();
        let had_removes = !to_remove.is_empty();
        for uid in &to_remove { state.muted_users.remove(uid); }
        if had_removes {
            let now_str = now.to_rfc3339();
            let _ = sqlx::query("DELETE FROM muted_users WHERE until_ts <= $1")
                .bind(now_str)
                .execute(&db.pool).await;
        }
    }
}

async fn send_embed_fn(http: &Http, ch: ChannelId, title: &str, desc: &str, color: u32) {
    let mut embed = CreateEmbed::default();
    embed.title(title).description(desc).color(color).timestamp(now_ts());
    let _ = ch.send_message(http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
}

// ------------------------------------------------------------
//  EVENT HANDLER
// ------------------------------------------------------------
struct Handler {
    state: Arc<BotState>,
    db: Arc<Database>,
    http: Arc<Http>,
}

#[async_trait]
impl EventHandler for Handler {
    async fn ready(&self, ctx: Context, ready: serenity::model::gateway::Ready) {
        println!("{}", ANTINUKE_ASCII);
        println!("Logged in as: {}", ready.user.name);
        for gid in ctx.cache.guilds() {
            if let Some(guild) = gid.to_guild_cached(&ctx.cache) {
                snapshot_all(&self.state, &self.http, &guild).await;
                build_permission_map(&self.state, &self.http, &ctx.cache, gid).await;
                tokio::spawn(poll_audit_logs(
                    self.state.clone(), self.http.clone(), ctx.cache.clone(), gid,
                ));
            }
        }
        tokio::spawn(cleanup_mutes(self.state.clone(), self.db.clone()));
        let server_count = ctx.cache.guilds().len();
        ctx.set_presence(
            Some(serenity::model::gateway::Activity::watching(format!("over {} servers!", server_count))),
            serenity::model::user::OnlineStatus::Online,
        ).await;
    }

    async fn guild_create(&self, ctx: Context, guild: Guild, _is_new: bool) {
        snapshot_all(&self.state, &self.http, &guild).await;
        build_permission_map(&self.state, &self.http, &ctx.cache, guild.id).await;
        tokio::spawn(poll_audit_logs(
            self.state.clone(), self.http.clone(), ctx.cache.clone(), guild.id,
        ));
        let server_count = ctx.cache.guilds().len();
        ctx.set_presence(
            Some(serenity::model::gateway::Activity::watching(format!("over {} servers!", server_count))),
            serenity::model::user::OnlineStatus::Online,
        ).await;
    }

    async fn guild_delete(&self, ctx: Context, _incomplete: UnavailableGuild, _full: Option<Guild>) {
        let server_count = ctx.cache.guilds().len();
        ctx.set_presence(
            Some(serenity::model::gateway::Activity::watching(format!("over {} servers!", server_count))),
            serenity::model::user::OnlineStatus::Online,
        ).await;
    }

    async fn webhook_update(&self, ctx: Context, guild_id: GuildId, channel_id: ChannelId) {
        let gid = guild_id;
        if !self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) { return; }
        let now = now_pht().timestamp_millis() as f64 / 1000.0;
        let entry = self.state.handled_webhook_events.entry(gid).or_insert_with(DashMap::new);
        if let Some(exp) = entry.get(&channel_id) { if now < *exp { return; } }
        entry.insert(channel_id, now + WEBHOOK_EVENT_DEDUP_TTL_SECS);
        if let Some(actor) = get_actor_fast(&self.state, &self.http, &ctx.cache, gid, "webhook_create").await {
            if is_whitelisted(&self.state, &self.http, &ctx.cache, gid, actor).await { return; }
            let http = self.http.clone();
            instant_ban_and_rollback(
                self.state.clone(), self.db.clone(), self.http.clone(), ctx.cache.clone(),
                gid, actor, "Unauthorized webhook creation",
                async move {
                    let webhooks = match http.get_guild_webhooks(gid.0).await {
                        Ok(w) => w, Err(_) => return,
                    };
                    for wh in webhooks {
                        let _ = http.delete_webhook(wh.id.0).await;
                    }
                },
                "All webhooks guild-wide purged".to_string(),
            ).await;
        }
    }

    async fn channel_create(&self, ctx: Context, channel: &GuildChannel) {
        let gid = channel.guild_id;
        if self.state.restoring.get(&gid).map(|r| *r).unwrap_or(false) {
            self.state.channel_snapshots.insert(channel.id, snap_channel(channel));
            return;
        }
        if !self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) {
            self.state.channel_snapshots.insert(channel.id, snap_channel(channel));
            return;
        }
        let now = now_pht().timestamp_millis() as f64 / 1000.0;
        let entry = self.state.handled_channel_creates.entry(gid).or_insert_with(DashMap::new);
        if let Some(exp) = entry.get(&channel.id) { if now < *exp { return; } }
        entry.insert(channel.id, now + CHANNEL_CREATE_DEDUP_TTL_SECS);
        if let Some(actor) = get_actor_fast(&self.state, &self.http, &ctx.cache, gid, "channel_create").await {
            if is_whitelisted(&self.state, &self.http, &ctx.cache, gid, actor).await {
                self.state.channel_snapshots.insert(channel.id, snap_channel(channel));
                return;
            }
            let channel_id = channel.id;
            let channel_name = channel.name.clone();
            let http = self.http.clone();
            instant_ban_and_rollback(
                self.state.clone(), self.db.clone(), self.http.clone(), ctx.cache.clone(),
                gid, actor, "Unauthorized channel creation",
                async move { let _ = http.delete_channel(channel_id.0).await; },
                format!("Channel **#{}** (`{}`) was deleted.", channel_name, channel_id.0),
            ).await;
        } else {
            self.state.channel_snapshots.insert(channel.id, snap_channel(channel));
        }
    }

    async fn channel_delete(&self, ctx: Context, channel: &GuildChannel) {
        let gid = channel.guild_id;
        if self.state.restoring.get(&gid).map(|r| *r).unwrap_or(false) { return; }
        if !self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) {
            self.state.channel_snapshots.remove(&channel.id);
            return;
        }
        if let Some(actor) = get_actor_fast(&self.state, &self.http, &ctx.cache, gid, "channel_delete").await {
            if is_whitelisted(&self.state, &self.http, &ctx.cache, gid, actor).await {
                self.state.channel_snapshots.remove(&channel.id);
                return;
            }
            let snap = self.state.channel_snapshots.get(&channel.id)
                .map(|s| s.clone())
                .unwrap_or_else(|| ChannelSnapshot {
                    name: channel.name.clone(),
                    category_id: channel.parent_id,
                    position: channel.position as i32,
                    channel_type: channel.kind,
                    overwrites: channel.permission_overwrites.clone(),
                    topic: if channel.kind == ChannelType::Text { channel.topic.clone() } else { None },
                    nsfw: if channel.kind == ChannelType::Text { channel.nsfw } else { false },
                    slowmode_delay: channel.rate_limit_per_user.unwrap_or(0),
                });
            self.state.channel_snapshots.remove(&channel.id);
            let queue_entry = self.state.rollback_queue.entry(gid).or_insert_with(DashMap::new);
            let mut actor_queue = queue_entry.entry(actor).or_insert_with(Vec::new);
            if !actor_queue.iter().any(|s| s.name == snap.name) {
                actor_queue.push(snap);
            } else { return; }
            let drain_set = self.state.drain_scheduled.entry(gid).or_insert_with(DashSet::new);
            if !drain_set.contains(&actor) {
                drain_set.insert(actor);
                {
                    let ban_set = self.state.ban_in_progress.entry(gid).or_insert_with(DashSet::new);
                    if !ban_set.contains(&actor) {
                        ban_set.insert(actor);
                        let http = self.http.clone();
                        let state = self.state.clone();
                        let db = self.db.clone();
                        tokio::spawn(async move {
                            let _ = http.ban_user(gid.0, actor.0, 0,
                                "[Anti-Nuke] Full nuke — channel deletion").await;
                            let _ = db.log_action(gid, actor, "ROLLBACK-BAN",
                                "full_nuke_channel_delete").await;
                            state.ban_in_progress.entry(gid).or_insert_with(DashSet::new).remove(&actor);
                        });
                    }
                }
                let state = self.state.clone();
                let http = self.http.clone();
                let cache = ctx.cache.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs_f64(DRAIN_DELAY_SECS)).await;
                    let queue = {
                        let e = state.rollback_queue.entry(gid).or_insert_with(DashMap::new);
                        e.remove(&actor).map(|(_, v)| v).unwrap_or_default()
                    };
                    if queue.is_empty() {
                        state.drain_scheduled.entry(gid).or_insert_with(DashSet::new).remove(&actor);
                        return;
                    }
                    state.restoring.insert(gid, true);
                    let queue_len = queue.len();
                    if let Some(guild) = gid.to_guild_cached(&cache) {
                        for snap in queue {
                            let _ = restore_channel(&http, &guild, &snap).await;
                        }
                    }
                    state.restoring.insert(gid, false);
                    state.drain_scheduled.entry(gid).or_insert_with(DashSet::new).remove(&actor);
                    if let Some(log_id) = gid.channels(&http).await.ok()
                        .and_then(|ch| ch.into_iter().find(|(_, c)| c.name == "security-logs").map(|(id, _)| id))
                    {
                        let mut embed = CreateEmbed::default();
                        embed.title("🚨 ANTI-NUKE — FULL NUKE ROLLBACK").color(0xFF0000u32).timestamp(now_ts())
                            .field("Actor", format!("`{}`", actor.0), true)
                            .field("Action", "Mass channel deletion", true)
                            .field("Ban", "✅ Banned (before restore)", true)
                            .field("Channels Restored", queue_len.to_string(), true)
                            .field("Details", "All deleted channels restored in bulk.", false);
                        let _ = log_id.send_message(&http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                    }
                });
            }
        }
    }

    async fn channel_update(&self, ctx: Context, old: Option<Channel>, new: Channel) {
        let old = match old.and_then(|c| c.guild()) { Some(o) => o, None => return };
        let new = match new.guild() { Some(n) => n, None => return };
        let gid = new.guild_id;
        if !self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) {
            self.state.channel_snapshots.insert(new.id, snap_channel(&new));
            return;
        }
        if let Some(actor) = get_actor_fast(&self.state, &self.http, &ctx.cache, gid, "channel_update").await {
            if is_whitelisted(&self.state, &self.http, &ctx.cache, gid, actor).await {
                self.state.channel_snapshots.insert(new.id, snap_channel(&new));
                return;
            }
            let snap = self.state.channel_snapshots.get(&new.id).map(|s| s.clone());
            let changed = if old.name != new.name {
                format!("name `{}` → `{}`", old.name, new.name)
            } else { "settings changed".to_string() };
            let old_name_log = old.name.clone();
            let channel_id = new.id;
            let http = self.http.clone();
            let state = self.state.clone();
            instant_ban_and_rollback(
                self.state.clone(), self.db.clone(), self.http.clone(), ctx.cache.clone(),
                gid, actor, "Unauthorized channel edit",
                async move {
                    if let Ok(Channel::Guild(mut live)) = http.get_channel(channel_id.0).await {
                        let restore_name = snap.as_ref().map(|s| s.name.clone())
                            .unwrap_or_else(|| old.name.clone());
                        let snap_ref = snap.clone();
                        let old_topic = old.topic.clone();
                        let old_nsfw = old.nsfw;
                        let old_ratelimit = old.rate_limit_per_user.unwrap_or(0);
                        let is_text = live.kind == ChannelType::Text;
                        let _ = live.edit(&http, |e| {
                            e.name(&restore_name);
                            if is_text {
                                if let Some(ref s) = snap_ref {
                                    e.topic(s.topic.as_deref().unwrap_or(""))
                                     .nsfw(s.nsfw)
                                     .rate_limit_per_user(s.slowmode_delay);
                                } else {
                                    e.topic(old_topic.as_deref().unwrap_or(""))
                                     .nsfw(old_nsfw)
                                     .rate_limit_per_user(old_ratelimit);
                                }
                            }
                            e
                        }).await;
                        state.channel_snapshots.insert(channel_id, snap_channel(&live));
                    }
                },
                format!("Channel **#{}** — {} → reverted.", old_name_log, changed),
            ).await;
        } else {
            self.state.channel_snapshots.insert(new.id, snap_channel(&new));
        }
    }

    async fn guild_update(&self, ctx: Context, old: Option<Guild>, new: serenity::model::guild::PartialGuild) {
        let old = match old {
            Some(g) => g,
            None => { self.state.guild_snapshots.insert(new.id, snap_partial_guild(&new)); return; }
        };
        let gid = new.id;
        if !self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) {
            self.state.guild_snapshots.insert(gid, snap_partial_guild(&new));
            return;
        }
        let now_f = now_pht().timestamp_millis() as f64 / 1000.0;
        if let Some(exp) = self.state.handled_guild_updates.get(&gid) {
            if now_f < *exp { return; }
        }
        self.state.handled_guild_updates.insert(gid, now_f + GUILD_UPDATE_DEDUP_TTL_SECS);
        if let Some(actor) = get_actor_fast(&self.state, &self.http, &ctx.cache, gid, "guild_update").await {
            if is_whitelisted(&self.state, &self.http, &ctx.cache, gid, actor).await {
                self.state.guild_snapshots.insert(gid, snap_partial_guild(&new));
                return;
            }
            let snap = self.state.guild_snapshots.get(&gid).map(|s| s.clone());
            let changes = format!("name `{}` → `{}`", old.name, new.name);
            let http = self.http.clone();
            let state = self.state.clone();
            instant_ban_and_rollback(
                self.state.clone(), self.db.clone(), self.http.clone(), ctx.cache.clone(),
                gid, actor, "Unauthorized guild settings change",
                async move {
                    if let Some(s) = snap {
                        let sname = s.name.clone();
                        let sdesc = s.description.clone();
                        let safk_timeout = s.afk_timeout;
                        let sverif = s.verification_level;
                        let snotif = s.default_notifications;
                        let secf = s.explicit_content_filter;
                        let _ = gid.edit(&http, |e| {
                            use serenity::model::guild::{
                                DefaultMessageNotificationLevel, ExplicitContentFilter,
                                VerificationLevel,
                            };
                            let vl = match sverif {
                                1 => VerificationLevel::Low,
                                2 => VerificationLevel::Medium,
                                3 => VerificationLevel::High,
                                4 => VerificationLevel::Higher,
                                _ => VerificationLevel::None,
                            };
                            let nl = match snotif {
                                1 => DefaultMessageNotificationLevel::Mentions,
                                _ => DefaultMessageNotificationLevel::All,
                            };
                            let ef = match secf {
                                1 => ExplicitContentFilter::WithoutRole,
                                2 => ExplicitContentFilter::All,
                                _ => ExplicitContentFilter::None,
                            };
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
                },
                format!("Changes: {}", changes),
            ).await;
        } else {
            self.state.guild_snapshots.insert(gid, snap_partial_guild(&new));
        }
    }

    async fn guild_role_create(&self, ctx: Context, role: Role) {
        let gid = role.guild_id;
        if !self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) {
            self.state.role_snapshots.insert(role.id, snap_role(&role));
            return;
        }
        let now = now_pht().timestamp_millis() as f64 / 1000.0;
        let entry = self.state.handled_role_events.entry(gid).or_insert_with(DashMap::new);
        if let Some(exp) = entry.get(&role.id.0) { if now < *exp { return; } }
        entry.insert(role.id.0, now + ROLE_EVENT_DEDUP_TTL_SECS);
        if let Some(actor) = get_actor_fast(&self.state, &self.http, &ctx.cache, gid, "role_create").await {
            if is_whitelisted(&self.state, &self.http, &ctx.cache, gid, actor).await {
                self.state.role_snapshots.insert(role.id, snap_role(&role));
                return;
            }
            let role_id = role.id;
            let role_name = role.name.clone();
            let http = self.http.clone();
            instant_ban_and_rollback(
                self.state.clone(), self.db.clone(), self.http.clone(), ctx.cache.clone(),
                gid, actor, "Unauthorized role creation",
                async move { let _ = http.delete_role(gid.0, role_id.0).await; },
                format!("Role **@{}** (`{}`) was deleted.", role_name, role_id.0),
            ).await;
        } else {
            self.state.role_snapshots.insert(role.id, snap_role(&role));
        }
    }

    async fn guild_role_update(&self, ctx: Context, old: Option<Role>, new: Role) {
        let old = match old {
            Some(r) => r,
            None => { self.state.role_snapshots.insert(new.id, snap_role(&new)); return; }
        };
        let gid = new.guild_id;
        if !self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) {
            self.state.role_snapshots.insert(new.id, snap_role(&new));
            return;
        }
        let now = now_pht().timestamp_millis() as f64 / 1000.0;
        let key = (new.id.0 as u64) * 10_000_000 + (old.permissions.bits() % 10_000_000);
        let entry = self.state.handled_role_events.entry(gid).or_insert_with(DashMap::new);
        if let Some(exp) = entry.get(&key) { if now < *exp { return; } }
        entry.insert(key, now + ROLE_EVENT_DEDUP_TTL_SECS);
        if let Some(actor) = get_actor_fast(&self.state, &self.http, &ctx.cache, gid, "role_update").await {
            if is_whitelisted(&self.state, &self.http, &ctx.cache, gid, actor).await {
                self.state.role_snapshots.insert(new.id, snap_role(&new));
                build_permission_map(&self.state, &self.http, &ctx.cache, gid).await;
                return;
            }
            let snap = self.state.role_snapshots.get(&new.id).map(|s| s.clone());
            let changes = if old.name != new.name {
                format!("name `{}` → `{}`", old.name, new.name)
            } else { "settings changed".to_string() };
            let old_name_log = old.name.clone();
            let role_id = new.id;
            let http = self.http.clone();
            let state = self.state.clone();
            instant_ban_and_rollback(
                self.state.clone(), self.db.clone(), self.http.clone(), ctx.cache.clone(),
                gid, actor, "Unauthorized role edit",
                async move {
                    let roles = http.get_guild_roles(gid.0).await;
                    if let Some(live) = roles.ok().as_deref()
                        .and_then(|rs| rs.iter().find(|r| r.id == role_id))
                    {
                        if let Some(s) = snap.clone() {
                            let sname = s.name.clone();
                            let sperms = s.permissions;
                            let scolour = s.colour;
                            let shoist = s.hoist;
                            let sment = s.mentionable;
                            let _ = live.edit(&http, |r| {
                                r.name(&sname)
                                 .permissions(Permissions::from_bits_truncate(sperms))
                                 .colour(scolour as u64)
                                 .hoist(shoist)
                                 .mentionable(sment)
                            }).await;
                        } else {
                            let oname = old.name.clone();
                            let operms = old.permissions;
                            let ocolour = old.colour.0;
                            let ohoist = old.hoist;
                            let oment = old.mentionable;
                            let _ = live.edit(&http, |r| {
                                r.name(&oname)
                                 .permissions(operms)
                                 .colour(ocolour as u64)
                                 .hoist(ohoist)
                                 .mentionable(oment)
                            }).await;
                        }
                        state.role_snapshots.insert(role_id, snap.unwrap_or_else(|| snap_role(live)));
                    }
                },
                format!("Role **@{}** — {} → reverted.", old_name_log, changes),
            ).await;
        } else {
            self.state.role_snapshots.insert(new.id, snap_role(&new));
        }
    }

    async fn guild_role_delete(&self, ctx: Context, gid: GuildId, role_id: RoleId, role: Option<Role>) {
        let role = match role {
            Some(r) => r,
            None => { self.state.role_snapshots.remove(&role_id); return; }
        };
        if !self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) {
            self.state.role_snapshots.remove(&role.id);
            return;
        }
        let now = now_pht().timestamp_millis() as f64 / 1000.0;
        let entry = self.state.handled_role_events.entry(gid).or_insert_with(DashMap::new);
        if let Some(exp) = entry.get(&role.id.0) { if now < *exp { return; } }
        entry.insert(role.id.0, now + ROLE_EVENT_DEDUP_TTL_SECS);
        if let Some(actor) = get_actor_fast(&self.state, &self.http, &ctx.cache, gid, "role_delete").await {
            if is_whitelisted(&self.state, &self.http, &ctx.cache, gid, actor).await {
                self.state.role_snapshots.remove(&role.id);
                return;
            }
            let snap = self.state.role_snapshots.remove(&role.id).map(|(_, s)| s);
            let role_name = role.name.clone();
            let role_name2 = role_name.clone();
            let role_id_val = role.id.0;
            let state = self.state.clone();
            let http = self.http.clone();
            instant_ban_and_rollback(
                self.state.clone(), self.db.clone(), self.http.clone(), ctx.cache.clone(),
                gid, actor, "Unauthorized role deletion",
                async move { restore_role(&state, &http, gid, &role_name, snap).await; },
                format!("Role **@{}** (`{}`) was restored.", role_name2, role_id_val),
            ).await;
        }
    }

    async fn guild_ban_removal(&self, ctx: Context, guild_id: GuildId, user: User) {
        if !self.state.protection_enabled.get(&guild_id).map(|e| *e).unwrap_or(false) { return; }
        if let Ok(logs) = guild_id.audit_logs(&self.http, Some(23), None, None, Some(5)).await {
            for entry in logs.entries {
                if entry.target_id == Some(user.id.0) {
                    let actor = entry.user_id;
                    if is_whitelisted(&self.state, &self.http, &ctx.cache, guild_id, actor).await { return; }
                    let _ = guild_id.ban_with_reason(&self.http, user.id, 0, "[Anti-Nuke] Unauthorized unban - reverted").await;
                    let _ = guild_id.ban_with_reason(&self.http, actor, 0, "[Anti-Nuke] Unauthorized unban attempt").await;
                    if let Some(log_id) = guild_id.channels(&self.http).await.ok()
                        .and_then(|ch| ch.into_iter().find(|(_, c)| c.name == "security-logs").map(|(id, _)| id))
                    {
                        let mut embed = CreateEmbed::default();
                        embed.title("🚨 ANTI-NUKE — UNAUTHORIZED UNBAN").color(0xFF0000u32).timestamp(now_ts())
                            .field("Unban Attempt By", format!("`{}`", actor.0), true)
                            .field("Target User", user.mention(), true)
                            .field("Action", "Actor banned, Target re-banned", false);
                        let _ = log_id.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                    }
                    break;
                }
            }
        }
    }

    async fn guild_ban_addition(&self, ctx: Context, gid: GuildId, banned_user: User) {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if let Ok(logs) = gid.audit_logs(&self.http, Some(22u8), None, None, Some(5)).await {
            for entry in logs.entries {
                let age = (now_pht().timestamp() - entry.id.created_at().unix_timestamp()).abs() as f64;
                if age < 15.0 && entry.target_id == Some(banned_user.id.0) {
                    let actor = entry.user_id;
                    check_mass_action(&self.state, &self.http, &ctx.cache, &self.db, gid, actor, "Ban").await;
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
                    let actor = entry.user_id;
                    check_mass_action(&self.state, &self.http, &ctx.cache, &self.db, gid, actor, "Kick").await;
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
                        let adder = entry.user_id;
                        if !is_whitelisted(&self.state, &self.http, &ctx.cache, gid, adder).await {
                            let _ = new_member.kick(&self.http).await;
                            let _ = self.db.log_action(gid, new_member.user.id, "AUTO-KICK-BOT",
                                &format!("Added by user {}", adder.0)).await;
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
            if let Some(role) = ctx.cache.guild(gid)
                .and_then(|g| g.roles.get(role_id).cloned())
            {
                if DANGEROUS_PERMISSIONS.iter().any(|p| role.permissions.contains(*p)) {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    if let Ok(logs) = gid.audit_logs(&self.http, Some(25u8), None, None, Some(15)).await {
                        for entry in logs.entries {
                            let age = (now_pht().timestamp() - entry.id.created_at().unix_timestamp()).abs() as f64;
                            if age < 20.0 && entry.target_id == Some(new.user.id.0) {
                                let assigner = entry.user_id;
                                if !is_whitelisted(&self.state, &self.http, &ctx.cache, gid, assigner).await {
                                    if let Ok(assigner_member) = gid.member(&self.http, assigner).await {
                                        let _ = assigner_member.kick_with_reason(&self.http,
                                            &format!("Granted dangerous permissions to {}", new.user.tag())).await;
                                        let _ = self.db.log_action(gid, assigner, "AUTO-KICK-DANGEROUS-PERMS",
                                            &format!("Assigned role {} to {}", role.name, new.user.tag())).await;
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

        if let Some(until) = self.state.muted_users.get(&msg.author.id) {
            if now < *until { let _ = msg.delete(&self.http).await; return; }
            else { self.state.muted_users.remove(&msg.author.id); }
        }

        let cl = msg.content.to_lowercase();
        let is_cmd = cl.starts_with("null")
            || (cl.starts_with('x') && cl.len() > 1
                && cl.chars().nth(1).map(|c| c.is_ascii_alphabetic()).unwrap_or(false));

        if is_cmd {
            if let Some(until) = self.state.rate_limited_until.get(&msg.author.id) {
                if now < *until { let _ = msg.delete(&self.http).await; return; }
                else { self.state.rate_limited_until.remove(&msg.author.id); }
            }
            let now_ts_f = now.timestamp_millis() as f64 / 1000.0;
            let mut timestamps = self.state.command_timestamps
                .entry(msg.author.id).or_insert_with(|| VecDeque::with_capacity(10));
            timestamps.push_back(now_ts_f);
            while let Some(t) = timestamps.front() {
                if now_ts_f - *t > RATE_LIMIT_WINDOW_SECS { timestamps.pop_front(); } else { break; }
            }
            if timestamps.len() > RATE_LIMIT_MAX_COMMANDS {
                let cooldown = now + ChronoDuration::seconds(RATE_LIMIT_COOLDOWN_SECS);
                self.state.rate_limited_until.insert(msg.author.id, cooldown);
                let _ = msg.delete(&self.http).await;
                let mut embed = CreateEmbed::default();
                embed.title("⏱️ Slow Down!")
                    .description(format!(
                        "{} you're sending commands too fast.\nPlease wait **{} seconds** before using commands again.",
                        msg.author.mention(), RATE_LIMIT_COOLDOWN_SECS
                    ))
                    .color(0xFF4500u32).timestamp(now_ts());
                if let Ok(sent) = msg.channel_id.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let _ = sent.delete(&self.http).await;
                }
                return;
            }
            self.process_commands(ctx, &msg).await;
            return;
        }

        if is_whitelisted(&self.state, &self.http, &ctx.cache, gid, msg.author.id).await { return; }

        {
            let mut times = self.state.user_message_times.entry(msg.author.id).or_insert_with(VecDeque::new);
            times.push_back(now);
            while let Some(t) = times.front() {
                if (now - *t).num_seconds() > 60 { times.pop_front(); } else { break; }
            }
            let mut msgs = self.state.user_messages.entry(msg.author.id).or_insert_with(VecDeque::new);
            msgs.push_back(msg.content.to_lowercase());
            while msgs.len() > 10 { msgs.pop_front(); }
        }

        if self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) {
            let member = match msg.member(&ctx).await { Ok(m) => m, Err(_) => return };
            let link_bypassed = is_link_bypassed(&self.state, &self.http, &ctx.cache, gid, &member).await;
            let cfg = self.state.guild_configs.get(&gid).map(|c| c.clone()).unwrap_or_default();

            let invite_re = Regex::new(
                r"(?i)discord\.gg/([a-zA-Z0-9]+)|discord(?:app)?\.com/invite/([a-zA-Z0-9]+)"
            ).unwrap();
            if let Some(caps) = invite_re.captures(&msg.content) {
                let code = match caps.get(1).or_else(|| caps.get(2)) {
                    Some(m) => m.as_str(), None => return
                };
                if let Ok(invite) = self.http.get_invite(code, false, false, None).await {
                    if let Some(inv_guild) = invite.guild {
                        if inv_guild.id != gid.0 {
                            let now_ts_f = now.timestamp_millis() as f64 / 1000.0;
                            let ad_reg = self.state.server_ad_registry.entry(gid).or_insert_with(DashMap::new);
                            let existing = ad_reg.get(&msg.author.id).map(|e| e.clone());
                            if let Some(ex) = existing {
                                if ex.invite_code == code && ex.channel_id == msg.channel_id {
                                    let _ = msg.delete(&self.http).await;
                                    let mut embed = CreateEmbed::default();
                                    embed.title("🚫 Duplicate Server Ad")
                                        .description(format!(
                                            "{}, your server ad is **already posted** in this channel.\nYou may only advertise once every **{} hour(s)**.",
                                            msg.author.mention(), SERVER_AD_EXPIRY_SECS / 3600
                                        ))
                                        .color(0xFF4500u32);
                                    if let Ok(sent) = msg.channel_id.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await {
                                        tokio::time::sleep(Duration::from_secs(8)).await;
                                        let _ = sent.delete(&self.http).await;
                                    }
                                    return;
                                } else {
                                    let _ = msg.delete(&self.http).await;
                                    let spam_map = self.state.ad_spam_channels.entry(gid).or_insert_with(DashMap::new);
                                    let mut channels = spam_map.entry(msg.author.id).or_insert_with(Vec::new);
                                    if !channels.contains(&msg.channel_id) { channels.push(msg.channel_id); }
                                    if let Some(orig_ch) = ex.channel_id.to_channel(&self.http).await.ok()
                                        .and_then(|c| c.guild())
                                    {
                                        let _ = orig_ch.delete_messages(&self.http, &[ex.message_id]).await;
                                    }
                                    ad_reg.remove(&msg.author.id);
                                    spam_map.remove(&msg.author.id);
                                    let timeout = now + ChronoDuration::minutes(AD_SPAM_TIMEOUT_MIN);
                                    let timeout_str = timeout.to_rfc3339();
                                    if let Ok(member) = gid.member(&self.http, msg.author.id).await {
                                        let _ = member.edit(&self.http, |e| {
                                            e.disable_communication_until(timeout_str.clone())
                                        }).await;
                                    }
                                    self.db.log_action(gid, msg.author.id, "AD-SPAM-TIMEOUT",
                                        &format!("Spammed ad in multiple channels")).await;
                                    if let Some(log_id) = gid.channels(&self.http).await.ok()
                                        .and_then(|ch| ch.into_iter().find(|(_, c)| c.name == "security-logs").map(|(id, _)| id))
                                    {
                                        let mut embed = CreateEmbed::default();
                                        embed.title("📢 AD SPAM DETECTED — TIMEOUT ISSUED").color(0xFF4500u32).timestamp(now_ts())
                                            .field("User", format!("{} (`{}`)", msg.author.mention(), msg.author.id), true)
                                            .field("Invite", format!("`discord.gg/{}`", code), true)
                                            .field("Timeout", format!("{} minutes", AD_SPAM_TIMEOUT_MIN), true)
                                            .field("Action", "All ad copies deleted + user timed out", false);
                                        let _ = log_id.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                                    }
                                    let mut embed = CreateEmbed::default();
                                    embed.title("🚫 Server Ad Spam Detected")
                                        .description(format!(
                                            "{} has been **timed out for {} minutes** for spamming their server ad.\nAll copies have been **deleted**.",
                                            msg.author.mention(), AD_SPAM_TIMEOUT_MIN
                                        ))
                                        .color(0xFF0000u32);
                                    if let Ok(sent) = msg.channel_id.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await {
                                        tokio::time::sleep(Duration::from_secs(15)).await;
                                        let _ = sent.delete(&self.http).await;
                                    }
                                    return;
                                }
                            } else {
                                ad_reg.insert(msg.author.id, ServerAdEntry {
                                    invite_code: code.to_string(),
                                    channel_id: msg.channel_id,
                                    message_id: msg.id,
                                    timestamp: now_ts_f,
                                });
                                let spam_map = self.state.ad_spam_channels.entry(gid).or_insert_with(DashMap::new);
                                spam_map.insert(msg.author.id, vec![msg.channel_id]);
                            }
                        }
                    }
                } else {
                    let _ = msg.delete(&self.http).await;
                    log_violation(&self.state, &self.http, &ctx.cache, gid, &msg.author,
                        "MALICIOUS DISCORD INVITE",
                        &format!("Posted a malicious Discord invite (code: {})", code),
                        msg.channel_id).await;
                    return;
                }
            }

            let recent_times_len = self.state.user_message_times.get(&msg.author.id)
                .map(|t| t.len()).unwrap_or(0);
            if recent_times_len > cfg.max_messages_per_minute {
                let _ = msg.delete(&self.http).await;
                log_violation(&self.state, &self.http, &ctx.cache, gid, &msg.author,
                    "SPAM DETECTION",
                    &format!("Sent {} messages in 1 minute", recent_times_len),
                    msg.channel_id).await;
                return;
            }

            let dup_count = {
                let recent_msgs_opt = self.state.user_messages.get(&msg.author.id);
                recent_msgs_opt.as_ref().map(|r| {
                    r.iter().filter(|m| *m == &msg.content.to_lowercase()).count()
                }).unwrap_or(0)
            };
            if dup_count > cfg.max_duplicate_messages {
                let _ = msg.delete(&self.http).await;
                log_violation(&self.state, &self.http, &ctx.cache, gid, &msg.author,
                    "DUPLICATE SPAM", "Sending identical messages repeatedly",
                    msg.channel_id).await;
                return;
            }

            if !link_bypassed {
                for word in &cfg.banned_words {
                    if msg.content.to_lowercase().contains(word.as_str()) {
                        let _ = msg.delete(&self.http).await;
                        log_violation(&self.state, &self.http, &ctx.cache, gid, &msg.author,
                            "BANNED WORD",
                            &format!("Used prohibited word: '{}'", word),
                            msg.channel_id).await;
                        return;
                    }
                }
            }

            let emoji_re = Regex::new(r"<:[^:]+:\d+>|[\u{1F600}-\u{1F64F}]").unwrap();
            let emoji_count = emoji_re.find_iter(&msg.content).count();
            if emoji_count > cfg.max_emojis {
                let _ = msg.delete(&self.http).await;
                log_violation(&self.state, &self.http, &ctx.cache, gid, &msg.author,
                    "EMOJI SPAM",
                    &format!("Used {} emojis (limit: {})", emoji_count, cfg.max_emojis),
                    msg.channel_id).await;
                return;
            }

            if !link_bypassed {
                let url_re = Regex::new(r"https?://[^\s]+").unwrap();
                for url in url_re.find_iter(&msg.content) {
                    let url_str = url.as_str();
                    let allowed = cfg.link_whitelist.iter().any(|d| url_str.contains(d.as_str()));
                    let is_gif = url_str.to_lowercase().ends_with(".gif");
                    if !allowed && !is_gif {
                        let _ = msg.delete(&self.http).await;
                        log_violation(&self.state, &self.http, &ctx.cache, gid, &msg.author,
                            "UNAUTHORIZED LINK",
                            &format!("Posted non-whitelisted link: {}", url_str),
                            msg.channel_id).await;
                        return;
                    }
                }
            }
        }
    }
}

// ------------------------------------------------------------
//  COMMAND PROCESSING
// ------------------------------------------------------------
impl Handler {
    async fn process_commands(&self, ctx: Context, msg: &Message) {
        let content = &msg.content;
        let cl2 = content.to_lowercase();
        let prefix_len = if cl2.starts_with("null") { 4 } else if cl2.starts_with('x') { 1 } else { return; };
        let after_prefix = content[prefix_len..].trim();
        let args: Vec<&str> = after_prefix.split_whitespace().collect();
        let cmd = args.first().unwrap_or(&"").to_lowercase();
        let rest = &args[1..];
        let gid = match msg.guild_id { Some(g) => g, None => return };
        let author = &msg.author;
        let channel = msg.channel_id;

        let guild = match gid.to_guild_cached(&ctx.cache) {
            Some(g) => g,
            None => {
                let _ = channel.send_message(&self.http, |m| {
                    m.content("⚠️ Guild not cached yet — please wait a moment and try again.")
                }).await;
                return;
            }
        };
        let owner_id = guild.owner_id;
        let is_owner = || author.id == owner_id;
        let member = match guild.members.get(&author.id).cloned() {
            Some(m) => m,
            None => {
                match gid.member(&self.http, author.id).await {
                    Ok(m) => m,
                    Err(_) => {
                        let _ = channel.send_message(&self.http, |m| {
                            m.content("⚠️ Could not fetch member data.")
                        }).await;
                        return;
                    }
                }
            }
        };
        drop(guild);

        let effective_perms = if let Some(g) = gid.to_guild_cached(&ctx.cache) {
            let mut perms = Permissions::empty();
            if let Some(everyone) = g.roles.get(&RoleId(gid.0)) { perms |= everyone.permissions; }
            for role_id in &member.roles {
                if let Some(role) = g.roles.get(role_id) { perms |= role.permissions; }
            }
            perms
        } else { Permissions::empty() };

        let is_admin     = is_owner() || effective_perms.contains(Permissions::ADMINISTRATOR);
        let manage_msgs  = is_owner() || effective_perms.contains(Permissions::MANAGE_MESSAGES)   || effective_perms.contains(Permissions::ADMINISTRATOR);
        let ban_members  = is_owner() || effective_perms.contains(Permissions::BAN_MEMBERS)        || effective_perms.contains(Permissions::ADMINISTRATOR);
        let kick_members = is_owner() || effective_perms.contains(Permissions::KICK_MEMBERS)       || effective_perms.contains(Permissions::ADMINISTRATOR);
        let manage_roles = is_owner() || effective_perms.contains(Permissions::MANAGE_ROLES)       || effective_perms.contains(Permissions::ADMINISTRATOR);

        // local send_embed closure
        macro_rules! sembed {
            ($title:expr, $desc:expr, $color:expr) => {
                send_embed_fn(&self.http, channel, $title, $desc, $color).await
            };
        }

        match cmd.as_str() {
            "debug" => {
                let msg_text = format!(
                    "```\n[DEBUG]\nUser: {} ({})\nGuild: {}\nOwner ID: {}\nIs Owner: {}\nIs Admin: {}\nManage Messages: {}\nBan Members: {}\nKick Members: {}\nManage Roles: {}\nRaw Perms bits: {}\nRoles: {}\n```",
                    author.name, author.id.0, gid.0, owner_id.0,
                    is_owner(), is_admin, manage_msgs, ban_members, kick_members, manage_roles,
                    effective_perms.bits(),
                    member.roles.iter().map(|r| r.0.to_string()).collect::<Vec<_>>().join(", ")
                );
                let _ = channel.send_message(&self.http, |m| m.content(msg_text)).await;
            }
            "antinuke" | "security" => {
                if !is_owner() {
                    sembed!("🔒 Owner Only", "This command can only be used by the **server owner**.", 0xFF0000u32);
                    return;
                }
                if rest.is_empty() {
                    let enabled = self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false);
                    let status = if enabled { "ENABLED" } else { "DISABLED" };
                    let color  = if enabled { 0x00FF00u32 } else { 0xFF0000u32 };
                    let mut embed = CreateEmbed::default();
                    embed.title("Protection Status")
                        .description(format!("Anti-Nuke + Security Protection: **{}**", status))
                        .color(color).timestamp(now_ts());
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                    return;
                }
                let setting = rest[0].to_lowercase();
                if ["on","enable","true","1"].contains(&setting.as_str()) {
                    self.state.protection_enabled.insert(gid, true);
                    self.db.set_protection(gid, true).await;
                    let cfg = self.state.guild_configs.get(&gid).map(|c| c.clone()).unwrap_or_default();
                    let mut embed = CreateEmbed::default();
                    embed.title("Protection Enabled")
                        .description("Anti-Nuke + Security protection is now **ACTIVE**")
                        .color(0x00FF00u32).timestamp(now_ts())
                        .field("Now Protected Against", format!(
                            "• Webhook creation abuse\n• Mass channel create / delete / update\n• Server (guild) settings tampering\n• Role create / update / delete spam\n• Message spam, caps, invite links, banned words\nThreshold: **{} actions / {}s** → **{}**",
                            cfg.mass_ban_threshold, cfg.mass_ban_window_secs,
                            cfg.punishment.as_str().to_uppercase()
                        ), false)
                        .field("⚠️ Important", "Whitelist trusted admins with `xwhitelistuser @user` to avoid false triggers.", false)
                        .footer(|f| f.text("Coded by ransxmware.xyz — Protection"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else if ["off","disable","false","0"].contains(&setting.as_str()) {
                    self.state.protection_enabled.insert(gid, false);
                    self.db.set_protection(gid, false).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("Protection Disabled")
                        .description("Anti-Nuke + Security protection is now **INACTIVE**")
                        .color(0xFF0000u32).timestamp(now_ts())
                        .footer(|f| f.text("Coded by ransxmware.xyz — Protection"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else {
                    sembed!("❌ Invalid Setting", "Usage: `xantinuke on` or `xantinuke off`", 0xFF0000u32);
                }
            }
            "whitelistrole" => {
                if !is_owner() { sembed!("🔒 Owner Only", "Server owner only.", 0xFF0000u32); return; }
                if let Some(role_id) = msg.mention_roles.iter().next() {
                    let role = match gid.to_guild_cached(&ctx.cache).and_then(|g| g.roles.get(role_id).cloned()) {
                        Some(r) => r,
                        None => { sembed!("❌ Role Not Found", "Could not find that role.", 0xFF0000u32); return; }
                    };
                    let mut set = self.state.whitelist_roles.entry(gid).or_insert_with(HashSet::new);
                    if set.contains(&role.id) {
                        sembed!("❌ Already Whitelisted", "Role already whitelisted.", 0xFF0000u32); return;
                    }
                    set.insert(role.id);
                    self.db.add_whitelist_role(gid, role.id).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("✅ Role Whitelisted")
                        .description(format!("{} added to whitelist.", role.mention()))
                        .color(0x00FF00u32).timestamp(now_ts())
                        .footer(|f| f.text("Coded by ransxmware.xyz"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else {
                    sembed!("❌ Missing Role", "Usage: `xwhitelistrole @role`", 0xFF0000u32);
                }
            }
            "unwhitelistrole" => {
                if !is_owner() { sembed!("🔒 Owner Only", "Server owner only.", 0xFF0000u32); return; }
                if let Some(role_id) = msg.mention_roles.iter().next() {
                    let role = match gid.to_guild_cached(&ctx.cache).and_then(|g| g.roles.get(role_id).cloned()) {
                        Some(r) => r,
                        None => { sembed!("❌ Role Not Found", "Could not find that role.", 0xFF0000u32); return; }
                    };
                    let mut set = self.state.whitelist_roles.entry(gid).or_insert_with(HashSet::new);
                    if !set.contains(&role.id) {
                        sembed!("❌ Not Whitelisted", "Role not in whitelist.", 0xFF0000u32); return;
                    }
                    set.remove(&role.id);
                    self.db.remove_whitelist_role(gid, role.id).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("✅ Role Removed from Whitelist")
                        .description(format!("{} removed.", role.mention()))
                        .color(0x00FF00u32).timestamp(now_ts())
                        .footer(|f| f.text("Coded by ransxmware.xyz"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else {
                    sembed!("❌ Missing Role", "Usage: `xunwhitelistrole @role`", 0xFF0000u32);
                }
            }
            "whitelistuser" => {
                if !is_owner() { sembed!("🔒 Owner Only", "Server owner only.", 0xFF0000u32); return; }
                if let Some(user_mention) = msg.mentions.iter().next() {
                    let uid = user_mention.id;
                    let mut set = self.state.whitelist_users.entry(gid).or_insert_with(HashSet::new);
                    if set.contains(&uid) {
                        sembed!("❌ Already Whitelisted", "User already whitelisted.", 0xFF0000u32); return;
                    }
                    set.insert(uid);
                    self.db.add_whitelist_user(gid, uid).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("✅ User Whitelisted")
                        .description(format!("{} added to whitelist.", user_mention.mention()))
                        .color(0x00FF00u32).timestamp(now_ts())
                        .thumbnail(user_mention.avatar_url().unwrap_or_else(|| user_mention.default_avatar_url()))
                        .footer(|f| f.text("Coded by ransxmware.xyz"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else {
                    sembed!("❌ Missing User", "Usage: `xwhitelistuser @user`", 0xFF0000u32);
                }
            }
            "unwhitelistuser" => {
                if !is_owner() { sembed!("🔒 Owner Only", "Server owner only.", 0xFF0000u32); return; }
                if let Some(user_mention) = msg.mentions.iter().next() {
                    let uid = user_mention.id;
                    let mut set = self.state.whitelist_users.entry(gid).or_insert_with(HashSet::new);
                    if !set.contains(&uid) {
                        sembed!("❌ Not Whitelisted", "User not in whitelist.", 0xFF0000u32); return;
                    }
                    set.remove(&uid);
                    self.db.remove_whitelist_user(gid, uid).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("✅ User Removed from Whitelist")
                        .description(format!("{} removed.", user_mention.mention()))
                        .color(0x00FF00u32).timestamp(now_ts())
                        .footer(|f| f.text("Coded by ransxmware.xyz"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else {
                    sembed!("❌ Missing User", "Usage: `xunwhitelistuser @user`", 0xFF0000u32);
                }
            }
            "whitelistlist" => {
                if !is_owner() { sembed!("🔒 Owner Only", "Server owner only.", 0xFF0000u32); return; }
                let guild = match gid.to_guild_cached(&ctx.cache) {
                    Some(g) => g,
                    None => { sembed!("❌ Error", "Guild not cached.", 0xFF0000u32); return; }
                };
                let wl_roles = self.state.whitelist_roles.get(&gid).map(|s| {
                    s.iter().map(|rid| guild.roles.get(rid)
                        .map(|r| r.mention().to_string())
                        .unwrap_or_else(|| format!("<deleted role {}>", rid.0)))
                        .collect::<Vec<_>>().join("\n")
                }).filter(|s| !s.is_empty()).unwrap_or_else(|| "None".to_string());
                let wl_users = self.state.whitelist_users.get(&gid).map(|s| {
                    s.iter().map(|uid| guild.members.get(uid)
                        .map(|m| m.mention().to_string())
                        .unwrap_or_else(|| format!("<user {}>", uid.0)))
                        .collect::<Vec<_>>().join("\n")
                }).filter(|s| !s.is_empty()).unwrap_or_else(|| "None".to_string());
                let mut embed = CreateEmbed::default();
                embed.title("🛡️ Whitelist List")
                    .color(EMBED_COLOR).timestamp(now_ts())
                    .field("🔒 Whitelisted Roles", wl_roles, false)
                    .field("👤 Whitelisted Users", wl_users, false)
                    .footer(|f| f.text("Coded by ransxmware.xyz"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "bypasslink" => {
                if !is_owner() { sembed!("🔒 Owner Only", "Server owner only.", 0xFF0000u32); return; }
                if let Some(user_mention) = msg.mentions.iter().next() {
                    let uid = user_mention.id;
                    let mut set = self.state.link_bypass_users.entry(gid).or_insert_with(HashSet::new);
                    if set.contains(&uid) {
                        sembed!("❌ Already Bypassed", "User already has link bypass.", 0xFF0000u32); return;
                    }
                    set.insert(uid);
                    self.db.add_link_bypass_user(gid, uid).await;
                    sembed!("✅ Link Bypass Granted", "User can now post any link.", 0x00FF00u32);
                } else if let Some(rid) = msg.mention_roles.iter().next() {
                    let rid = *rid;
                    let mut set = self.state.link_bypass_roles.entry(gid).or_insert_with(HashSet::new);
                    if set.contains(&rid) {
                        sembed!("❌ Already Bypassed", "Role already has link bypass.", 0xFF0000u32); return;
                    }
                    set.insert(rid);
                    self.db.add_link_bypass_role(gid, rid).await;
                    sembed!("✅ Role Link Bypass Granted", "Role members can now post any link.", 0x00FF00u32);
                } else {
                    sembed!("❌ Invalid Target", "Mention a user or role.", 0xFF0000u32);
                }
            }
            "removebypasslink" => {
                if !is_owner() { sembed!("🔒 Owner Only", "Server owner only.", 0xFF0000u32); return; }
                if let Some(user_mention) = msg.mentions.iter().next() {
                    let uid = user_mention.id;
                    let mut set = self.state.link_bypass_users.entry(gid).or_insert_with(HashSet::new);
                    if !set.contains(&uid) {
                        sembed!("❌ Not Bypassed", "User does not have a link bypass.", 0xFF0000u32); return;
                    }
                    set.remove(&uid);
                    self.db.remove_link_bypass_user(gid, uid).await;
                    sembed!("✅ Link Bypass Revoked", "User link bypass removed.", 0xFF4500u32);
                } else if let Some(rid) = msg.mention_roles.iter().next() {
                    let rid = *rid;
                    let mut set = self.state.link_bypass_roles.entry(gid).or_insert_with(HashSet::new);
                    if !set.contains(&rid) {
                        sembed!("❌ Not Bypassed", "Role does not have a link bypass.", 0xFF0000u32); return;
                    }
                    set.remove(&rid);
                    self.db.remove_link_bypass_role(gid, rid).await;
                    sembed!("✅ Role Link Bypass Revoked", "Role link bypass removed.", 0xFF4500u32);
                } else {
                    sembed!("❌ Invalid Target", "Mention a user or role.", 0xFF0000u32);
                }
            }
            "setup" => {
                if !is_admin { sembed!("❌ Missing Permissions", "Need Administrator.", 0xFF0000u32); return; }
                if let Some(existing) = gid.channels(&self.http).await.ok()
                    .and_then(|ch| ch.into_iter().find(|(_, c)| c.name == "security-logs").map(|(_, c)| c))
                {
                    sembed!("Channel Already Exists", &format!("Already exists: {}", existing.mention()), 0xFFA500u32);
                    return;
                }
                let bot_id = match self.http.get_current_user().await {
                    Ok(u) => u.id,
                    Err(_) => { sembed!("❌ Error", "Could not fetch bot user.", 0xFF0000u32); return; }
                };
                let overwrites = vec![
                    PermissionOverwrite {
                        allow: Permissions::empty(), deny: Permissions::VIEW_CHANNEL,
                        kind: PermissionOverwriteType::Role(RoleId(gid.0)),
                    },
                    PermissionOverwrite {
                        allow: Permissions::VIEW_CHANNEL | Permissions::SEND_MESSAGES,
                        deny: Permissions::empty(),
                        kind: PermissionOverwriteType::Member(bot_id),
                    },
                ];
                let ow2 = overwrites.clone();
                if let Ok(new_channel) = gid.create_channel(&self.http, |c| {
                    c.name("security-logs").permissions(ow2)
                     .topic("Coded by ransxmware.xyz — Automated security logs")
                }).await {
                    sembed!("Setup Complete", &format!("Security logs channel created: {}", new_channel.mention()), 0x00FF00u32);
                } else {
                    sembed!("❌ Setup Failed", "An error occurred during setup.", 0xFF0000u32);
                }
            }
            "config" => {
                if !is_admin { sembed!("❌ Missing Permissions", "Need Administrator.", 0xFF0000u32); return; }
                let enabled = self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false);
                let cfg = self.state.guild_configs.get(&gid).map(|c| c.clone()).unwrap_or_default();
                let log_channel = gid.channels(&self.http).await.ok()
                    .and_then(|ch| ch.into_iter().find(|(_, c)| c.name == "security-logs")
                        .map(|(_, c)| c.mention().to_string()))
                    .unwrap_or_else(|| "❌ Not Set".to_string());
                let mut embed = CreateEmbed::default();
                embed.title("Security Configuration").color(EMBED_COLOR).timestamp(now_ts())
                    .field("Protection", if enabled { "✅ ENABLED" } else { "🔴 DISABLED" }, false)
                    .field("Logs Channel", log_channel, true)
                    .field("Anti-Nuke Settings", format!(
                        "Threshold: {} actions\nWindow: {}s\nPunishment: {}",
                        cfg.mass_ban_threshold, cfg.mass_ban_window_secs,
                        cfg.punishment.as_str().to_uppercase()
                    ), false)
                    .field("Security Limits", format!(
                        "Messages/min: {}\nDuplicate limit: {}\nMax emojis: {}\nAuto-ban at: {} violations",
                        cfg.max_messages_per_minute, cfg.max_duplicate_messages,
                        cfg.max_emojis, cfg.auto_ban_threshold
                    ), false)
                    .field("Allowed Domains", cfg.link_whitelist.join(", "), false)
                    .field("Banned Words", format!("{} words filtered", cfg.banned_words.len()), true)
                    .footer(|f| f.text("Coded by ransxmware.xyz"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "stats" => {
                if !manage_msgs { sembed!("❌ Missing Permissions", "Need Manage Messages.", 0xFF0000u32); return; }
                let total_violations: usize = self.state.user_violations.iter().map(|e| *e.value()).sum();
                let total_muted = self.state.muted_users.len();
                let total_warnings: usize = self.state.user_warnings.iter().map(|e| e.value().len()).sum();
                let mut embed = CreateEmbed::default();
                embed.title("Security Statistics").color(EMBED_COLOR).timestamp(now_ts())
                    .field("Total Violations", total_violations.to_string(), true)
                    .field("Currently Muted", total_muted.to_string(), true)
                    .field("Total Warnings", total_warnings.to_string(), true)
                    .field("Protection", if self.state.protection_enabled.get(&gid).map(|e| *e).unwrap_or(false) { "✅ Active" } else { "🔴 Inactive" }, true)
                    .footer(|f| f.text("Coded by ransxmware.xyz"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "purge" => {
                if !manage_msgs { sembed!("❌ Missing Permissions", "Need Manage Messages.", 0xFF0000u32); return; }
                let amount = match rest.first().and_then(|s| s.parse::<usize>().ok()) {
                    Some(a) if a > 0 && a <= 100 => a,
                    _ => { sembed!("❌ Invalid Amount", "Usage: `xpurge <1-100>`", 0xFF0000u32); return; }
                };
                let _ = msg.delete(&self.http).await;
                let messages = channel.messages(&self.http, |m| m.limit(amount as u64)).await.unwrap_or_default();
                if !messages.is_empty() {
                    let _ = channel.delete_messages(&self.http, messages.iter().collect::<Vec<_>>()).await;
                }
                let count = messages.len();
                let mut embed = CreateEmbed::default();
                embed.title("Messages Purged")
                    .description(format!("Deleted **{}** messages from {}", count, channel.mention()))
                    .color(0x00FF00u32).timestamp(now_ts())
                    .footer(|f| f.text("Coded by ransxmware.xyz"));
                if let Ok(s) = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let _ = s.delete(&self.http).await;
                }
            }
            "warn" => {
                if !manage_msgs { sembed!("❌ Missing Permissions", "Need Manage Messages.", 0xFF0000u32); return; }
                let target = match msg.mentions.iter().next() {
                    Some(u) => u,
                    None => { sembed!("❌ Missing User", "Usage: `xwarn @user <reason>`", 0xFF0000u32); return; }
                };
                let reason = if rest.len() > 1 { rest[1..].join(" ") } else { "No reason provided".to_string() };
                if target.id == author.id { sembed!("❌ Cannot Warn Yourself", "You cannot warn yourself!", 0xFF0000u32); return; }
                let warning = WarningData { reason: reason.clone(), moderator: author.id, timestamp: now_pht(), guild_id: gid };
                let mut warnings = self.state.user_warnings.entry(target.id).or_insert_with(Vec::new);
                warnings.push(warning);
                self.db.log_action(gid, target.id, "WARN", &reason).await;
                let count = warnings.iter().filter(|w| w.guild_id == gid).count();
                let mut embed = CreateEmbed::default();
                embed.title("⚠️ USER WARNING").color(0xFFA500u32).timestamp(now_ts())
                    .field("User", target.mention(), true)
                    .field("Moderator", author.mention(), true)
                    .field("Count", count.to_string(), true)
                    .field("Reason", reason.clone(), false)
                    .thumbnail(target.avatar_url().unwrap_or_else(|| target.default_avatar_url()))
                    .footer(|f| f.text("Coded by ransxmware.xyz"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "mute" => {
                if !manage_msgs { sembed!("❌ Missing Permissions", "Need Manage Messages.", 0xFF0000u32); return; }
                let target = match msg.mentions.iter().next() {
                    Some(u) => u,
                    None => { sembed!("❌ Missing User", "Usage: `xmute @user [minutes] [reason]`", 0xFF0000u32); return; }
                };
                if target.id == author.id { sembed!("❌ Cannot Mute Yourself", "You cannot mute yourself!", 0xFF0000u32); return; }
                let duration = rest.get(1).and_then(|s| s.parse::<i64>().ok()).unwrap_or(10);
                let reason = if rest.len() > 2 { rest[2..].join(" ") } else { "No reason provided".to_string() };
                let until = now_pht() + ChronoDuration::minutes(duration);
                self.state.muted_users.insert(target.id, until);
                self.db.add_mute(gid, target.id, until).await;
                self.db.log_action(gid, target.id, "MUTE", &format!("{}min — {}", duration, reason)).await;
                let mut embed = CreateEmbed::default();
                embed.title("USER MUTED").color(0xFF4500u32).timestamp(now_ts())
                    .field("User", target.mention(), true)
                    .field("Duration", format!("{} minutes", duration), true)
                    .field("Reason", reason, false)
                    .field("Expires", until.to_rfc3339(), false)
                    .thumbnail(target.avatar_url().unwrap_or_else(|| target.default_avatar_url()))
                    .footer(|f| f.text("Coded by ransxmware.xyz"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "unmute" => {
                if !manage_msgs { sembed!("❌ Missing Permissions", "Need Manage Messages.", 0xFF0000u32); return; }
                let target = match msg.mentions.iter().next() {
                    Some(u) => u,
                    None => { sembed!("❌ Missing User", "Usage: `xunmute @user`", 0xFF0000u32); return; }
                };
                if !self.state.muted_users.contains_key(&target.id) {
                    sembed!("❌ Not Muted", "User is not muted.", 0xFF0000u32); return;
                }
                self.state.muted_users.remove(&target.id);
                self.db.remove_mute(gid, target.id).await;
                let mut embed = CreateEmbed::default();
                embed.title("USER UNMUTED")
                    .description(format!("{} has been unmuted.", target.mention()))
                    .color(0x00FF00u32).timestamp(now_ts())
                    .footer(|f| f.text("Coded by ransxmware.xyz"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "role" => {
                if !manage_roles { sembed!("❌ Missing Permissions", "Need Manage Roles.", 0xFF0000u32); return; }
                let target = match msg.mentions.iter().next() {
                    Some(u) => u,
                    None => { sembed!("❌ Missing User", "Usage: `xrole @user @role`", 0xFF0000u32); return; }
                };
                let role_id = match msg.mention_roles.iter().next() {
                    Some(r) => *r,
                    None => { sembed!("❌ Missing Role", "Please mention a role.", 0xFF0000u32); return; }
                };
                let role = match gid.to_guild_cached(&ctx.cache).and_then(|g| g.roles.get(&role_id).cloned()) {
                    Some(r) => r,
                    None => { sembed!("❌ Role Not Found", "Could not find that role.", 0xFF0000u32); return; }
                };
                if let Ok(mut member) = gid.member(&self.http, target.id).await {
                    if member.roles.contains(&role.id) {
                        sembed!("Role Already Assigned", "User already has this role.", 0xFFA500u32); return;
                    }
                    let _ = member.add_role(&self.http, role.id).await;
                    self.db.log_action(gid, target.id, "ROLE GIVEN", &format!("{}", role.name)).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("Role Assigned")
                        .description(format!("Gave {} the role {}", target.mention(), role.mention()))
                        .color(EMBED_COLOR).timestamp(now_ts())
                        .footer(|f| f.text("Coded by ransxmware.xyz"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else {
                    sembed!("❌ User Not Found", "Could not find that member.", 0xFF0000u32);
                }
            }
            "iplookup" => {
                if !manage_msgs { sembed!("❌ Missing Permissions", "Need Manage Messages.", 0xFF0000u32); return; }
                let ip = match rest.first() {
                    Some(i) => *i,
                    None => { sembed!("❌ Missing IP", "Usage: `xiplookup <ip>`", 0xFF0000u32); return; }
                };
                let ip_re = Regex::new(r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$").unwrap();
                if !ip_re.is_match(ip) {
                    sembed!("❌ Invalid IP", "Please provide a valid IPv4 address.", 0xFF0000u32); return;
                }
                match reqwest::get(&format!("http://ip-api.com/json/{}", ip)).await {
                    Ok(resp) => match resp.json::<serde_json::Value>().await {
                        Ok(data) if data["status"].as_str() == Some("success") => {
                            let lat = data["lat"].as_f64().unwrap_or(0.0);
                            let lon = data["lon"].as_f64().unwrap_or(0.0);
                            let mut embed = CreateEmbed::default();
                            embed.title("[+] IP Address Lookup").color(0x0099FFu32).timestamp(now_ts())
                                .field("City",    data["city"].as_str().unwrap_or("Unknown"), true)
                                .field("Region",  data["regionName"].as_str().unwrap_or("Unknown"), true)
                                .field("Country", format!("{} ({})", data["country"].as_str().unwrap_or("Unknown"), data["countryCode"].as_str().unwrap_or("N/A")), true)
                                .field("ISP",     data["isp"].as_str().unwrap_or("Unknown"), true)
                                .field("AS",      data["as"].as_str().unwrap_or("Unknown"), true)
                                .field("Coords",  format!("{}, {}", lat, lon), true)
                                .field("TZ",      data["timezone"].as_str().unwrap_or("Unknown"), true)
                                .footer(|f| f.text("IP Lookup Service"));
                            let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                        }
                        _ => { sembed!("❌ Lookup Failed", "Could not retrieve info for this IP.", 0xFF0000u32); }
                    },
                    Err(_) => { sembed!("❌ Service Unavailable", "IP lookup service unavailable.", 0xFF0000u32); }
                }
            }
            "ipgrab" => {
                if !manage_msgs { sembed!("❌ Missing Permissions", "Need Manage Messages.", 0xFF0000u32); return; }
                let target = if let Some(u) = msg.mentions.iter().next() { u.clone() } else { author.clone() };
                let (fake_ip, city, province, isp, lat, lon) = {
                    let mut rng = rand::thread_rng();
                    let ph_ranges = ["202.90","203.177","210.213","218.108","124.105","112.198","180.190"];
                    let fake_ip = format!("{}.{}.{}", ph_ranges[rng.gen::<usize>() % ph_ranges.len()], rng.gen::<u8>(), rng.gen::<u8>());
                    let cities = ["Manila","Quezon City","Makati","Cebu City","Davao City","Taguig","Pasig"];
                    let provinces = ["Metro Manila","Cebu","Davao del Sur","Cavite","Rizal"];
                    let isps = ["PLDT Inc.","Globe Telecom","Smart Communications","Sky Broadband","Converge ICT"];
                    let lat: f64 = rng.gen::<f64>() * (21.0 - 4.5) + 4.5;
                    let lon: f64 = rng.gen::<f64>() * (127.0 - 116.0) + 116.0;
                    (fake_ip, cities[rng.gen::<usize>() % cities.len()].to_string(), provinces[rng.gen::<usize>() % provinces.len()].to_string(), isps[rng.gen::<usize>() % isps.len()].to_string(), lat, lon)
                };
                let mut embed = CreateEmbed::default();
                embed.title("IP GRAB").color(0xFF0000u32).timestamp(now_ts())
                    .description(format!("**@GRABBED: {}**", target.name))
                    .field("IP",      format!("`{}`", fake_ip), true)
                    .field("Status",  "**CONFIRMED**", true)
                    .field("City",    city, true)
                    .field("Province", province, true)
                    .field("ISP",     isp, true)
                    .field("Country", "Philippines", true)
                    .field("Coords",  format!("{:.6}, {:.6}", lat, lon), true)
                    .thumbnail(target.avatar_url().unwrap_or_else(|| target.default_avatar_url()))
                    .footer(|f| f.text("Coded by ransxmware.xyz"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                tokio::time::sleep(Duration::from_secs(5)).await;
                let mut reveal = CreateEmbed::default();
                reveal.title("😂 Got You!")
                    .description(format!("Relax {}, that was **100% FAKE**!\nNo actual IP was captured. Discord does not expose user IPs.", target.mention()))
                    .color(0x00FF00u32).timestamp(now_ts())
                    .footer(|f| f.text("Stay safe online 💚"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = reveal.clone(); e })).await;
            }
            "status" => {
                if !is_admin { sembed!("❌ Missing Permissions", "Need Administrator.", 0xFF0000u32); return; }
                let status_str = match rest.first() {
                    Some(s) => s.to_lowercase(),
                    None => { sembed!("❌ Missing Status", "Usage: `xstatus <online/dnd/invisible>`", 0xFF0000u32); return; }
                };
                let status = match status_str.as_str() {
                    "online"    => serenity::model::user::OnlineStatus::Online,
                    "dnd"       => serenity::model::user::OnlineStatus::DoNotDisturb,
                    "invisible" => serenity::model::user::OnlineStatus::Invisible,
                    _ => { sembed!("❌ Invalid Status", "Valid: `online`, `dnd`, `invisible`", 0xFF0000u32); return; }
                };
                let server_count = ctx.cache.guilds().len();
                ctx.set_presence(
                    Some(serenity::model::gateway::Activity::watching(format!("over {} servers!", server_count))),
                    status,
                ).await;
                sembed!("Status Updated", &format!("Bot status changed to **{}**", status_str.to_uppercase()), 0x00FF00u32);
            }
            "violations" => {
                if !manage_msgs { sembed!("❌ Missing Permissions", "Need Manage Messages.", 0xFF0000u32); return; }
                let target = if let Some(u) = msg.mentions.iter().next() { u.clone() } else { author.clone() };
                let vcount = self.state.user_violations.get(&target.id).map(|c| *c).unwrap_or(0);
                let wcount = self.state.user_warnings.get(&target.id).map(|w| w.iter().filter(|w| w.guild_id == gid).count()).unwrap_or(0);
                let is_muted = self.state.muted_users.contains_key(&target.id);
                let (risk, color) = if vcount == 0 { ("Clean Record", 0x00FF00u32) }
                    else if vcount < 3 { ("Low Risk", 0xFFFF00u32) }
                    else if vcount < 5 { ("Medium Risk", 0xFF8000u32) }
                    else { ("High Risk", 0xFF0000u32) };
                let mut embed = CreateEmbed::default();
                embed.title("User Violations Report").color(color).timestamp(now_ts())
                    .field("Security Violations", vcount.to_string(), true)
                    .field("Warnings", wcount.to_string(), true)
                    .field("Muted", if is_muted { "Yes" } else { "No" }, true)
                    .field("Risk Level", risk, false)
                    .thumbnail(target.avatar_url().unwrap_or_else(|| target.default_avatar_url()))
                    .footer(|f| f.text("Coded by ransxmware.xyz"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "clearviolations" => {
                if !is_admin { sembed!("❌ Missing Permissions", "Need Administrator.", 0xFF0000u32); return; }
                let target = match msg.mentions.iter().next() {
                    Some(u) => u,
                    None => { sembed!("❌ Missing User", "Usage: `xclearviolations @user`", 0xFF0000u32); return; }
                };
                let old = self.state.user_violations.get(&target.id).map(|c| *c).unwrap_or(0);
                self.state.user_violations.insert(target.id, 0);
                if let Some(mut warns) = self.state.user_warnings.get_mut(&target.id) {
                    warns.retain(|w| w.guild_id != gid);
                }
                let mut embed = CreateEmbed::default();
                embed.title("Violations Cleared").color(0x00FF00u32).timestamp(now_ts())
                    .field("Previous", old.to_string(), true)
                    .field("Current", "0", true)
                    .field("Cleared by", author.mention(), true)
                    .footer(|f| f.text("Coded by ransxmware.xyz"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "ping" => {
                let start = std::time::Instant::now();
                let mut sent = match channel.send_message(&self.http, |m| m.content("🏓 Pinging...")).await {
                    Ok(m) => m, Err(_) => return
                };
                let api_latency = start.elapsed().as_millis();
                let (quality, color) = if api_latency < 80 { ("🟢 Excellent", 0x00FF7Fu32) }
                    else if api_latency < 150 { ("🟡 Good", 0xFFFF00u32) }
                    else if api_latency < 300 { ("🟠 Fair", 0xFF8C00u32) }
                    else { ("🔴 Poor", 0xFF0000u32) };
                let mut embed = CreateEmbed::default();
                embed.title("🏓 Pong!").color(color).timestamp(now_ts())
                    .field("API Round-Trip", format!("`{}ms` — {}", api_latency, quality), true)
                    .field("Semaphore", format!("`{}/20` free", self.state.api_semaphore.available_permits()), true)
                    .footer(|f| f.text("Coded by ransxmware.xyz"));
                let _ = sent.edit(&self.http, |e| e.content("").embed(|em| { *em = embed.clone(); em })).await;
            }
            "kick" => {
                if !kick_members { sembed!("❌ Missing Permissions", "Need Kick Members.", 0xFF0000u32); return; }
                let target = match msg.mentions.iter().next() {
                    Some(u) => u,
                    None => { sembed!("❌ Missing User", "Usage: `xkick @user [reason]`", 0xFF0000u32); return; }
                };
                let reason = if rest.len() > 1 { rest[1..].join(" ") } else { "No reason provided".to_string() };
                if target.id == author.id { sembed!("❌ Cannot Kick Yourself", "You cannot kick yourself!", 0xFF0000u32); return; }
                if let Ok(member) = gid.member(&self.http, target.id).await {
                    let _ = member.kick_with_reason(&self.http, &reason).await;
                    self.db.log_action(gid, target.id, "KICK", &format!("by {} — {}", author.tag(), reason)).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("👢 Member Kicked")
                        .description(format!("{} has been kicked.", target.mention()))
                        .color(0xFF4500u32).timestamp(now_ts())
                        .field("Reason", reason, false)
                        .field("By", author.mention(), true)
                        .thumbnail(target.avatar_url().unwrap_or_else(|| target.default_avatar_url()))
                        .footer(|f| f.text("Coded by ransxmware.xyz"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else {
                    sembed!("❌ User Not Found", "Could not find that member.", 0xFF0000u32);
                }
            }
            "ban" => {
                if !ban_members { sembed!("❌ Missing Permissions", "Need Ban Members.", 0xFF0000u32); return; }
                let target = match msg.mentions.iter().next() {
                    Some(u) => u,
                    None => { sembed!("❌ Missing User", "Usage: `xban @user [reason]`", 0xFF0000u32); return; }
                };
                let reason = if rest.len() > 1 { rest[1..].join(" ") } else { "No reason provided".to_string() };
                if target.id == author.id { sembed!("❌ Cannot Ban Yourself", "You cannot ban yourself!", 0xFF0000u32); return; }
                if let Ok(member) = gid.member(&self.http, target.id).await {
                    let _ = member.ban_with_reason(&self.http, 0, &reason).await;
                    self.db.log_action(gid, target.id, "BAN", &format!("by {} — {}", author.tag(), reason)).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("🔨 Member Banned")
                        .description(format!("{} has been permanently banned.", target.mention()))
                        .color(0xFF0000u32).timestamp(now_ts())
                        .field("Reason", reason, false)
                        .field("By", author.mention(), true)
                        .thumbnail(target.avatar_url().unwrap_or_else(|| target.default_avatar_url()))
                        .footer(|f| f.text("Coded by ransxmware.xyz"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else {
                    sembed!("❌ User Not Found", "Could not find that member.", 0xFF0000u32);
                }
            }
            "unban" => {
                if !ban_members { sembed!("❌ Missing Permissions", "Need Ban Members.", 0xFF0000u32); return; }
                let query = rest.join(" ").trim().to_string();
                if query.is_empty() {
                    sembed!("❌ Missing User", "Usage: `xunban <user ID or username>`", 0xFF0000u32); return;
                }
                let bans = gid.bans(&self.http).await.unwrap_or_default();
                let mut found_user: Option<User> = None;
                if let Ok(uid) = query.parse::<u64>() {
                    found_user = bans.iter().find(|b| b.user.id.0 == uid).map(|b| b.user.clone());
                }
                if found_user.is_none() {
                    let lower = query.to_lowercase();
                    found_user = bans.iter().find(|b| b.user.name.to_lowercase().contains(&lower)).map(|b| b.user.clone());
                }
                if let Some(user) = found_user {
                    let _ = gid.unban(&self.http, user.id).await;
                    self.db.log_action(gid, user.id, "UNBAN", &format!("by {}", author.tag())).await;
                    let mut embed = CreateEmbed::default();
                    embed.title("✅ Member Unbanned")
                        .description(format!("**{}** has been unbanned.", user.tag()))
                        .color(0x00FF00u32).timestamp(now_ts())
                        .field("By", author.mention(), true)
                        .footer(|f| f.text("Coded by ransxmware.xyz"));
                    let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
                } else {
                    sembed!("❌ User Not Found", "Could not find that user in the ban list. Try using their user ID.", 0xFF0000u32);
                }
            }
            "av" => {
                let target = if let Some(u) = msg.mentions.iter().next() {
                    Some(u.clone())
                } else if !rest.is_empty() {
                    let name = rest.join(" ").to_lowercase();
                    ctx.cache.guild(gid).and_then(|g| {
                        g.members.values().find(|m| {
                            m.display_name().to_lowercase().contains(&name)
                                || m.user.name.to_lowercase().contains(&name)
                        }).map(|m| m.user.clone())
                    })
                } else { None };
                if let Some(user) = target {
                    let mut embed = CreateEmbed::default();
                    embed.color(EMBED_COLOR)
                        .image(user.avatar_url().unwrap_or_else(|| user.default_avatar_url()));
                    let _ = channel.send_message(&self.http, |m| {
                        m.content(format!("Avatar for {}", user.mention()))
                         .embed(|e| { *e = embed.clone(); e })
                    }).await;
                } else {
                    sembed!("❌ User Not Found", "No member found matching that name.", 0xFF0000u32);
                }
            }
            "serverinfo" => {
                let guild = match gid.to_guild_cached(&ctx.cache) {
                    Some(g) => g,
                    None => { sembed!("❌ Error", "Guild not cached.", 0xFF0000u32); return; }
                };
                let owner = match guild.members.get(&guild.owner_id).cloned() {
                    Some(m) => m.user,
                    None => match guild.owner_id.to_user(&self.http).await {
                        Ok(u) => u,
                        Err(_) => { sembed!("❌ Error", "Could not fetch owner.", 0xFF0000u32); return; }
                    }
                };
                let boost_level = premium_tier_num(guild.premium_tier);
                let boost_count = guild.premium_subscription_count;
                let channels = gid.channels(&self.http).await.unwrap_or_default();
                let text  = channels.values().filter(|c| c.kind == ChannelType::Text).count();
                let voice = channels.values().filter(|c| c.kind == ChannelType::Voice).count();
                let cats  = channels.values().filter(|c| c.kind == ChannelType::Category).count();
                let mut embed = CreateEmbed::default();
                embed.title(format!("☁️ {}", guild.name))
                    .color(EMBED_COLOR).timestamp(now_ts())
                    .field("Owner", format!("{} ({})", owner.mention(), owner.name), false)
                    .field("ID", guild.id.0.to_string(), false)
                    .field("Members", guild.member_count.to_string(), false)
                    .field("Boosts", format!("{} (Level {})", boost_count, boost_level), false)
                    .field("Roles", guild.roles.len().to_string(), false)
                    .field("Channels", format!("{} text · {} voice · {} categories", text, voice, cats), false);
                if let Some(icon) = guild.icon_url() { embed.thumbnail(icon); }
                embed.footer(|f| f.text("Coded by ransxmware.xyz"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "addlink" => {
                if !is_admin { sembed!("❌ Missing Permissions", "Need Administrator.", 0xFF0000u32); return; }
                let domain = match rest.first() {
                    Some(d) => d.to_lowercase().trim_start_matches("https://").trim_start_matches("http://").split('/').next().unwrap_or(d).to_string(),
                    None => { sembed!("❌ Missing Domain", "Usage: `xaddlink <domain>`", 0xFF0000u32); return; }
                };
                {
                    let mut cfg = self.state.guild_configs.entry(gid).or_insert_with(GuildSecurityConfig::default);
                    if cfg.link_whitelist.contains(&domain) {
                        sembed!("❌ Already Whitelisted", "Domain already in list.", 0xFF0000u32); return;
                    }
                    cfg.link_whitelist.push(domain.clone());
                    self.db.save_guild_config(gid, &cfg).await;
                }
                sembed!("✅ Link Whitelisted", &format!("`{}` added to allowed links.", domain), 0x00FF00u32);
            }
            "removelink" => {
                if !is_admin { sembed!("❌ Missing Permissions", "Need Administrator.", 0xFF0000u32); return; }
                let domain = match rest.first() {
                    Some(d) => d.to_lowercase().trim_start_matches("https://").trim_start_matches("http://").split('/').next().unwrap_or(d).to_string(),
                    None => { sembed!("❌ Missing Domain", "Usage: `xremovelink <domain>`", 0xFF0000u32); return; }
                };
                {
                    let mut cfg = self.state.guild_configs.entry(gid).or_insert_with(GuildSecurityConfig::default);
                    let pos = cfg.link_whitelist.iter().position(|d| *d == domain);
                    match pos {
                        Some(p) => { cfg.link_whitelist.remove(p); self.db.save_guild_config(gid, &cfg).await; }
                        None => { sembed!("❌ Not in Whitelist", "Domain not in list.", 0xFF0000u32); return; }
                    }
                }
                sembed!("✅ Link Removed", &format!("`{}` removed from allowed links.", domain), 0x00FF00u32);
            }
            "history" => {
                if !manage_msgs { sembed!("❌ Missing Permissions", "Need Manage Messages.", 0xFF0000u32); return; }
                let target = match msg.mentions.iter().next() {
                    Some(u) => u,
                    None => { sembed!("❌ Missing User", "Usage: `xhistory @user`", 0xFF0000u32); return; }
                };
                let rows = sqlx::query(
                    "SELECT action, reason, timestamp FROM action_history WHERE guild_id = $1 AND user_id = $2 ORDER BY id DESC LIMIT 15"
                )
                .bind(gid.0 as i64)
                .bind(target.id.0 as i64)
                .fetch_all(&self.db.pool).await.unwrap_or_default();
                let mut embed = CreateEmbed::default();
                embed.title(format!("Action History — {}", target.name))
                    .color(EMBED_COLOR).timestamp(now_ts())
                    .thumbnail(target.avatar_url().unwrap_or_else(|| target.default_avatar_url()));
                if rows.is_empty() {
                    embed.field("No History", "No recorded actions for this user.", false);
                } else {
                    for row in &rows {
                        use sqlx::Row;
                        let action: &str = row.get("action");
                        let reason: &str = row.get("reason");
                        let timestamp: &str = row.get("timestamp");
                        embed.field(format!("`{}` — {}", action, timestamp), reason, false);
                    }
                }
                let violations = self.state.user_violations.get(&target.id).map(|c| *c).unwrap_or(0);
                embed.field("User ID", format!("`{}`", target.id), true)
                    .field("Violations", violations.to_string(), true)
                    .footer(|f| f.text("Coded by ransxmware.xyz"));
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            "help" => {
                let mut embed = CreateEmbed::default();
                embed.title("🛡️ Null, X : Security Menu").description("**REVSHIT**")
                    .color(EMBED_COLOR).timestamp(now_ts())
                    .field("🔒 Anti-Nuke",
                        "`xantinuke on/off` `xsecurity on/off` `xsetup` `xconfig` `xstats`", true)
                    .field("📋 Whitelist",
                        "`xwhitelistrole` `xunwhitelistrole` `xwhitelistuser` `xunwhitelistuser` `xwhitelistlist` `xbypasslink` `xremovebypasslink` `xaddlink` `xremovelink`", true)
                    .field("⚔️ Moderation",
                        "`xpurge` `xwarn` `xmute` `xunmute` `xkick` `xban` `xunban` `xrole` `xhistory`", true)
                    .field("🔧 Utility",
                        "`xping` `xiplookup` `xipgrab` `xstatus` `xav` `xserverinfo` `xviolations` `xclearviolations`", true)
                    .footer(|f| f.text("Coded by ransxmware.xyz — Prefix: x or null"))
                    .thumbnail(ctx.cache.current_user().avatar_url().unwrap_or_default());
                let _ = channel.send_message(&self.http, |m| m.embed(|e| { *e = embed.clone(); e })).await;
            }
            _ => {}
        }
    }
}

// ------------------------------------------------------------
//  SLASH COMMANDS (poise)
// ------------------------------------------------------------
type Error = Box<dyn std::error::Error + Send + Sync>;

#[poise::command(slash_command, prefix_command)]
async fn antinuke_slash(
    ctx: poise::Context<'_, Data, Error>,
    #[description = "Enable or disable protection"] action: String,
) -> Result<(), Error> {
    let gid = ctx.guild_id().unwrap();
    let data = ctx.data();
    let enabled = matches!(action.to_lowercase().as_str(), "on" | "enable" | "true" | "1");
    data.state.protection_enabled.insert(gid, enabled);
    data.db.set_protection(gid, enabled).await;
    ctx.say(format!("🛡️ Protection {}", if enabled { "enabled" } else { "disabled" })).await?;
    Ok(())
}

#[poise::command(slash_command, prefix_command)]
async fn whitelist_user_slash(
    ctx: poise::Context<'_, Data, Error>,
    #[description = "User to whitelist"] user: serenity_poise::User,
) -> Result<(), Error> {
    let gid = ctx.guild_id().unwrap();
    let data = ctx.data();
    data.state.whitelist_users.entry(gid).or_insert_with(HashSet::new).insert(user.id);
    data.db.add_whitelist_user(gid, user.id).await;
    ctx.say(format!("✅ Whitelisted {}", user.mention())).await?;
    Ok(())
}

#[poise::command(slash_command, prefix_command)]
async fn unwhitelist_user_slash(
    ctx: poise::Context<'_, Data, Error>,
    #[description = "User to remove from whitelist"] user: serenity_poise::User,
) -> Result<(), Error> {
    let gid = ctx.guild_id().unwrap();
    let data = ctx.data();
    if let Some(mut set) = data.state.whitelist_users.get_mut(&gid) { set.remove(&user.id); }
    data.db.remove_whitelist_user(gid, user.id).await;
    ctx.say(format!("✅ Removed {} from whitelist", user.mention())).await?;
    Ok(())
}

#[poise::command(slash_command, prefix_command)]
async fn second_owner_slash(
    ctx: poise::Context<'_, Data, Error>,
    #[description = "User to set as second owner (or none to clear)"] user: Option<serenity_poise::User>,
) -> Result<(), Error> {
    let gid = ctx.guild_id().unwrap();
    let data = ctx.data();
    let guild = gid.to_guild_cached(&ctx.cache()).unwrap();
    if ctx.author().id != guild.owner_id {
        ctx.say("Only the server owner can set the second owner.").await?;
        return Ok(());
    }
    let uid = user.as_ref().map(|u| u.id);
    let reply_text = if let Some(ref u) = user {
        format!("👑 Set {} as second owner.", u.mention())
    } else {
        "✅ Removed second owner.".to_string()
    };
    {
        let mut cfg = data.state.guild_configs.entry(gid).or_insert_with(GuildSecurityConfig::default);
        cfg.second_owner_id = uid;
        data.db.save_guild_config(gid, &cfg).await;
    }
    ctx.say(reply_text).await?;
    Ok(())
}

struct Data {
    state: Arc<BotState>,
    db: Arc<Database>,
    http: Arc<Http>,
}

// ------------------------------------------------------------
//  MAIN
// ------------------------------------------------------------
#[tokio::main]
async fn main() -> Result<(), Error> {
    let token = std::env::var("DISCORD_TOKEN")?;
    let db_url = std::env::var("DATABASE_URL")?;
    let state = Arc::new(BotState::new());
    let db = Arc::new(Database::new(&db_url).await);
    db.load_all(&state).await;
    let http = Arc::new(Http::new(&token));
    let data = Data { state: state.clone(), db: db.clone(), http: http.clone() };

    let framework = poise::Framework::builder()
        .options(poise::FrameworkOptions {
            commands: vec![
                antinuke_slash(),
                whitelist_user_slash(),
                unwhitelist_user_slash(),
                second_owner_slash(),
            ],
            prefix_options: poise::PrefixFrameworkOptions {
                prefix: Some("x".into()),
                additional_prefixes: vec![poise::Prefix::Literal("null")],
                ..Default::default()
            },
            ..Default::default()
        })
        .setup(|ctx, _ready, framework| {
            Box::pin(async move {
                poise::builtins::register_globally(ctx, &framework.options().commands).await?;
                Ok(data)
            })
        })
        .build();

    let mut client = Client::builder(&token, GatewayIntents::all())
        .framework(framework)
        .event_handler(Handler { state, db, http })
        .await?;
    client.start().await?;
    Ok(())
}
