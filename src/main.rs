//! Cockatiel soundboard module.
//!
//! Viewers spend points to play a whitelisted sound effect on the STREAMER'S
//! OWN machine — the audio twin of the fake-input module. The streamer opts in
//! and defines WHICH sounds viewers may trigger (a whitelist: name -> file),
//! so nothing unlisted can ever be played.
//!
//! Command: `!snd <name>` (e.g. `!snd airhorn`, `!snd spongebob`). The engine
//! gates the module (price / authority / min_rank) and charges the viewer
//! once; this module must NOT deduct a second time — it just plays the sound.
//!
//! Playback rule: sounds play SERIALLY — only one at a time. Each viewer's
//! next sound waits until the current file has finished playing, then starts
//! immediately. An optional `cooldown_secs` in config adds a fixed pause
//! between plays on top of that (streamer-adjustable; default 0 = play the
//! moment the previous file ends).

use std::collections::HashMap;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use cockatiel_client::proto::container::Payload;
use cockatiel_client::proto::*;
use cockatiel_client::CockatielClient;
use futures_util::{SinkExt, StreamExt};
use prost::Message;
use rodio::{Decoder, OutputStream, OutputStreamHandle, Sink};
use serde::{Deserialize, Serialize};
use std::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{error, info, warn};
use tracing_subscriber::FmtSubscriber;

type WsWriteHalf = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    WsMessage,
>;

const COMMAND_NAME: &str = "snd";

// ── config ────────────────────────────────────────────────────────────────

/// The module's editable settings, persisted under config.json's flat
/// `module_specific` object (the convention every module uses).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
struct Config {
    /// The command flag prefix (e.g. `!`).
    command_flag: String,
    /// How much score a viewer must spend to trigger a sound. Deducted from
    /// their CURRENT score by the engine's gate; the lifetime total is
    /// untouched.
    price: u64,
    /// The folder the streamer drops audio files into. The module scans it and
    /// exposes every supported file (wav/mp3/flac/ogg) as `!snd <file stem>`.
    /// A newly dropped file appears without a restart; a removed file stops
    /// working.
    #[serde(default = "default_sounds_dir")]
    sounds_dir: String,
    /// The single playback volume in [0, 1], applied AFTER normalisation, so
    /// every clip sounds the same loudness regardless of how the source file
    /// was recorded.
    #[serde(default = "default_volume")]
    volume: f32,
    /// Fixed pause (seconds) between plays. Default 0 = the next sound starts
    /// the moment the previous file finishes. Streamer-adjustable.
    #[serde(default)]
    cooldown_secs: u64,
    /// Reconnect backoff bounds (seconds).
    reconnect_base_secs: u64,
    reconnect_max_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            command_flag: "!".into(),
            price: 10_000,
            sounds_dir: default_sounds_dir(),
            volume: default_volume(),
            cooldown_secs: 0,
            reconnect_base_secs: 1,
            reconnect_max_secs: 30,
        }
    }
}

fn default_sounds_dir() -> String {
    "sounds".into()
}

fn default_volume() -> f32 {
    1.0
}

/// The audio file extensions the soundboard accepts.
const SUPPORTED_EXTENSIONS: &[&str] = &["wav", "mp3", "flac", "ogg", "m4a", "aac"];

/// Scan `sounds_dir` for supported audio files and return `stem -> path`.
/// `stem` is the file name without its extension, so `airhorn.wav` is
/// triggered with `!snd airhorn`. Hidden files (leading `.`) are skipped.
fn scan_sounds_dir(sounds_dir: &str) -> HashMap<String, PathBuf> {
    let mut out = HashMap::new();
    let Ok(entries) = std::fs::read_dir(sounds_dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else { continue };
        if name.starts_with('.') {
            continue;
        }
        let Some(ext) = path.extension().and_then(|e| e.to_str()) else { continue };
        if SUPPORTED_EXTENSIONS.contains(&ext.to_ascii_lowercase().as_str()) {
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or(name);
            out.insert(stem.to_string(), path);
        }
    }
    out
}

fn load_config(path: &Path) -> Config {
    let data = std::fs::read_to_string(path).unwrap_or_default();
    let root: serde_json::Value =
        serde_json::from_str(&data).unwrap_or_else(|_| serde_json::json!({}));
    let specific = root
        .get("module_specific")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    serde_json::from_value(specific).unwrap_or_default()
}

// ── the sound player (serial) ─────────────────────────────────────────────

/// A request to play a sound. `from` is the viewer's handle for chat
/// replies/logging.
struct PlayRequest {
    name: String,
    path: PathBuf,
    volume: f32,
    from: String,
}

/// Runs the serial playback loop: one sound at a time, next starts when the
/// current file has finished (plus an optional fixed cooldown).
fn spawn_player(rx: mpsc::Receiver<PlayRequest>, cooldown_secs: u64) {
    // rodio's OutputStream/Sink are not Send, so the player owns them on a
    // dedicated thread and blocks on the channel — serial by construction.
    std::thread::spawn(move || {
        let (_stream, handle) = match OutputStream::try_default() {
            Ok(pair) => pair,
            Err(e) => {
                error!("no audio output available: {e}");
                return;
            }
        };
        while let Ok(req) = rx.recv() {
            match play_one(&handle, &req.path, req.volume, &req.name, &req.from) {
                Ok(()) => info!("played '{}' for {}", req.name, req.from),
                Err(e) => error!("failed to play '{}': {e}", req.name),
            }
            if cooldown_secs > 0 {
                std::thread::sleep(Duration::from_secs(cooldown_secs));
            }
        }
    });
}

/// Play one file at the given volume, NORMALISED so every clip is the same
/// loudness. The whole file is decoded first, the peak sample found, and each
/// sample scaled by `1/peak * volume` — a quiet recording and a loud one then
/// both play at `volume`, rather than one blasting over the other.
fn play_one(
    handle: &OutputStreamHandle,
    path: &Path,
    volume: f32,
    name: &str,
    from: &str,
) -> Result<(), String> {
    if !path.exists() {
        return Err(format!("file not found: {}", path.display()));
    }
    let file = std::fs::File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let source = Decoder::new(BufReader::new(file))
        .map_err(|e| format!("decode {}: {e}", path.display()))?;

    // Decode fully, find the peak, and re-emit as a normalised buffer.
    use rodio::source::Source;
    let channels = source.channels();
    let sample_rate = source.sample_rate();
    let samples: Vec<f32> = source.convert_samples().collect();
    let peak = samples.iter().fold(0.0f32, |acc, s| acc.max(s.abs()));
    let peak = peak.max(1e-9); // avoid divide-by-zero on a silent file
    let volume = volume.clamp(0.0, 1.0);
    let gain = (1.0 / peak) * volume;
    let normalised: Vec<f32> = samples.into_iter().map(|s| s * gain).collect();
    let buf = rodio::buffer::SamplesBuffer::new(channels, sample_rate, normalised);

    let sink = Sink::try_new(handle).map_err(|e| format!("sink: {e}"))?;
    sink.set_volume(1.0); // normalisation already applied the gain
    sink.append(buf);
    // Block this task until the file has fully played.
    sink.sleep_until_end();
    let _ = (name, from);
    Ok(())
}

// ── connection / message loop ─────────────────────────────────────────────

#[derive(Clone)]
struct Session {
    auth_token: String,
    module_name: String,
    instance_uuid7: String,
}

async fn send_container(write: &Arc<AsyncMutex<WsWriteHalf>>, container: Container) {
    let mut buf = Vec::new();
    if container.encode(&mut buf).is_ok() {
        let mut w = write.lock().await;
        let _ = w.send(WsMessage::Binary(buf)).await;
    }
}

/// Post a chat message to the platform the command came from.
async fn send_to_chat(
    write: &Arc<AsyncMutex<WsWriteHalf>>,
    s: &Session,
    msg: String,
    chat: &ChatMessage,
) {
    let Some(ud) = &chat.user_data else { return };
    let container = Container {
        version: 1,
        auth_token: s.auth_token.clone(),
        module_name: s.module_name.clone(),
        module_instance_uuid7: s.instance_uuid7.clone(),
        payload: Some(Payload::SendToPlatforms(SendToPlatforms {
            msg,
            level: 0,
            module_uuid7: s.instance_uuid7.clone(),
            pid: String::new(),
            platform: chat.platform.clone(),
            actor_platform: chat.platform.clone(),
            actor_handle: ud.username.clone(),
            actor_uuid7: chat.user_uuid7.clone(),
            channel_id: chat.channel_id.clone(),
        })),
    };
    send_container(write, container).await;
}

/// Parse `!snd <name>` from the raw message: everything after the command
/// token, trimmed. e.g. `!snd airhorn` -> `airhorn`.
fn extract_sound_name(raw: &str, flag: &str) -> Option<String> {
    let trimmed = raw.trim();
    let lower = trimmed.to_ascii_lowercase();
    let token = format!("{}{}", flag.to_ascii_lowercase(), COMMAND_NAME);
    if !lower.starts_with(&token) {
        return None;
    }
    let rest = trimmed[token.len()..].trim();
    if rest.is_empty() {
        return None;
    }
    // Require a separator after the command token so `!sndfoo` isn't a command.
    if !trimmed[token.len()..].starts_with(char::is_whitespace) {
        return None;
    }
    Some(rest.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .map_err(|e| format!("tracing init: {e}"))?;

    let config = load_config(Path::new("config.json"));

    let (play_tx, play_rx) = mpsc::channel::<PlayRequest>();
    spawn_player(play_rx, config.cooldown_secs);

    // Shared scan map: name -> path, refreshed periodically so a newly dropped
    // file appears without a restart.
    let sounds: Arc<std::sync::Mutex<HashMap<String, PathBuf>>> =
        Arc::new(std::sync::Mutex::new(scan_sounds_dir(&config.sounds_dir)));
    {
        let sounds = Arc::clone(&sounds);
        let dir = config.sounds_dir.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                *sounds.lock().unwrap() = scan_sounds_dir(&dir);
            }
        });
    }

    let cfg = config.clone();
    tokio::spawn(async move {
        session_loop(&cfg, play_tx, sounds).await;
    });

    loop {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}

async fn session_loop(
    config: &Config,
    play_tx: mpsc::Sender<PlayRequest>,
    sounds: Arc<std::sync::Mutex<HashMap<String, PathBuf>>>,
) {
    let mut backoff = config.reconnect_base_secs;
    loop {
        match run_session(config, play_tx.clone(), sounds.clone()).await {
            Ok(()) => {}
            Err(e) => error!("session error: {e}"),
        }
        warn!("engine disconnected — reconnecting in {backoff}s");
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(config.reconnect_max_secs.max(1));
    }
}

async fn run_session(
    config: &Config,
    play_tx: mpsc::Sender<PlayRequest>,
    sounds: Arc<std::sync::Mutex<HashMap<String, PathBuf>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = CockatielClient::connect("config.json").await?;
    let (write, read) = client.stream.split();
    let write_shared: Arc<AsyncMutex<WsWriteHalf>> = Arc::new(AsyncMutex::new(write));
    let session = Session {
        auth_token: client.auth_token.clone(),
        module_name: client.config.module_name.clone(),
        instance_uuid7: client.instance_uuid7.clone(),
    };

    // Register the !snd command so the engine routes `!snd <name>` to us.
    let commands = Container {
        version: 1,
        auth_token: session.auth_token.clone(),
        module_name: session.module_name.clone(),
        module_instance_uuid7: session.instance_uuid7.clone(),
        payload: Some(Payload::CommandsPayload(Commands {
            commands: vec![Command {
                command_name: COMMAND_NAME.to_string(),
                command_flag: config.command_flag.clone(),
                command_description: "spend points to play a whitelisted sound on the streamer's machine (e.g. !snd airhorn, !snd spongebob)".to_string(),
                command_flags: vec![],
            }],
            alert_on_unknown_command: false,
        })),
    };
    send_container(&write_shared, commands).await;

    let flag = config.command_flag.clone();

    let s = session.clone();
    let mut read = read;
    loop {
        let Some(msg) = read.next().await else { break };
        let data = match msg {
            Ok(WsMessage::Binary(d)) => d,
            Ok(WsMessage::Close(_)) => break,
            Ok(_) => continue,
            Err(e) => {
                warn!("ws error: {e}");
                break;
            }
        };
        let Ok(container) = Container::decode(data.as_ref()) else {
            continue;
        };

        match container.payload {
            Some(Payload::AuthVerify(_)) => {
                let reply = Container {
                    version: 1,
                    auth_token: s.auth_token.clone(),
                    module_name: s.module_name.clone(),
                    module_instance_uuid7: s.instance_uuid7.clone(),
                    payload: Some(Payload::AuthVerify(AuthVerify {
                        cur_auth: s.auth_token.clone(),
                    })),
                };
                send_container(&write_shared, reply).await;
            }
            Some(Payload::MessagePreProcess(pre)) => {
                let MessagePreProcess { message_uuid7: uuid, raw_message, audio, audio_type } = pre;
                // ACK the stage on EVERY path so the pipeline never stalls.
                let ack = Container {
                    version: 1,
                    auth_token: s.auth_token.clone(),
                    module_name: s.module_name.clone(),
                    module_instance_uuid7: s.instance_uuid7.clone(),
                    payload: Some(Payload::MessagePreProcess(MessagePreProcess {
                        message_uuid7: uuid,
                        raw_message: raw_message.clone(),
                        audio,
                        audio_type,
                    })),
                };
                send_container(&write_shared, ack).await;

                let Some(chat) = &raw_message else { continue };
                let Some(cmd) = &chat.command else { continue };
                if cmd.command_name != COMMAND_NAME {
                    continue;
                }
                let Some(name) = extract_sound_name(&chat.raw_message, &flag) else { continue };

                // The ENGINE already gates this module: it skips the module
                // entirely when the viewer can't afford the manifest `price`
                // (and enforces `authority`/`min_rank`), so a message reaching
                // us means the viewer was already charged. This module must not
                // deduct a second time — it just plays the scanned sound.

                // Look up the name in the current scan of the sounds folder;
                // refuse anything not present.
                let Some(path) = sounds.lock().unwrap().get(&name).cloned() else {
                    send_to_chat(
                        &write_shared,
                        &s,
                        format!("unknown sound '{name}' — the streamer hasn't put that in the sounds folder"),
                        chat,
                    )
                    .await;
                    continue;
                };

                let handle = chat
                    .user_data
                    .as_ref()
                    .map(|u| u.username.clone())
                    .unwrap_or_default();
                match play_tx.send(PlayRequest {
                    name: name.clone(),
                    path,
                    volume: config.volume,
                    from: handle.clone(),
                }) {
                    Ok(()) => {
                        info!("queued sound '{name}' for {handle}");
                        send_to_chat(
                            &write_shared,
                            &s,
                            format!("{handle} played {name}!"),
                            chat,
                        )
                        .await;
                    }
                    Err(_) => {
                        error!("player queue closed — dropping sound '{name}'");
                        send_to_chat(
                            &write_shared,
                            &s,
                            format!("{handle}: the sound player isn't running"),
                            chat,
                        )
                        .await;
                    }
                }
            }
            Some(Payload::MessageInProcess(process)) => {
                let ack = Container {
                    version: 1,
                    auth_token: s.auth_token.clone(),
                    module_name: s.module_name.clone(),
                    module_instance_uuid7: s.instance_uuid7.clone(),
                    payload: Some(Payload::MessageInProcess(MessageInProcess {
                        message_uuid7: process.message_uuid7,
                        raw_message: process.raw_message,
                        processed_message: process.processed_message,
                        abandon_message: process.abandon_message,
                        audio: process.audio,
                        audio_type: process.audio_type,
                    })),
                };
                send_container(&write_shared, ack).await;
            }
            _ => {}
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_sound_name_parses_command() {
        assert_eq!(extract_sound_name("!snd airhorn", "!"), Some("airhorn".into()));
        assert_eq!(extract_sound_name("  !snd   spongebob  ", "!"), Some("spongebob".into()));
        assert_eq!(extract_sound_name("!snd laugh", "!"), Some("laugh".into()));
        assert_eq!(extract_sound_name("!snd", "!"), None, "no name");
        assert_eq!(extract_sound_name("!sndfoo", "!"), None, "no separator");
        assert_eq!(extract_sound_name("hello world", "!"), None, "not a command");
    }

    #[test]
    fn scan_picks_up_files_by_stem_and_filters_extensions() {
        let tmp = std::env::temp_dir().join(format!("cockatiel-sb-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&tmp).unwrap();
        for (name, content) in [
            ("airhorn.wav", "x"),
            ("laugh.mp3", "x"),
            ("bell.flac", "x"),
            ("coin.ogg", "x"),
            ("note.txt", "x"),   // unsupported extension — ignored
            (".hidden.wav", "x"), // hidden file — ignored
        ] {
            std::fs::write(tmp.join(name), content).unwrap();
        }
        let map = scan_sounds_dir(tmp.to_str().unwrap());
        assert!(map.contains_key("airhorn"), "stem of airhorn.wav");
        assert!(map.contains_key("laugh"));
        assert!(map.contains_key("bell"));
        assert!(map.contains_key("coin"));
        assert!(!map.contains_key("note"), "txt is not a supported audio extension");
        assert!(!map.contains_key(".hidden"), "hidden files are skipped");
        assert_eq!(map.len(), 4);
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn scan_returns_empty_for_a_missing_folder() {
        let map = scan_sounds_dir("/nonexistent/cockatiel/sounds");
        assert!(map.is_empty());
    }

    #[test]
    fn sounds_parse_from_config() {
        let cfg: Config = serde_json::from_value(serde_json::json!({
            "sounds_dir": "my-sounds",
            "volume": 0.6,
            "cooldown_secs": 2
        })).unwrap_or_default();
        assert_eq!(cfg.sounds_dir, "my-sounds");
        assert_eq!(cfg.volume, 0.6);
        assert_eq!(cfg.cooldown_secs, 2);
        // Defaults when absent.
        let def = Config::default();
        assert_eq!(def.sounds_dir, "sounds");
        assert_eq!(def.volume, 1.0);
    }

    #[test]
    fn volume_clamps_to_unit_range() {
        let cfg = Config { volume: 3.0, ..Config::default() };
        assert_eq!(cfg.volume.clamp(0.0, 1.0), 1.0);
        let cfg = Config { volume: -1.0, ..Config::default() };
        assert_eq!(cfg.volume.clamp(0.0, 1.0), 0.0);
    }
}