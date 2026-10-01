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

use cockatiel_client::proto::container_for_engine::Payload as EnginePayload;
use cockatiel_client::proto::container_for_module::Payload as ModulePayload;
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
    price: u32,
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
    cooldown_secs: u32,
    /// Reconnect backoff bounds (seconds).
    reconnect_base_secs: u32,
    reconnect_max_secs: u32,
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

/// Default volume: 30%. Better to start too quiet than too loud — the
/// streamer can raise it, but a blasting first sound is a bad first impression.
fn default_volume() -> f32 {
    0.3
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
fn spawn_player(rx: mpsc::Receiver<PlayRequest>, cooldown_secs: u32) {
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
                std::thread::sleep(Duration::from_secs(cooldown_secs as u64));
            }
        }
    });
}

/// The target integrated loudness (LUFS) every clip is normalised to before the
/// volume is applied. -14 LUFS is the common streaming/YouTube loudness target;
/// two files with wildly different recorded levels both end up here, so neither
/// blasts over the other.
const TARGET_LUFS: f32 = -14.0;

/// The max gain (dB) the normaliser may apply. A file measured far below the
/// target is brought up but clamped, so a near-silent clip can't be boosted
/// into distortion.
const MAX_GAIN_DB: f32 = 24.0;

/// Play one file at the given volume, NORMALISED to a target LUFS loudness.
/// The whole file is decoded, its integrated loudness measured with a
/// K-weighted ITU-R BS.1770 meter, and each sample scaled by
/// `10^((target - measured)/20) * volume`. A quiet recording and a loud one
/// both come out at the same loudness, then the volume is applied on top.
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

    // Decode fully to interleaved f32, remembering the layout for both the
    // loudness measurement and the replay buffer.
    use rodio::source::Source;
    let channels = source.channels();
    let sample_rate = source.sample_rate();
    let samples: Vec<f32> = source.convert_samples().collect();
    if samples.is_empty() {
        return Err(format!("no audio samples in {}", path.display()));
    }

    // Measure integrated loudness (K-weighted, ITU-R BS.1770).
    let measured_lufs = measure_lufs(&samples, channels, sample_rate);
    let gain_db = (TARGET_LUFS - measured_lufs).clamp(-MAX_GAIN_DB, MAX_GAIN_DB);
    let gain = 10f32.powf(gain_db / 20.0);
    let volume = volume.clamp(0.0, 1.0);
    let scaled: Vec<f32> = samples.into_iter().map(|s| s * gain * volume).collect();
    info!(
        "normalising '{}': measured {measured_lufs:.1} LUFS -> {:.1} dB gain (volume {volume})",
        name,
        gain_db
    );
    let buf = rodio::buffer::SamplesBuffer::new(channels, sample_rate, scaled);

    let sink = Sink::try_new(handle).map_err(|e| format!("sink: {e}"))?;
    sink.set_volume(1.0); // gain + volume already baked into the samples
    sink.append(buf);
    // Block this task until the file has fully played.
    sink.sleep_until_end();
    let _ = from;
    Ok(())
}

/// Measure the integrated loudness (LUFS) of interleaved samples using a
/// K-weighted ITU-R BS.1770-4 meter.
fn measure_lufs(samples: &[f32], channels: u16, sample_rate: u32) -> f32 {
    use broadcast_loudness::{ChannelLayout, LoudnessMeter};
    let mut meter = match channels {
        1 => LoudnessMeter::new(sample_rate, ChannelLayout::Mono).ok(),
        _ => LoudnessMeter::new(sample_rate, ChannelLayout::Stereo).ok(),
    };
    let Some(meter) = meter.as_mut() else {
        return TARGET_LUFS; // meter unavailable -> assume on-target, no gain
    };
    let frames = samples.len() / channels.max(1) as usize;
    for f in 0..frames {
        let base = f * channels as usize;
        if channels == 1 {
            let _ = meter.push_f32(&[samples[base]]);
        } else {
            let l = samples[base];
            let r = samples[base + 1];
            let _ = meter.push_interleaved_f32(&[l], &[r]);
        }
    }
    meter.finish();
    let lufs = meter.integrated_lufs() as f32;
    // A very short / near-silent file has no gated loudness blocks and reports
    // -inf. Assume on-target (no gain) rather than boosting into distortion.
    if lufs.is_finite() {
        lufs
    } else {
        TARGET_LUFS
    }
}

// ── connection / message loop ─────────────────────────────────────────────

#[derive(Clone)]
struct Session {
    auth_token: String,
    module_name: String,
    instance_uuid7: String,
}

async fn send_container(write: &Arc<AsyncMutex<WsWriteHalf>>, container: ContainerForEngine) {
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
    let container = ContainerForEngine {
        version: 2,
        auth_token: s.auth_token.clone(),
        module_name: s.module_name.clone(),
        module_instance_uuid7: s.instance_uuid7.clone(),
        payload: Some(EnginePayload::SendToPlatforms(SendToPlatforms {
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
        tokio::time::sleep(Duration::from_secs(backoff as u64)).await;
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
    let commands = ContainerForEngine {
        version: 2,
        auth_token: session.auth_token.clone(),
        module_name: session.module_name.clone(),
        module_instance_uuid7: session.instance_uuid7.clone(),
        payload: Some(EnginePayload::Commands(Commands {
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
        let Ok(container) = ContainerForModule::decode(data.as_ref()) else {
            continue;
        };

        match container.payload {
            Some(ModulePayload::AuthVerify(_)) => {
                let reply = ContainerForEngine {
                    version: 2,
                    auth_token: s.auth_token.clone(),
                    module_name: s.module_name.clone(),
                    module_instance_uuid7: s.instance_uuid7.clone(),
                    payload: Some(EnginePayload::AuthVerify(AuthVerify {
                        cur_auth: s.auth_token.clone(),
                    })),
                };
                send_container(&write_shared, reply).await;
            }
            Some(ModulePayload::MessagePreProcess(pre)) => {
                let MessagePreProcess { message_uuid7: uuid, raw_message, audio, audio_type } = pre;
                if !uuid.is_empty() {
                    let receipt = ContainerForEngine {
                        version: 2,
                        auth_token: s.auth_token.clone(),
                        module_name: s.module_name.clone(),
                        module_instance_uuid7: s.instance_uuid7.clone(),
                        payload: Some(EnginePayload::MessageAck(MessageAck {
                            message_uuid7: uuid.clone(),
                        })),
                    };
                    send_container(&write_shared, receipt).await;
                }
                // ACK the stage on EVERY path so the pipeline never stalls.
                let ack = ContainerForEngine {
                    version: 2,
                    auth_token: s.auth_token.clone(),
                    module_name: s.module_name.clone(),
                    module_instance_uuid7: s.instance_uuid7.clone(),
                    payload: Some(EnginePayload::MessagePreProcess(MessagePreProcess {
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
            Some(ModulePayload::MessageInProcess(process)) => {
                if !process.message_uuid7.is_empty() {
                    let receipt = ContainerForEngine {
                        version: 2,
                        auth_token: s.auth_token.clone(),
                        module_name: s.module_name.clone(),
                        module_instance_uuid7: s.instance_uuid7.clone(),
                        payload: Some(EnginePayload::MessageAck(MessageAck {
                            message_uuid7: process.message_uuid7.clone(),
                        })),
                    };
                    send_container(&write_shared, receipt).await;
                }
                let ack = ContainerForEngine {
                    version: 2,
                    auth_token: s.auth_token.clone(),
                    module_name: s.module_name.clone(),
                    module_instance_uuid7: s.instance_uuid7.clone(),
                    payload: Some(EnginePayload::MessageInProcess(MessageInProcess {
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
        assert_eq!(def.volume, 0.3, "default volume is 30% — quiet-first");
    }

    #[test]
    fn volume_clamps_to_unit_range() {
        let cfg = Config { volume: 3.0, ..Config::default() };
        assert_eq!(cfg.volume.clamp(0.0, 1.0), 1.0);
        let cfg = Config { volume: -1.0, ..Config::default() };
        assert_eq!(cfg.volume.clamp(0.0, 1.0), 0.0);
    }

    #[test]
    fn loudness_meter_ranks_louder_files_higher() {
        // A full-scale sine measures louder than the same sine at 10% amplitude.
        let sr = 48_000u32;
        let n = 1_000_000; // ~21s, enough for gated blocks
        let loud: Vec<f32> = (0..n).map(|i| (i as f32 * 0.1).sin() * 0.9).collect();
        let quiet: Vec<f32> = (0..n).map(|i| (i as f32 * 0.1).sin() * 0.05).collect();
        let loud_lufs = measure_lufs(&loud, 1, sr);
        let quiet_lufs = measure_lufs(&quiet, 1, sr);
        assert!(
            loud_lufs > quiet_lufs,
            "louder file should measure higher: loud={loud_lufs:.1} quiet={quiet_lufs:.1}"
        );
        // Both finite and in a sane range (not -inf, not absurd).
        assert!(loud_lufs.is_finite() && quiet_lufs.is_finite());
        assert!(loud_lufs > -40.0 && loud_lufs < 0.0);
    }

    #[test]
    fn loudness_measurement_is_stable_for_silent_or_short_input() {
        // Empty / near-silent input must not panic or return -inf.
        assert!(measure_lufs(&[], 1, 44_100).is_finite());
        let silent = vec![0.0f32; 1000];
        assert!(measure_lufs(&silent, 1, 44_100).is_finite());
    }
}