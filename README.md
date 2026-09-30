# Cockatiel Soundboard

Viewers spend points to play a sound effect on the streamer's machine — the
audio twin of the `fake-input` module.

Command: `!snd <name>` (e.g. `!snd airhorn`, `!snd laugh`).

## How sounds are discovered

Drop audio files into the **sounds folder** and the module picks them all up
automatically (re-scanned every 5s, so a newly added file appears without a
restart). The file's stem is the command name — `airhorn.wav` is triggered with
`!snd airhorn`. Remove a file and it stops working. Nothing else needs to be
configured per sound.

## Configuration (`config.json`)

Under `module_specific`:

```json
{
  "command_flag": "!",
  "price": 10000,
  "sounds_dir": "sounds",
  "volume": 0.3,
  "cooldown_secs": 0
}
```

- **`sounds_dir`** — the folder to scan for audio files (relative to the
  module's working directory, or absolute).
- **`volume`** — the single playback volume `[0,1]` (default `0.3` — 30%, quiet
  first; the streamer can raise it). It is applied AFTER normalisation, so a
  quiet recording and a loud one both play at the same loudness.
- **`price`** — points a viewer spends per sound (deducted from their current
  score by the engine; the lifetime total is untouched).
- **`cooldown_secs`** — optional fixed pause (seconds) between plays. Default
  `0` = the next sound starts the moment the previous one finishes.

## Normalisation

Every clip is decoded and its integrated loudness is measured with a
**K-weighted ITU-R BS.1770 meter (LUFS)**. Each sample is then scaled toward a
target of **-14 LUFS** (the common streaming loudness target), clamped to ±24 dB
so a near-silent clip can't be boosted into distortion, and finally the
configured `volume` is applied. The result: all sounds play at the same
loudness regardless of how loud or quiet the source file was recorded, and the
30% default keeps the first play conservative.

## Playback rule

Sounds play **serially**: only one at a time. Each viewer's next sound waits
until the current file has finished, then plays immediately.

## Gates

The engine enforces `price`, `authority`, and `min_rank` from
`cockatiel_module_info.json` before a message reaches the module, so the module
never deducts points itself. Ship defaults: `authority: 0` (anyone), `min_rank: 0`
(no rank gate), `price: 10000`.

## Supported formats

wav / mp3 / flac / ogg / m4a / aac (via rodio + symphonia). No external player
required.