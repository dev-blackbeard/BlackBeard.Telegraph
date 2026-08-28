# CLAUDE.md — Telegraph (PUBLIC repository)

Telegraph is a generic newline-delimited-JSON-over-TCP pub/sub transport for .NET. It has no
opinion about what you send over it.

This repository is public and stands entirely on its own.

---

## 0. What this repository must never become

Telegraph does not know about Argus, or about any other consumer. Not a reference, not a comment,
not a naming convention that assumes one particular use. It is generic infrastructure; the moment
it starts encoding assumptions specific to one consumer, it stops being reusable by any other one,
which is the entire reason it exists as its own repository instead of a module inside a consumer.

`Pose6Dof` and `TelegraphEnvelope` are the one deliberate exception, and even they are opt-in:
`TelegraphPublisher.Publish<T>`/`TelegraphSubscriber.ReadAsync<T>` work with any JSON-serialisable
type, so a consumer with its own message shape is never forced through these two.

## 1. Decisions recorded once, so they are never silently redefined

`Pose6Dof`'s coordinate frame, units, and rotation representation (see the type's own XML doc
comments and `README.md`) were chosen deliberately:

- Position: WGS84 geodetic, latitude/longitude in degrees, altitude in metres.
- Attitude: unit quaternion (X/Y/Z/W) primary, roll/pitch/yaw in degrees alongside it.
- Linear velocity: local North/East/Down, metres per second.
- Angular velocity: body-frame X/Y/Z, degrees per second.
- Every field nullable; unsupplied is `null`, never `0`.

Changing any of these is a breaking change to every consumer's mapping code, not a tweak — treat
it accordingly.

## 2. Conventions

- `net8.0` only, deliberately not multi-targeted to `netstandard2.0`: every consumer this package
  is designed for is already on `net8.0` or a `net8.0-<platform>` TFM, and staying off
  `netstandard2.0` keeps `System.Text.Json` and `IAsyncEnumerable<T>` in-box with no extra package
  references or polyfills.
- Wire format is newline-delimited UTF-8 JSON, one message per line. No framing beyond the
  newline. Keep it that way unless there's a concrete reason a consumer cannot work with it —
  the inspectability (`nc host port`) is a feature, not an oversight.
- No topics, no replay buffer, no reconnect logic. See `README.md`'s "What this package
  deliberately does not do" — these are scope decisions, not gaps to fill reflexively.
- Deterministic builds, SourceLink, symbol packages, central package management — same as any
  packable .NET library.

## Backlog

- [ ] None currently. Add here rather than expanding scope inline in a PR description.
