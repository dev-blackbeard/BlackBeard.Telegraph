# Telegraph

A small, generic newline-delimited-JSON-over-TCP pub/sub transport for .NET. It has no opinion
about what you send over it.

```bash
dotnet add package BlackBeard.Telegraph
```

## Why

Sometimes you want to move some data from one process to another — a console host publishing a
live feed, a UI app subscribing to it — without designing a wire protocol, writing a client SDK,
or standing up a broker. Telegraph is that: one TCP port, one JSON object per line, any shape.

## Quick start

```csharp
// Publisher (e.g. a console host generating or replaying data)
using var publisher = new TelegraphPublisher(5000);
await publisher.StartAsync();

publisher.Publish(new { Message = "hello" });

// Subscriber (e.g. a UI app)
using var subscriber = new TelegraphSubscriber("localhost", 5000);
await subscriber.ConnectAsync();

await foreach (var item in subscriber.ReadAsync<MyMessageType>())
{
    // ...
}
```

`Publish<T>`/`ReadAsync<T>` are generic over any type `System.Text.Json` can (de)serialise.
`TelegraphEnvelope`/`Pose6Dof` (below) are one ready-made shape for 6DOF-plus-metadata streams,
not a requirement.

## Wire format

Newline-delimited UTF-8 JSON: one message per line, on a plain TCP stream. No framing beyond the
newline, no handshake, no compression. Inspectable with `nc localhost 5000`. A late-connecting
subscriber only sees messages published after it connects — there is no replay buffer.

## `Pose6Dof` and `TelegraphEnvelope`

An opt-in message shape for 6DOF-plus-metadata streams, so that use case doesn't require designing
a type first. Using it is optional — see Quick start above for sending anything else.

The coordinate frame, units, and rotation representation are fixed and documented on the type
itself (`Pose6Dof.cs`), not left to be rediscovered per consumer:

- Position: WGS84 geodetic — latitude/longitude in degrees, altitude in metres above the
  reference ellipsoid.
- Attitude: a unit quaternion (X/Y/Z/W) as the primary representation, with roll/pitch/yaw in
  degrees carried alongside as a convenience.
- Linear velocity: local North/East/Down, in metres per second.
- Angular velocity: body-frame X/Y/Z, in degrees per second.

Every field is nullable; an unsupplied field is `null`, never `0` — so a downstream mapping to a
richer domain type is a field-for-field copy rather than a guess about whether a zero was measured
or missing.

`TelegraphEnvelope` wraps a `Pose6Dof?` with an `EntityId`, an optional `GroupTag` (Telegraph never
inspects it — it exists because `EntityId` alone is not guaranteed unique across producers a
subscriber might combine), a `TimestampUtc`, and a generic `IReadOnlyDictionary<string, string>?`
for anything else.

## What this package deliberately does not do

- No topics or multiplexing. One connection is one implicit channel. (A plausible v2, not built.)
- No message replay/buffering for late subscribers.
- No retry/reconnect logic — that's a caller concern, since what "retry" should mean depends on
  the caller's own semantics (resume from where? tolerate a gap? reset state?).
- No knowledge of any particular domain. If you're looking for geospatial stream diagnostics, that
  is a different package entirely; Telegraph doesn't know it exists.
