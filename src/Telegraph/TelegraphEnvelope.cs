using System;
using System.Collections.Generic;

namespace Telegraph;

/// <summary>
/// The batteries-included message shape: an identity, a timestamp, an optional <see cref="Pose6Dof"/>,
/// and a generic string attribute map for anything else.
/// </summary>
/// <remarks>
/// Using this type is optional. <see cref="TelegraphPublisher.Publish{T}(T)"/> and
/// <see cref="TelegraphSubscriber.ReadAsync{T}(System.Threading.CancellationToken)"/> are generic
/// over any JSON-serialisable type, so a caller with its own message shape is never forced through
/// this one. It exists so a 6DOF-plus-metadata stream is usable without designing a message type
/// first.
/// </remarks>
public sealed class TelegraphEnvelope
{
    /// <summary>Creates an envelope for an identified entity at a point in time.</summary>
    /// <param name="entityId">Stable identity of whatever this envelope describes.</param>
    /// <param name="timestampUtc">When this envelope was produced, in UTC.</param>
    /// <exception cref="ArgumentNullException"><paramref name="entityId"/> is <c>null</c>.</exception>
    public TelegraphEnvelope(string entityId, DateTimeOffset timestampUtc)
    {
        if (entityId == null)
        {
            throw new ArgumentNullException(nameof(entityId));
        }

        EntityId = entityId;
        TimestampUtc = timestampUtc;
    }

    /// <summary>Stable identity of whatever this envelope describes.</summary>
    public string EntityId { get; set; }

    /// <summary>
    /// An optional disambiguator alongside <see cref="EntityId"/>. Telegraph never inspects it —
    /// it exists because <see cref="EntityId"/> alone is not guaranteed unique across every
    /// producer a subscriber might combine, and a receiver that needs uniqueness can key on the
    /// pair instead of on <see cref="EntityId"/> alone.
    /// </summary>
    public string? GroupTag { get; set; }

    /// <summary>When this envelope was produced, in UTC.</summary>
    public DateTimeOffset TimestampUtc { get; set; }

    /// <summary>The 6DOF payload, if this envelope carries one.</summary>
    public Pose6Dof? Pose { get; set; }

    /// <summary>Free-form string metadata, if this envelope carries any.</summary>
    public IReadOnlyDictionary<string, string>? Attributes { get; set; }
}
