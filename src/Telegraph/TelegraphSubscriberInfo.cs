using System;
using System.Net;
using System.Threading;

namespace Telegraph;

/// <summary>
/// Point-in-time visibility into one subscriber connected to a <see cref="TelegraphPublisher"/>:
/// where it connected from, when, and how much has been sent to it.
/// </summary>
/// <remarks>
/// Instances are handed out by <see cref="TelegraphPublisher.Subscribers"/> and identify a
/// subscriber for <see cref="TelegraphPublisher.Disconnect(TelegraphSubscriberInfo)"/>; a
/// subscriber that disconnects (on its own or via <c>Disconnect</c>) keeps its own instance
/// unchanged rather than reusing it for a later connection.
/// </remarks>
public sealed class TelegraphSubscriberInfo
{
    private long _bytesSent;
    private long _messagesSent;

    internal TelegraphSubscriberInfo(IPEndPoint remoteEndPoint, DateTimeOffset connectedAt)
    {
        RemoteEndPoint = remoteEndPoint;
        ConnectedAt = connectedAt;
    }

    /// <summary>The subscriber's remote address and port.</summary>
    public IPEndPoint RemoteEndPoint { get; }

    /// <summary>When this subscriber connected.</summary>
    public DateTimeOffset ConnectedAt { get; }

    /// <summary>Total bytes written to this subscriber so far, across every <see cref="TelegraphPublisher.Publish{T}(T)"/> call it received.</summary>
    public long BytesSent
    {
        get { return Interlocked.Read(ref _bytesSent); }
    }

    /// <summary>Total messages written to this subscriber so far.</summary>
    public long MessagesSent
    {
        get { return Interlocked.Read(ref _messagesSent); }
    }

    internal void RecordSent(int byteCount)
    {
        Interlocked.Add(ref _bytesSent, byteCount);
        Interlocked.Increment(ref _messagesSent);
    }
}
