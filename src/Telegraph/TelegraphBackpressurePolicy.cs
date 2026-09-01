namespace Telegraph;

/// <summary>
/// How <see cref="TelegraphPublisher.Publish{T}(T)"/> treats a subscriber whose write would
/// otherwise block, e.g. because its socket's send buffer is full.
/// </summary>
public enum TelegraphBackpressurePolicy
{
    /// <summary>
    /// The write blocks until the subscriber's buffer drains or the write fails. The subscriber
    /// is dropped only if the write throws. This is the default, and matches the behaviour of
    /// every <see cref="TelegraphPublisher"/> before this policy existed.
    /// </summary>
    BlockUntilDrained,

    /// <summary>
    /// Before writing, a quick, non-blocking check rules out a subscriber whose socket cannot
    /// accept any bytes at all right now; a subscriber that fails the check is skipped for this
    /// message rather than blocking the call to <see cref="TelegraphPublisher.Publish{T}(T)"/>,
    /// and stays connected to receive later messages normally. The check only proves the buffer
    /// isn't <em>completely</em> full, not that the whole message will fit -- writes are never
    /// split across messages (splitting one would corrupt the newline-delimited wire format for a
    /// subscriber that is meant to stay connected), so a subscriber with a nearly-full buffer can
    /// still pass the check and then block this call for however long that one write takes. Use
    /// this when missing an occasional message is acceptable for a slow subscriber and most of
    /// the time it should not add latency for the others; use
    /// <see cref="DisconnectAfterTimeout"/> instead if a hard bound on blocking time matters more
    /// than keeping that subscriber connected.
    /// </summary>
    DropForSlowSubscriber,

    /// <summary>
    /// A write is allowed to block for up to <see cref="TelegraphPublisher.BackpressureTimeout"/>
    /// before the subscriber is disconnected, the same as any other write failure.
    /// </summary>
    DisconnectAfterTimeout,
}
