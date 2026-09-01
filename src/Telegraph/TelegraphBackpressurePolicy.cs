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
    /// A subscriber that is not immediately ready to receive is skipped for this message rather
    /// than blocking the call to <see cref="TelegraphPublisher.Publish{T}(T)"/>; it stays
    /// connected and may receive later messages normally. Use this when one slow subscriber must
    /// never add latency to delivery for the others, and missing an occasional message is
    /// acceptable for that subscriber.
    /// </summary>
    DropForSlowSubscriber,

    /// <summary>
    /// A write is allowed to block for up to <see cref="TelegraphPublisher.BackpressureTimeout"/>
    /// before the subscriber is disconnected, the same as any other write failure.
    /// </summary>
    DisconnectAfterTimeout,
}
