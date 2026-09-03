using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Telegraph.Tests;

/// <summary>
/// Proves <see cref="TelegraphPublisher.AllowedRanges"/> accepts a connection whose remote
/// address falls within a configured range, rejects one that doesn't (before it ever counts as a
/// subscriber), and that leaving it empty matches the unrestricted default.
/// </summary>
public sealed class CidrAllowListTests
{
    [Fact]
    public async Task ConnectionFromAnAllowedRangeIsAccepted()
    {
        using var publisher = new TelegraphPublisher(0)
        {
            AllowedRanges = { IPNetwork.Parse("127.0.0.0/8") },
        };
        await publisher.StartAsync();

        using var subscriber = new TelegraphSubscriber("127.0.0.1", publisher.Port);
        await subscriber.ConnectAsync();
        await WaitForSubscriberCountAsync(publisher, 1);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Task<TelegraphEnvelope> readTask = ReadOneAsync<TelegraphEnvelope>(subscriber, cts.Token);

        publisher.Publish(new TelegraphEnvelope("allowed", DateTimeOffset.UtcNow));

        TelegraphEnvelope received = await readTask;
        Assert.Equal("allowed", received.EntityId);
    }

    [Fact]
    public async Task ConnectionFromOutsideEveryAllowedRangeIsRejected()
    {
        using var publisher = new TelegraphPublisher(0)
        {
            AllowedRanges = { IPNetwork.Parse("10.0.0.0/8") },
        };
        await publisher.StartAsync();

        using var subscriber = new TelegraphSubscriber("127.0.0.1", publisher.Port);

        // The TCP-level connect still succeeds -- the publisher accepts it before checking the
        // address -- but the connection is closed immediately after, before any message.
        await subscriber.ConnectAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var received = new List<TelegraphEnvelope>();
        await foreach (TelegraphEnvelope envelope in subscriber.ReadAsync<TelegraphEnvelope>(cts.Token))
        {
            received.Add(envelope);
        }

        Assert.Empty(received);
        Assert.Equal(0, publisher.SubscriberCount);
    }

    private static async Task<T> ReadOneAsync<T>(TelegraphSubscriber subscriber, CancellationToken cancellationToken)
    {
        await foreach (T message in subscriber.ReadAsync<T>(cancellationToken))
        {
            return message;
        }

        throw new TimeoutException("No message received before the read loop ended.");
    }

    private static async Task WaitForSubscriberCountAsync(TelegraphPublisher publisher, int count)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(10);
        while (publisher.SubscriberCount < count && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10);
        }

        Assert.Equal(count, publisher.SubscriberCount);
    }
}
