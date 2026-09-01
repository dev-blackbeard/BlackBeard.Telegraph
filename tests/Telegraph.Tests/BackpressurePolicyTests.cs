using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Telegraph.Tests;

/// <summary>
/// Proves each <see cref="TelegraphBackpressurePolicy"/> behaves as documented, and that the
/// default policy is unchanged from <see cref="TelegraphPublisher"/>'s pre-existing behaviour.
/// </summary>
public sealed class BackpressurePolicyTests
{
    [Fact]
    public void DefaultPolicyIsBlockUntilDrained()
    {
        using var publisher = new TelegraphPublisher(0);

        Assert.Equal(TelegraphBackpressurePolicy.BlockUntilDrained, publisher.BackpressurePolicy);
    }

    [Fact]
    public async Task DropForSlowSubscriberSkipsAMessageForASlowSubscriberWithoutBlockingOthers()
    {
        using var publisher = new TelegraphPublisher(0)
        {
            BackpressurePolicy = TelegraphBackpressurePolicy.DropForSlowSubscriber,
        };
        await publisher.StartAsync();

        // A raw TcpClient with a tiny receive window, connected but never read from, so its
        // buffer fills quickly and a further write to it would block.
        using var slow = new TcpClient { ReceiveBufferSize = 1024 };
        await slow.ConnectAsync(IPAddress.Loopback, publisher.Port);
        await WaitForSubscriberCountAsync(publisher, 1);

        string filler = new string('x', 8192);
        for (int i = 0; i < 50; i++)
        {
            publisher.Publish(new { Filler = filler });
        }

        using var fast = new TelegraphSubscriber("127.0.0.1", publisher.Port);
        await fast.ConnectAsync();
        await WaitForSubscriberCountAsync(publisher, 2);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Task<TelegraphEnvelope> fastRead = ReadOneAsync<TelegraphEnvelope>(fast, cts.Token);

        var stopwatch = Stopwatch.StartNew();
        publisher.Publish(new TelegraphEnvelope("after-slow", DateTimeOffset.UtcNow));
        stopwatch.Stop();

        TelegraphEnvelope received = await fastRead;
        Assert.Equal("after-slow", received.EntityId);
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(5), "Publish should not block on a slow subscriber under DropForSlowSubscriber.");

        // Skipped, not dropped -- the slow subscriber is still connected.
        Assert.Equal(2, publisher.SubscriberCount);
    }

    [Fact]
    public async Task DisconnectAfterTimeoutDropsASubscriberThatDoesNotDrainInTime()
    {
        using var publisher = new TelegraphPublisher(0)
        {
            BackpressurePolicy = TelegraphBackpressurePolicy.DisconnectAfterTimeout,
            BackpressureTimeout = TimeSpan.FromMilliseconds(200),
        };
        await publisher.StartAsync();

        using var slow = new TcpClient { ReceiveBufferSize = 1024 };
        await slow.ConnectAsync(IPAddress.Loopback, publisher.Port);
        await WaitForSubscriberCountAsync(publisher, 1);

        string filler = new string('x', 8192);
        DateTime deadline = DateTime.UtcNow.AddSeconds(10);
        while (publisher.SubscriberCount > 0 && DateTime.UtcNow < deadline)
        {
            publisher.Publish(new { Filler = filler });
        }

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
