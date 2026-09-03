using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Telegraph.Tests;

/// <summary>
/// Proves the transport actually round-trips over a real loopback TCP connection, and that it is
/// genuinely data-model agnostic rather than only tested against <see cref="TelegraphEnvelope"/>.
/// </summary>
public sealed class RoundTripTests
{
    [Fact]
    public async Task SubscriberReceivesEnvelopesInOrder()
    {
        using var publisher = new TelegraphPublisher(0);
        await publisher.StartAsync();

        using var subscriber = new TelegraphSubscriber("127.0.0.1", publisher.Port);
        await subscriber.ConnectAsync();
        await WaitForSubscriberCountAsync(publisher, 1);

        var sent = new List<TelegraphEnvelope>();
        for (int i = 0; i < 5; i++)
        {
            sent.Add(new TelegraphEnvelope("entity-" + i.ToString(), DateTimeOffset.UtcNow)
            {
                Pose = new Pose6Dof { LatitudeDegrees = i, LongitudeDegrees = -i },
            });
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Task<List<TelegraphEnvelope>> readTask = ReadCountAsync<TelegraphEnvelope>(subscriber, sent.Count, cts.Token);

        foreach (TelegraphEnvelope envelope in sent)
        {
            publisher.Publish(envelope);
        }

        List<TelegraphEnvelope> received = await readTask;

        Assert.Equal(sent.Count, received.Count);
        for (int i = 0; i < sent.Count; i++)
        {
            Assert.Equal(sent[i].EntityId, received[i].EntityId);
            Assert.Equal(sent[i].Pose?.LatitudeDegrees, received[i].Pose?.LatitudeDegrees);
            Assert.Equal(sent[i].Pose?.LongitudeDegrees, received[i].Pose?.LongitudeDegrees);
        }
    }

    [Fact]
    public async Task TransportRoundTripsAnArbitraryTypeNotJustTelegraphEnvelope()
    {
        using var publisher = new TelegraphPublisher(0);
        await publisher.StartAsync();

        using var subscriber = new TelegraphSubscriber("127.0.0.1", publisher.Port);
        await subscriber.ConnectAsync();
        await WaitForSubscriberCountAsync(publisher, 1);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Task<WeatherReading> readTask = ReadOneAsync<WeatherReading>(subscriber, cts.Token);

        publisher.Publish(new WeatherReading { StationId = "kabc", TemperatureCelsius = 21.5 });

        WeatherReading received = await readTask;

        Assert.Equal("kabc", received.StationId);
        Assert.Equal(21.5, received.TemperatureCelsius);
    }

    [Fact]
    public async Task ALateSubscriberDoesNotSeeMessagesPublishedBeforeItConnected()
    {
        using var publisher = new TelegraphPublisher(0);
        await publisher.StartAsync();

        // Published with no subscriber connected yet -- there is no replay buffer, so this must
        // never be the message a later subscriber sees first.
        publisher.Publish(new TelegraphEnvelope("before", DateTimeOffset.UtcNow));

        using var subscriber = new TelegraphSubscriber("127.0.0.1", publisher.Port);
        await subscriber.ConnectAsync();
        await WaitForSubscriberCountAsync(publisher, 1);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Task<TelegraphEnvelope> readTask = ReadOneAsync<TelegraphEnvelope>(subscriber, cts.Token);

        publisher.Publish(new TelegraphEnvelope("after", DateTimeOffset.UtcNow));

        TelegraphEnvelope received = await readTask;

        Assert.Equal("after", received.EntityId);
    }

    [Fact]
    public async Task PublishBroadcastsToEveryConnectedSubscriber()
    {
        using var publisher = new TelegraphPublisher(0);
        await publisher.StartAsync();

        using var first = new TelegraphSubscriber("127.0.0.1", publisher.Port);
        using var second = new TelegraphSubscriber("127.0.0.1", publisher.Port);
        await first.ConnectAsync();
        await second.ConnectAsync();
        await WaitForSubscriberCountAsync(publisher, 2);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Task<TelegraphEnvelope> firstRead = ReadOneAsync<TelegraphEnvelope>(first, cts.Token);
        Task<TelegraphEnvelope> secondRead = ReadOneAsync<TelegraphEnvelope>(second, cts.Token);

        publisher.Publish(new TelegraphEnvelope("broadcast", DateTimeOffset.UtcNow));

        await Task.WhenAll(firstRead, secondRead);

        Assert.Equal("broadcast", firstRead.Result.EntityId);
        Assert.Equal("broadcast", secondRead.Result.EntityId);
    }

    [Fact]
    public async Task PublisherCanBindToLoopbackExplicitly()
    {
        using var publisher = new TelegraphPublisher(System.Net.IPAddress.Loopback, 0);
        await publisher.StartAsync();

        using var subscriber = new TelegraphSubscriber("127.0.0.1", publisher.Port);
        await subscriber.ConnectAsync();
        await WaitForSubscriberCountAsync(publisher, 1);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Task<TelegraphEnvelope> readTask = ReadOneAsync<TelegraphEnvelope>(subscriber, cts.Token);

        publisher.Publish(new TelegraphEnvelope("loopback", DateTimeOffset.UtcNow));

        TelegraphEnvelope received = await readTask;

        Assert.Equal("loopback", received.EntityId);
    }

    [Fact]
    public async Task SubscribersExposesRemoteEndPointAndSentCounters()
    {
        using var publisher = new TelegraphPublisher(0);
        await publisher.StartAsync();

        using var subscriber = new TelegraphSubscriber("127.0.0.1", publisher.Port);
        await subscriber.ConnectAsync();
        await WaitForSubscriberCountAsync(publisher, 1);

        System.Collections.Generic.IReadOnlyList<TelegraphSubscriberInfo> before = publisher.Subscribers;
        Assert.Single(before);
        Assert.Equal("127.0.0.1", before[0].RemoteEndPoint.Address.ToString());
        Assert.Equal(0, before[0].MessagesSent);
        Assert.Equal(0, before[0].BytesSent);
        Assert.True(before[0].ConnectedAt <= DateTimeOffset.UtcNow);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Task<TelegraphEnvelope> readTask = ReadOneAsync<TelegraphEnvelope>(subscriber, cts.Token);
        publisher.Publish(new TelegraphEnvelope("counted", DateTimeOffset.UtcNow));
        await readTask;

        TelegraphSubscriberInfo after = Assert.Single(publisher.Subscribers);
        Assert.Same(before[0], after);
        Assert.Equal(1, after.MessagesSent);
        Assert.True(after.BytesSent > 0);
    }

    [Fact]
    public async Task DisconnectDropsOnlyTheGivenSubscriber()
    {
        using var publisher = new TelegraphPublisher(0);
        await publisher.StartAsync();

        using var first = new TelegraphSubscriber("127.0.0.1", publisher.Port);
        using var second = new TelegraphSubscriber("127.0.0.1", publisher.Port);
        await first.ConnectAsync();
        await second.ConnectAsync();
        await WaitForSubscriberCountAsync(publisher, 2);

        TelegraphSubscriberInfo target = publisher.Subscribers[0];

        Assert.True(publisher.Disconnect(target));
        Assert.False(publisher.Disconnect(target));

        DateTime deadline = DateTime.UtcNow.AddSeconds(10);
        while (publisher.SubscriberCount != 1 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10);
        }

        Assert.Equal(1, publisher.SubscriberCount);
        Assert.DoesNotContain(target, publisher.Subscribers);
    }

    [Fact]
    public async Task AcceptLoopSurvivesAClientThatDisconnectsBeforeItCanBeInspected()
    {
        using var publisher = new TelegraphPublisher(0);
        await publisher.StartAsync();

        // A client that connects and drops immediately, the way a port scanner, health check, or
        // load-balancer probe would. Before the fix this could throw while the accept loop
        // inspected the socket's RemoteEndPoint, permanently killing the loop.
        using (var probe = new TcpClient())
        {
            await probe.ConnectAsync(IPAddress.Loopback, publisher.Port);
        }

        using var subscriber = new TelegraphSubscriber("127.0.0.1", publisher.Port);
        await subscriber.ConnectAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Task<TelegraphEnvelope> readTask = ReadOneAsync<TelegraphEnvelope>(subscriber, cts.Token);

        // Whether the probe's RemoteEndPoint access actually threw is a timing-dependent race, so
        // rather than waiting on a specific SubscriberCount (which the probe itself may or may not
        // still occupy), republish until the real subscriber -- once the accept loop has gotten to
        // it -- actually receives its message. If the loop died on the probe, this never resolves
        // and the test times out via the token above instead of hanging forever.
        var target = new TelegraphEnvelope("after-probe", DateTimeOffset.UtcNow);
        while (!readTask.IsCompleted && !cts.IsCancellationRequested)
        {
            publisher.Publish(target);
            await Task.Delay(20);
        }

        TelegraphEnvelope received = await readTask;
        Assert.Equal("after-probe", received.EntityId);
    }

    private static async Task<T> ReadOneAsync<T>(TelegraphSubscriber subscriber, CancellationToken cancellationToken)
    {
        await foreach (T message in subscriber.ReadAsync<T>(cancellationToken))
        {
            return message;
        }

        throw new TimeoutException("No message received before the read loop ended.");
    }

    private static async Task<List<T>> ReadCountAsync<T>(TelegraphSubscriber subscriber, int count, CancellationToken cancellationToken)
    {
        var received = new List<T>();
        await foreach (T message in subscriber.ReadAsync<T>(cancellationToken))
        {
            received.Add(message);
            if (received.Count == count)
            {
                break;
            }
        }

        return received;
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

    private sealed class WeatherReading
    {
        public string StationId { get; set; } = string.Empty;

        public double TemperatureCelsius { get; set; }
    }
}
