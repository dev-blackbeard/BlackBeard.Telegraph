using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Telegraph.Tests;

/// <summary>
/// Proves the UDP transport round-trips a real loopback datagram, broadcasts to every registered
/// subscriber, and fails fast rather than silently fragment an oversized message.
/// </summary>
public sealed class UdpTransportTests
{
    [Fact]
    public async Task SubscriberReceivesPublishedEnvelopes()
    {
        using var publisher = new TelegraphUdpPublisher(0);
        await publisher.StartAsync();

        using var subscriber = new TelegraphUdpSubscriber("127.0.0.1", publisher.Port);
        await subscriber.ConnectAsync();
        await WaitForSubscriberCountAsync(publisher, 1);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Task<TelegraphEnvelope> readTask = ReadOneAsync<TelegraphEnvelope>(subscriber, cts.Token);

        publisher.Publish(new TelegraphEnvelope("udp-entity", DateTimeOffset.UtcNow)
        {
            Pose = new Pose6Dof { LatitudeDegrees = 12.5, LongitudeDegrees = -8.25 },
        });

        TelegraphEnvelope received = await readTask;

        Assert.Equal("udp-entity", received.EntityId);
        Assert.Equal(12.5, received.Pose?.LatitudeDegrees);
        Assert.Equal(-8.25, received.Pose?.LongitudeDegrees);
    }

    [Fact]
    public async Task TransportRoundTripsAnArbitraryTypeNotJustTelegraphEnvelope()
    {
        using var publisher = new TelegraphUdpPublisher(0);
        await publisher.StartAsync();

        using var subscriber = new TelegraphUdpSubscriber("127.0.0.1", publisher.Port);
        await subscriber.ConnectAsync();
        await WaitForSubscriberCountAsync(publisher, 1);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Task<WeatherReading> readTask = ReadOneAsync<WeatherReading>(subscriber, cts.Token);

        publisher.Publish(new WeatherReading { StationId = "kudp", TemperatureCelsius = 19.5 });

        WeatherReading received = await readTask;

        Assert.Equal("kudp", received.StationId);
        Assert.Equal(19.5, received.TemperatureCelsius);
    }

    [Fact]
    public async Task PublishBroadcastsToEveryRegisteredSubscriber()
    {
        using var publisher = new TelegraphUdpPublisher(0);
        await publisher.StartAsync();

        using var first = new TelegraphUdpSubscriber("127.0.0.1", publisher.Port);
        using var second = new TelegraphUdpSubscriber("127.0.0.1", publisher.Port);
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
    public void PublishThrowsForAMessageLargerThanASinglePacket()
    {
        using var publisher = new TelegraphUdpPublisher(0);

        var oversized = new TelegraphEnvelope("oversized", DateTimeOffset.UtcNow)
        {
            Attributes = new Dictionary<string, string>
            {
                ["filler"] = new string('x', TelegraphUdpPublisher.MaxDatagramSize),
            },
        };

        ArgumentException exception = Assert.Throws<ArgumentException>(() => publisher.Publish(oversized));
        Assert.Contains(TelegraphUdpPublisher.MaxDatagramSize.ToString(), exception.Message);
    }

    private static async Task<T> ReadOneAsync<T>(TelegraphUdpSubscriber subscriber, CancellationToken cancellationToken)
    {
        await foreach (T message in subscriber.ReadAsync<T>(cancellationToken))
        {
            return message;
        }

        throw new TimeoutException("No message received before the read loop ended.");
    }

    private static async Task WaitForSubscriberCountAsync(TelegraphUdpPublisher publisher, int count)
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
