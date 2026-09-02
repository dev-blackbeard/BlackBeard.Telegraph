using System;
using System.Buffers.Binary;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Telegraph.Tests;

/// <summary>
/// Proves <see cref="TelegraphFraming.LengthPrefixed"/> round-trips over real loopback TCP, and
/// that -- unlike the newline-delimited default -- it is not disrupted by a payload containing a
/// raw newline byte.
/// </summary>
public sealed class LengthPrefixedFramingTests
{
    [Fact]
    public async Task SubscriberReceivesMessagesUnderLengthPrefixedFraming()
    {
        using var publisher = new TelegraphPublisher(0, TelegraphFraming.LengthPrefixed);
        await publisher.StartAsync();

        using var subscriber = new TelegraphSubscriber("127.0.0.1", publisher.Port, TelegraphFraming.LengthPrefixed);
        await subscriber.ConnectAsync();
        await WaitForSubscriberCountAsync(publisher, 1);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Task<TelegraphEnvelope> readTask = ReadOneAsync<TelegraphEnvelope>(subscriber, cts.Token);

        publisher.Publish(new TelegraphEnvelope("length-prefixed", DateTimeOffset.UtcNow));

        TelegraphEnvelope received = await readTask;
        Assert.Equal("length-prefixed", received.EntityId);
    }

    [Fact]
    public async Task LengthPrefixedFramingIsNotDisruptedByAnEmbeddedNewlineInThePayload()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;

            using var subscriber = new TelegraphSubscriber("127.0.0.1", port, TelegraphFraming.LengthPrefixed);
            Task connectTask = subscriber.ConnectAsync();

            using TcpClient serverSideClient = await listener.AcceptTcpClientAsync();
            await connectTask;

            using NetworkStream serverStream = serverSideClient.GetStream();

            // A raw, unescaped newline where a JSON string value would be -- the scenario
            // length-prefixed framing exists for. Hand-built rather than produced by Publish,
            // since a correctly-escaping serialiser (System.Text.Json included) never emits this
            // on its own; the point is to prove framing survives it regardless of where it came
            // from.
            byte[] corruptedPayload = Encoding.UTF8.GetBytes("{\"EntityId\":\"line1\nline2\"}");
            await WriteLengthPrefixedFrameAsync(serverStream, corruptedPayload);

            byte[] goodPayload = JsonSerializer.SerializeToUtf8Bytes(new TelegraphEnvelope("after-corrupt", DateTimeOffset.UtcNow));
            await WriteLengthPrefixedFrameAsync(serverStream, goodPayload);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            TelegraphEnvelope received = await ReadOneAsync<TelegraphEnvelope>(subscriber, cts.Token);

            // Length-based framing tracks frame 1's boundary by its declared byte count, not by
            // scanning for a delimiter -- so frame 2 arrives intact and distinct from it,
            // regardless of whether frame 1 itself happened to deserialise.
            Assert.Equal("after-corrupt", received.EntityId);
        }
        finally
        {
            listener.Stop();
        }
    }

    [Fact]
    public async Task LengthPrefixedFramingRejectsAnImplausibleLengthPrefix()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;

            using var subscriber = new TelegraphSubscriber("127.0.0.1", port, TelegraphFraming.LengthPrefixed);
            Task connectTask = subscriber.ConnectAsync();

            using TcpClient serverSideClient = await listener.AcceptTcpClientAsync();
            await connectTask;

            using NetworkStream serverStream = serverSideClient.GetStream();

            var bogusLengthPrefix = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(bogusLengthPrefix, TelegraphSubscriber.MaxFrameSize + 1);
            await serverStream.WriteAsync(bogusLengthPrefix);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await Assert.ThrowsAsync<InvalidDataException>(async () =>
            {
                await foreach (TelegraphEnvelope _ in subscriber.ReadAsync<TelegraphEnvelope>(cts.Token))
                {
                    break;
                }
            });
        }
        finally
        {
            listener.Stop();
        }
    }

    private static async Task WriteLengthPrefixedFrameAsync(NetworkStream stream, byte[] payload)
    {
        var lengthPrefix = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(lengthPrefix, payload.Length);
        await stream.WriteAsync(lengthPrefix);
        await stream.WriteAsync(payload);
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
