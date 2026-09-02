using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Telegraph.Tests;

/// <summary>
/// Proves the pre-shared-key handshake authenticates a real loopback connection, that a
/// mismatched secret never reaches the broadcast list or yields any messages, and that a
/// subscriber configured for the handshake fails cleanly against something that never completes
/// it.
/// </summary>
public sealed class PreSharedKeyHandshakeTests
{
    [Fact]
    public async Task SubscriberReceivesMessagesAfterASuccessfulHandshake()
    {
        using var publisher = new TelegraphPublisher(0, "correct-horse-battery-staple");
        await publisher.StartAsync();

        using var subscriber = new TelegraphSubscriber("127.0.0.1", publisher.Port, "correct-horse-battery-staple");
        await subscriber.ConnectAsync();
        await WaitForSubscriberCountAsync(publisher, 1);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Task<TelegraphEnvelope> readTask = ReadOneAsync<TelegraphEnvelope>(subscriber, cts.Token);

        publisher.Publish(new TelegraphEnvelope("psk-entity", DateTimeOffset.UtcNow));

        TelegraphEnvelope received = await readTask;
        Assert.Equal("psk-entity", received.EntityId);
    }

    [Fact]
    public async Task AConnectionWithAWrongSharedSecretIsNeverAddedAndReadsNothing()
    {
        using var publisher = new TelegraphPublisher(0, "correct-secret");
        await publisher.StartAsync();

        using var subscriber = new TelegraphSubscriber("127.0.0.1", publisher.Port, "wrong-secret");

        // ConnectAsync itself completes -- the subscriber sends its (wrong) response and moves
        // on, since there is no explicit rejection message in the protocol.
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

    [Fact]
    public async Task ConnectAsyncThrowsIfThePublisherClosesBeforeSendingTheNonce()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;

            Task acceptAndCloseTask = Task.Run(async () =>
            {
                using TcpClient serverSideClient = await listener.AcceptTcpClientAsync();
                // Close immediately, without ever sending a nonce.
            });

            using var subscriber = new TelegraphSubscriber("127.0.0.1", port, "some-secret");
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

            await Assert.ThrowsAsync<AuthenticationException>(() => subscriber.ConnectAsync(cts.Token));

            await acceptAndCloseTask;
        }
        finally
        {
            listener.Stop();
        }
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
