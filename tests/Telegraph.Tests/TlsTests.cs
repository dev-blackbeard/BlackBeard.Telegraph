using System;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Telegraph.Tests;

/// <summary>
/// Proves TLS round-trips over real loopback TCP when both ends opt in, that it is genuinely
/// enforced (a client that doesn't trust the server certificate cannot connect), and that a
/// connection which fails the handshake never counts as a subscriber.
/// </summary>
public sealed class TlsTests
{
    [Fact]
    public async Task SubscriberReceivesMessagesOverTls()
    {
        using X509Certificate2 certificate = CreateSelfSignedCertificate();

        using var publisher = new TelegraphPublisher(0, new SslServerAuthenticationOptions
        {
            ServerCertificate = certificate,
        });
        await publisher.StartAsync();

        var clientOptions = new SslClientAuthenticationOptions
        {
            TargetHost = "localhost",
            RemoteCertificateValidationCallback = (_, cert, _, _) =>
                cert != null && cert.GetCertHashString() == certificate.GetCertHashString(),
        };

        using var subscriber = new TelegraphSubscriber("127.0.0.1", publisher.Port, clientOptions);
        await subscriber.ConnectAsync();
        await WaitForSubscriberCountAsync(publisher, 1);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        Task<TelegraphEnvelope> readTask = ReadOneAsync<TelegraphEnvelope>(subscriber, cts.Token);

        publisher.Publish(new TelegraphEnvelope("tls-entity", DateTimeOffset.UtcNow));

        TelegraphEnvelope received = await readTask;
        Assert.Equal("tls-entity", received.EntityId);
    }

    [Fact]
    public async Task ConnectingWithoutTrustingTheServerCertificateFails()
    {
        using X509Certificate2 certificate = CreateSelfSignedCertificate();

        using var publisher = new TelegraphPublisher(0, new SslServerAuthenticationOptions
        {
            ServerCertificate = certificate,
        });
        await publisher.StartAsync();

        // No RemoteCertificateValidationCallback -- the default chain-trust validation, which a
        // self-signed certificate never satisfies.
        var clientOptions = new SslClientAuthenticationOptions { TargetHost = "localhost" };

        using var subscriber = new TelegraphSubscriber("127.0.0.1", publisher.Port, clientOptions);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        await Assert.ThrowsAnyAsync<AuthenticationException>(() => subscriber.ConnectAsync(cts.Token));

        // The failed handshake must never have made it into the broadcast list.
        Assert.Equal(0, publisher.SubscriberCount);
    }

    private static X509Certificate2 CreateSelfSignedCertificate()
    {
        using RSA rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using X509Certificate2 ephemeral = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddMinutes(5));

        // Re-imported from PFX bytes rather than used directly: a certificate straight out of
        // CreateSelfSigned can carry an ephemeral key that SslStream's server-side handshake
        // can't use on some platforms (notably Windows) without this round-trip.
        return new X509Certificate2(ephemeral.Export(X509ContentType.Pfx));
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
