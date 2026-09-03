using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Telegraph;

/// <summary>
/// Registers with a <see cref="TelegraphUdpPublisher"/> and reads its broadcast as a sequence of
/// messages, one per UDP datagram.
/// </summary>
/// <remarks>
/// The UDP counterpart to <see cref="TelegraphSubscriber"/>: no reliability, ordering, or
/// delivery guarantees. A datagram that fails to deserialise as the requested type is skipped
/// rather than ending the sequence, the same as <see cref="TelegraphSubscriber.ReadAsync{T}(CancellationToken)"/>.
/// </remarks>
public sealed class TelegraphUdpSubscriber : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true,
    };

    private readonly string _host;
    private readonly int _port;
    private UdpClient? _client;
    private bool _disposed;

    /// <summary>Creates a subscriber. Call <see cref="ConnectAsync(CancellationToken)"/> before reading.</summary>
    /// <param name="host">The publisher's host name or address.</param>
    /// <param name="port">The publisher's port.</param>
    /// <exception cref="ArgumentNullException"><paramref name="host"/> is <c>null</c>.</exception>
    public TelegraphUdpSubscriber(string host, int port)
    {
        if (host == null)
        {
            throw new ArgumentNullException(nameof(host));
        }

        _host = host;
        _port = port;
    }

    /// <summary><c>true</c> once <see cref="ConnectAsync(CancellationToken)"/> has completed successfully.</summary>
    public bool IsConnected
    {
        get { return _client != null; }
    }

    /// <summary>
    /// Registers with the publisher by sending it one (otherwise empty) datagram, so it starts
    /// including this subscriber in its broadcasts.
    /// </summary>
    /// <param name="cancellationToken">Cancels the registration send.</param>
    /// <remarks>
    /// Registration is itself a UDP datagram and carries no delivery guarantee -- if it is lost,
    /// the publisher never learns about this subscriber. This call has no way to detect that; a
    /// caller for whom that matters should re-register periodically rather than assume one
    /// successful call here means the publisher received it.
    /// </remarks>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        var client = new UdpClient();
        client.Connect(_host, _port);
        await client.SendAsync(ReadOnlyMemory<byte>.Empty, cancellationToken).ConfigureAwait(false);

        _client = client;
    }

    /// <summary>
    /// Reads messages as they arrive, until <paramref name="cancellationToken"/> is cancelled --
    /// there is no "the publisher closed the connection" signal over UDP the way there is for
    /// <see cref="TelegraphSubscriber.ReadAsync{T}(CancellationToken)"/>.
    /// </summary>
    /// <typeparam name="T">The message type. Any type <see cref="JsonSerializer"/> can deserialise.</typeparam>
    /// <param name="cancellationToken">Stops reading.</param>
    /// <returns>The messages, in whatever order the network delivered their datagrams.</returns>
    /// <exception cref="InvalidOperationException"><see cref="ConnectAsync(CancellationToken)"/> has not completed.</exception>
    public async IAsyncEnumerable<T> ReadAsync<T>([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (_client == null)
        {
            throw new InvalidOperationException("Call ConnectAsync before ReadAsync.");
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            UdpReceiveResult result = await _client.ReceiveAsync(cancellationToken).ConfigureAwait(false);

            T? message;
            try
            {
                message = JsonSerializer.Deserialize<T>(result.Buffer, JsonOptions);
            }
            catch (JsonException)
            {
                continue;
            }

            if (message != null)
            {
                yield return message;
            }
        }
    }

    /// <summary>Stops listening.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _client?.Dispose();
    }
}
