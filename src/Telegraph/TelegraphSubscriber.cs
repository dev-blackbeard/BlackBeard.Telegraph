using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Telegraph;

/// <summary>
/// Connects to a <see cref="TelegraphPublisher"/> and reads its broadcast as a sequence of
/// messages.
/// </summary>
/// <remarks>
/// Reads and deserialises one JSON line at a time, in the order the publisher wrote them. A line
/// that fails to deserialise as the requested type is skipped rather than ending the sequence —
/// a subscriber reading as one message type on a stream that occasionally carries another should
/// not stop over a shape it does not recognise.
/// </remarks>
public sealed class TelegraphSubscriber : IDisposable
{
    private const int NonceSize = 32;

    private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true,
    };

    private readonly string _host;
    private readonly int _port;
    private readonly byte[]? _sharedSecret;
    private TcpClient? _client;
    private StreamReader? _reader;
    private bool _disposed;

    /// <summary>Creates a subscriber. Call <see cref="ConnectAsync(CancellationToken)"/> before reading.</summary>
    /// <param name="host">The publisher's host name or address.</param>
    /// <param name="port">The publisher's port.</param>
    /// <exception cref="ArgumentNullException"><paramref name="host"/> is <c>null</c>.</exception>
    public TelegraphSubscriber(string host, int port)
    {
        if (host == null)
        {
            throw new ArgumentNullException(nameof(host));
        }

        _host = host;
        _port = port;
    }

    /// <summary>Creates a subscriber that completes a pre-shared-key handshake on connect.</summary>
    /// <param name="host">The publisher's host name or address.</param>
    /// <param name="port">The publisher's port.</param>
    /// <param name="sharedSecret">
    /// The secret this subscriber and the <see cref="TelegraphPublisher"/> it connects to must
    /// agree on out of band. Must not be null or empty.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="host"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="sharedSecret"/> is null or empty.</exception>
    /// <remarks>
    /// See the remarks on <see cref="TelegraphPublisher(int, string)"/> for the handshake this
    /// performs, and what it does and does not protect against. If the secret this subscriber was
    /// constructed with doesn't match the publisher's, the publisher closes the connection as soon
    /// as it sees the mismatched response -- there is no explicit rejection message, so this
    /// surfaces as the connection appearing closed the moment <see cref="ReadAsync{T}(CancellationToken)"/>
    /// is used, the same as it would for any other reason the publisher might close it.
    /// </remarks>
    public TelegraphSubscriber(string host, int port, string sharedSecret)
        : this(host, port)
    {
        if (string.IsNullOrEmpty(sharedSecret))
        {
            throw new ArgumentException("Shared secret must not be null or empty.", nameof(sharedSecret));
        }

        _sharedSecret = Encoding.UTF8.GetBytes(sharedSecret);
    }

    /// <summary><c>true</c> once <see cref="ConnectAsync(CancellationToken)"/> has completed successfully.</summary>
    public bool IsConnected
    {
        get { return _client != null && _client.Connected; }
    }

    /// <summary>Opens the connection to the publisher.</summary>
    /// <param name="cancellationToken">Cancels the connection attempt, and the pre-shared-key handshake if one is required.</param>
    /// <exception cref="AuthenticationException">
    /// A shared secret was configured, but the publisher closed the connection before sending its
    /// nonce.
    /// </exception>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        var client = new TcpClient();
        await client.ConnectAsync(_host, _port, cancellationToken).ConfigureAwait(false);

        NetworkStream stream = client.GetStream();

        if (_sharedSecret != null)
        {
            var nonce = new byte[NonceSize];
            bool gotNonce = await ReadFullyAsync(stream, nonce, nonce.Length, cancellationToken).ConfigureAwait(false);
            if (!gotNonce)
            {
                client.Dispose();
                throw new AuthenticationException("The publisher closed the connection before completing the pre-shared-key handshake.");
            }

            byte[] response;
            using (var hmac = new HMACSHA256(_sharedSecret))
            {
                response = hmac.ComputeHash(nonce);
            }

            await stream.WriteAsync(response, cancellationToken).ConfigureAwait(false);
        }

        _client = client;
        _reader = new StreamReader(stream, Encoding.UTF8);
    }

    /// <summary>
    /// Reads messages as they arrive, until the connection closes or <paramref name="cancellationToken"/> is cancelled.
    /// </summary>
    /// <typeparam name="T">The message type. Any type <see cref="JsonSerializer"/> can deserialise.</typeparam>
    /// <param name="cancellationToken">Stops reading.</param>
    /// <returns>The messages, in the order they were published.</returns>
    /// <exception cref="InvalidOperationException"><see cref="ConnectAsync(CancellationToken)"/> has not completed.</exception>
    public async IAsyncEnumerable<T> ReadAsync<T>([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (_reader == null)
        {
            throw new InvalidOperationException("Call ConnectAsync before ReadAsync.");
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            string? line = await _reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            if (line == null)
            {
                // The publisher closed the connection.
                yield break;
            }

            if (string.IsNullOrWhiteSpace(line))
            {
                continue;
            }

            T? message;
            try
            {
                message = JsonSerializer.Deserialize<T>(line, JsonOptions);
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

    /// <summary>Closes the connection.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _reader?.Dispose();
        _client?.Dispose();
    }

    private static async Task<bool> ReadFullyAsync(Stream stream, byte[] buffer, int count, CancellationToken cancellationToken)
    {
        int offset = 0;
        while (offset < count)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(offset, count - offset), cancellationToken).ConfigureAwait(false);
            if (read == 0)
            {
                return false;
            }

            offset += read;
        }

        return true;
    }
}
