using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
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
/// Reads and deserialises one message at a time, in the order the publisher wrote them. A message
/// that fails to deserialise as the requested type is skipped rather than ending the sequence —
/// a subscriber reading as one message type on a stream that occasionally carries another should
/// not stop over a shape it does not recognise.
/// </remarks>
public sealed class TelegraphSubscriber : IDisposable
{
    /// <summary>
    /// The largest length prefix <see cref="ReadAsync{T}(CancellationToken)"/> will honour under
    /// <see cref="TelegraphFraming.LengthPrefixed"/>, in bytes. Length-prefixed framing has no way
    /// to resynchronise with the stream once a prefix is wrong, so a clearly-bogus value (whether
    /// from a corrupted stream or a publisher on a different framing) is rejected outright rather
    /// than attempting an unbounded allocation.
    /// </summary>
    public const int MaxFrameSize = 16 * 1024 * 1024;

    private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true,
    };

    private readonly string _host;
    private readonly int _port;
    private readonly TelegraphFraming _framing;
    private readonly SslClientAuthenticationOptions? _sslOptions;
    private TcpClient? _client;
    private Stream? _stream;
    private StreamReader? _reader;
    private bool _disposed;

    /// <summary>Creates a subscriber that connects in plaintext, using <see cref="TelegraphFraming.NewlineDelimited"/> framing. Call <see cref="ConnectAsync(CancellationToken)"/> before reading.</summary>
    /// <param name="host">The publisher's host name or address.</param>
    /// <param name="port">The publisher's port.</param>
    /// <exception cref="ArgumentNullException"><paramref name="host"/> is <c>null</c>.</exception>
    public TelegraphSubscriber(string host, int port)
        : this(host, port, TelegraphFraming.NewlineDelimited)
    {
    }

    /// <summary>Creates a subscriber that connects in plaintext, using the given wire framing. Call <see cref="ConnectAsync(CancellationToken)"/> before reading.</summary>
    /// <param name="host">The publisher's host name or address.</param>
    /// <param name="port">The publisher's port.</param>
    /// <param name="framing">
    /// How messages are delimited on the wire. Must match the <see cref="TelegraphFraming"/> the
    /// publisher was constructed with -- there is nothing on the wire that identifies which one is
    /// in use.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="host"/> is <c>null</c>.</exception>
    public TelegraphSubscriber(string host, int port, TelegraphFraming framing)
    {
        if (host == null)
        {
            throw new ArgumentNullException(nameof(host));
        }

        _host = host;
        _port = port;
        _framing = framing;
    }

    /// <summary>
    /// Creates a subscriber that requires TLS, using <see cref="TelegraphFraming.NewlineDelimited"/>
    /// framing. Call <see cref="ConnectAsync(CancellationToken)"/> before reading.
    /// </summary>
    /// <param name="host">The publisher's host name or address.</param>
    /// <param name="port">The publisher's port.</param>
    /// <param name="sslOptions">
    /// Client-side TLS options (target host, certificate validation, client certificates). Must
    /// pair with a <see cref="TelegraphPublisher"/> constructed with <see cref="System.Net.Security.SslServerAuthenticationOptions"/>
    /// -- there is nothing on the wire that identifies whether TLS is expected.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="host"/> or <paramref name="sslOptions"/> is <c>null</c>.</exception>
    public TelegraphSubscriber(string host, int port, SslClientAuthenticationOptions sslOptions)
        : this(host, port, TelegraphFraming.NewlineDelimited)
    {
        _sslOptions = sslOptions ?? throw new ArgumentNullException(nameof(sslOptions));
    }

    /// <summary><c>true</c> once <see cref="ConnectAsync(CancellationToken)"/> has completed successfully.</summary>
    public bool IsConnected
    {
        get { return _client != null && _client.Connected; }
    }

    /// <summary>Opens the connection to the publisher.</summary>
    /// <param name="cancellationToken">Cancels the connection attempt, and the TLS handshake if one is required.</param>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        var client = new TcpClient();
        await client.ConnectAsync(_host, _port, cancellationToken).ConfigureAwait(false);

        Stream stream = client.GetStream();
        if (_sslOptions != null)
        {
            var sslStream = new SslStream(stream, leaveInnerStreamOpen: false);
            bool authenticated = false;
            try
            {
                await sslStream.AuthenticateAsClientAsync(_sslOptions, cancellationToken).ConfigureAwait(false);
                authenticated = true;
            }
            finally
            {
                if (!authenticated)
                {
                    sslStream.Dispose();
                    client.Dispose();
                }
            }

            stream = sslStream;
        }

        _client = client;
        _stream = stream;

        if (_framing == TelegraphFraming.NewlineDelimited)
        {
            _reader = new StreamReader(stream, Encoding.UTF8);
        }
    }

    /// <summary>
    /// Reads messages as they arrive, until the connection closes or <paramref name="cancellationToken"/> is cancelled.
    /// </summary>
    /// <typeparam name="T">The message type. Any type <see cref="JsonSerializer"/> can deserialise.</typeparam>
    /// <param name="cancellationToken">Stops reading.</param>
    /// <returns>The messages, in the order they were published.</returns>
    /// <exception cref="InvalidOperationException"><see cref="ConnectAsync(CancellationToken)"/> has not completed.</exception>
    /// <exception cref="InvalidDataException">
    /// Under <see cref="TelegraphFraming.LengthPrefixed"/>, a length prefix was negative or larger
    /// than <see cref="MaxFrameSize"/>.
    /// </exception>
    public async IAsyncEnumerable<T> ReadAsync<T>([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (_stream == null)
        {
            throw new InvalidOperationException("Call ConnectAsync before ReadAsync.");
        }

        if (_framing == TelegraphFraming.LengthPrefixed)
        {
            var lengthBuffer = new byte[4];
            while (!cancellationToken.IsCancellationRequested)
            {
                bool gotLength = await ReadFullyAsync(_stream, lengthBuffer, 4, cancellationToken).ConfigureAwait(false);
                if (!gotLength)
                {
                    // The publisher closed the connection.
                    yield break;
                }

                int payloadLength = BinaryPrimitives.ReadInt32BigEndian(lengthBuffer);
                if (payloadLength < 0 || payloadLength > MaxFrameSize)
                {
                    throw new InvalidDataException(
                        $"Length-prefixed frame claims {payloadLength} bytes, outside 0..{MaxFrameSize} -- refusing to allocate for what is likely a corrupted stream or a framing mismatch with the publisher.");
                }

                var payload = new byte[payloadLength];
                bool gotPayload = await ReadFullyAsync(_stream, payload, payloadLength, cancellationToken).ConfigureAwait(false);
                if (!gotPayload)
                {
                    // The connection closed mid-frame.
                    yield break;
                }

                T? message;
                try
                {
                    message = JsonSerializer.Deserialize<T>(payload, JsonOptions);
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

            yield break;
        }

        // _reader is always set whenever _stream is and _framing is NewlineDelimited (see
        // ConnectAsync) -- the _stream == null check above already rules out the only case where
        // it could be missing here.
        while (!cancellationToken.IsCancellationRequested)
        {
            string? line = await _reader!.ReadLineAsync(cancellationToken).ConfigureAwait(false);
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

        // _reader (when set) owns and disposes _stream itself; disposing _stream too is only
        // needed for the LengthPrefixed path, where there is no _reader wrapping it. Stream
        // disposal is idempotent, so covering both here is safe either way.
        _reader?.Dispose();
        _stream?.Dispose();
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
