using System;
using System.Collections.Generic;
using System.IO;
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
/// Reads and deserialises one JSON line at a time, in the order the publisher wrote them. A line
/// that fails to deserialise as the requested type is skipped rather than ending the sequence —
/// a subscriber reading as one message type on a stream that occasionally carries another should
/// not stop over a shape it does not recognise.
/// </remarks>
public sealed class TelegraphSubscriber : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true,
    };

    private readonly string _host;
    private readonly int _port;
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

    /// <summary><c>true</c> once <see cref="ConnectAsync(CancellationToken)"/> has completed successfully.</summary>
    public bool IsConnected
    {
        get { return _client != null && _client.Connected; }
    }

    /// <summary>Opens the connection to the publisher.</summary>
    /// <param name="cancellationToken">Cancels the connection attempt.</param>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        var client = new TcpClient();
        await client.ConnectAsync(_host, _port, cancellationToken).ConfigureAwait(false);

        _client = client;
        _reader = new StreamReader(client.GetStream(), Encoding.UTF8);
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
}
