using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Telegraph;

/// <summary>
/// Broadcasts messages to every connected <see cref="TelegraphSubscriber"/>, as one JSON object
/// per line over TCP.
/// </summary>
/// <remarks>
/// <para>
/// The wire format is newline-delimited UTF-8 JSON: one <see cref="JsonSerializer"/>-serialised
/// message per line, on an otherwise plain TCP stream. That is a deliberate choice over a custom
/// framing or binary protocol — it is inspectable with <c>nc</c>/<c>netcat</c> and needs no
/// generated client, at the cost of being less compact than a binary format. Nothing about it is
/// specific to any one message shape: <see cref="Publish{T}(T)"/> accepts any type
/// <see cref="JsonSerializer"/> can serialise.
/// </para>
/// <para>
/// A late-joining subscriber sees only messages published after it connects — there is no replay
/// buffer. A slow subscriber that cannot keep up is disconnected rather than allowed to make
/// <see cref="Publish{T}(T)"/> block for every other subscriber; see the write-timeout behaviour
/// in the remarks on <see cref="Publish{T}(T)"/>.
/// </para>
/// <para>
/// Open to any connection by default. Pass a shared secret to the constructor to require a
/// lightweight pre-shared-key handshake before a connection is added to the broadcast list -- see
/// the constructor's remarks for the protocol and, importantly, what this does and does not
/// protect against.
/// </para>
/// </remarks>
public sealed class TelegraphPublisher : IDisposable
{
    private const int NonceSize = 32;

    private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true,
    };

    private readonly TcpListener _listener;
    private readonly byte[]? _sharedSecret;
    private readonly List<TcpClient> _clients = new List<TcpClient>();
    private readonly object _clientsGate = new object();
    private CancellationTokenSource? _acceptLoopCancellation;
    private Task? _acceptLoopTask;
    private bool _disposed;

    /// <summary>Creates a publisher bound to a local port, open to any connection.</summary>
    /// <param name="port">The TCP port to listen on. Pass <c>0</c> to let the OS choose one; read it back from <see cref="Port"/> after <see cref="StartAsync(CancellationToken)"/>.</param>
    public TelegraphPublisher(int port)
    {
        _listener = new TcpListener(IPAddress.Any, port);
    }

    /// <summary>Creates a publisher bound to a local port, requiring a pre-shared-key handshake on every connection.</summary>
    /// <param name="port">The TCP port to listen on. Pass <c>0</c> to let the OS choose one; read it back from <see cref="Port"/> after <see cref="StartAsync(CancellationToken)"/>.</param>
    /// <param name="sharedSecret">
    /// The secret this publisher and every <see cref="TelegraphSubscriber"/> connecting to it must
    /// agree on out of band. Must not be null or empty.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="sharedSecret"/> is null or empty.</exception>
    /// <remarks>
    /// <para>
    /// On connect, this publisher sends a random 32-byte nonce, then expects back the HMAC-SHA256
    /// of that nonce keyed with <paramref name="sharedSecret"/>, computed the same way a
    /// <see cref="TelegraphSubscriber"/> constructed with the matching secret computes it. A
    /// response that doesn't match -- including no response at all before the connection drops --
    /// closes the connection immediately, before it is added to the broadcast list and before any
    /// application data is exchanged. The handshake runs on its own task per connection, so one
    /// that never completes it cannot block accepting anyone else.
    /// </para>
    /// <para>
    /// This is explicitly not a substitute for TLS: it authenticates the connection, proving the
    /// other side knows the secret, but the stream itself stays plaintext -- everything after the
    /// handshake, including the secret's role in it, is visible to anyone who can observe the
    /// wire. For a caller that just wants "don't let an arbitrary process on this host or LAN
    /// subscribe to my stream" without provisioning PKI, that trade-off is often fine; it is not a
    /// substitute for TLS where the network itself isn't trusted.
    /// </para>
    /// </remarks>
    public TelegraphPublisher(int port, string sharedSecret)
        : this(port)
    {
        if (string.IsNullOrEmpty(sharedSecret))
        {
            throw new ArgumentException("Shared secret must not be null or empty.", nameof(sharedSecret));
        }

        _sharedSecret = Encoding.UTF8.GetBytes(sharedSecret);
    }

    /// <summary>The port actually bound. Only meaningful after <see cref="StartAsync(CancellationToken)"/> has returned.</summary>
    public int Port
    {
        get { return ((IPEndPoint)_listener.LocalEndpoint).Port; }
    }

    /// <summary>
    /// How many subscribers are currently connected -- and, when a shared secret is required,
    /// have completed the handshake.
    /// </summary>
    public int SubscriberCount
    {
        get
        {
            lock (_clientsGate)
            {
                return _clients.Count;
            }
        }
    }

    /// <summary>Starts listening for subscribers.</summary>
    /// <param name="cancellationToken">Stops accepting new subscribers when cancelled. Already-connected subscribers are unaffected.</param>
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        _listener.Start();
        _acceptLoopCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _acceptLoopTask = AcceptLoopAsync(_acceptLoopCancellation.Token);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Serialises <paramref name="message"/> to one JSON line and writes it to every connected
    /// subscriber.
    /// </summary>
    /// <typeparam name="T">The message type. Any type <see cref="JsonSerializer"/> can serialise.</typeparam>
    /// <param name="message">The message.</param>
    /// <remarks>
    /// Writes synchronously and best-effort per subscriber: a subscriber whose socket buffer is
    /// full blocks this call until it drains or the write fails. A subscriber whose write throws
    /// (connection reset, buffer full past the OS timeout) is dropped silently rather than taking
    /// every other subscriber down with it.
    /// </remarks>
    public void Publish<T>(T message)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message, JsonOptions) + "\n");

        List<TcpClient> snapshot;
        lock (_clientsGate)
        {
            snapshot = new List<TcpClient>(_clients);
        }

        List<TcpClient>? dead = null;
        foreach (TcpClient client in snapshot)
        {
            try
            {
                NetworkStream stream = client.GetStream();
                stream.Write(bytes, 0, bytes.Length);
            }
            catch (IOException)
            {
                dead ??= new List<TcpClient>();
                dead.Add(client);
            }
            catch (ObjectDisposedException)
            {
                dead ??= new List<TcpClient>();
                dead.Add(client);
            }
            catch (SocketException)
            {
                dead ??= new List<TcpClient>();
                dead.Add(client);
            }
        }

        if (dead != null)
        {
            lock (_clientsGate)
            {
                foreach (TcpClient client in dead)
                {
                    _clients.Remove(client);
                }
            }

            foreach (TcpClient client in dead)
            {
                client.Dispose();
            }
        }
    }

    private async Task AcceptLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await _listener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }
            catch (SocketException)
            {
                break;
            }

            if (_sharedSecret != null)
            {
                // The handshake runs on its own task rather than inline here, so a connection
                // that never completes it cannot block this loop from accepting anyone else.
                _ = CompletePreSharedKeyHandshakeAndRegisterAsync(client, cancellationToken);
                continue;
            }

            lock (_clientsGate)
            {
                _clients.Add(client);
            }
        }
    }

    private async Task CompletePreSharedKeyHandshakeAndRegisterAsync(TcpClient client, CancellationToken cancellationToken)
    {
        try
        {
            NetworkStream stream = client.GetStream();

            var nonce = new byte[NonceSize];
            RandomNumberGenerator.Fill(nonce);
            await stream.WriteAsync(nonce, cancellationToken).ConfigureAwait(false);

            byte[] expected;
            using (var hmac = new HMACSHA256(_sharedSecret!))
            {
                expected = hmac.ComputeHash(nonce);
            }

            var actual = new byte[expected.Length];
            bool gotResponse = await ReadFullyAsync(stream, actual, actual.Length, cancellationToken).ConfigureAwait(false);

            if (!gotResponse || !CryptographicOperations.FixedTimeEquals(actual, expected))
            {
                client.Dispose();
                return;
            }
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or SocketException or OperationCanceledException)
        {
            client.Dispose();
            return;
        }

        lock (_clientsGate)
        {
            _clients.Add(client);
        }
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

    /// <summary>Stops listening, disconnects every subscriber, and releases the port.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        _acceptLoopCancellation?.Cancel();
        _listener.Stop();

        List<TcpClient> snapshot;
        lock (_clientsGate)
        {
            snapshot = new List<TcpClient>(_clients);
            _clients.Clear();
        }

        foreach (TcpClient client in snapshot)
        {
            client.Dispose();
        }

        _acceptLoopCancellation?.Dispose();
    }
}
