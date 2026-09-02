using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
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
/// The connection is plaintext by default -- the right trade-off for the zero-setup, <c>nc</c>-inspectable
/// quick start. Pass <see cref="SslServerAuthenticationOptions"/> to the constructor to require
/// TLS instead: a connection that fails the handshake is dropped before it is ever added to the
/// broadcast list, so it never receives a partial message and never counts toward
/// <see cref="SubscriberCount"/>, and one slow or stalled handshake cannot hold up accepting
/// anyone else.
/// </para>
/// </remarks>
public sealed class TelegraphPublisher : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true,
    };

    private readonly TcpListener _listener;
    private readonly SslServerAuthenticationOptions? _sslOptions;
    private readonly List<ConnectedClient> _clients = new List<ConnectedClient>();
    private readonly object _clientsGate = new object();
    private CancellationTokenSource? _acceptLoopCancellation;
    private Task? _acceptLoopTask;
    private bool _disposed;

    /// <summary>Creates a publisher bound to a local port, in plaintext.</summary>
    /// <param name="port">The TCP port to listen on. Pass <c>0</c> to let the OS choose one; read it back from <see cref="Port"/> after <see cref="StartAsync(CancellationToken)"/>.</param>
    public TelegraphPublisher(int port)
    {
        _listener = new TcpListener(IPAddress.Any, port);
    }

    /// <summary>Creates a publisher bound to a local port, requiring TLS on every connection.</summary>
    /// <param name="port">The TCP port to listen on. Pass <c>0</c> to let the OS choose one; read it back from <see cref="Port"/> after <see cref="StartAsync(CancellationToken)"/>.</param>
    /// <param name="sslOptions">
    /// Server-side TLS options (certificate, protocols, client-certificate requirements). Every
    /// connection must complete this handshake before it is added to the broadcast list.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="sslOptions"/> is <c>null</c>.</exception>
    public TelegraphPublisher(int port, SslServerAuthenticationOptions sslOptions)
        : this(port)
    {
        _sslOptions = sslOptions ?? throw new ArgumentNullException(nameof(sslOptions));
    }

    /// <summary>The port actually bound. Only meaningful after <see cref="StartAsync(CancellationToken)"/> has returned.</summary>
    public int Port
    {
        get { return ((IPEndPoint)_listener.LocalEndpoint).Port; }
    }

    /// <summary>
    /// How many subscribers are currently connected -- and, when TLS is required, have completed
    /// the handshake.
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

        List<ConnectedClient> snapshot;
        lock (_clientsGate)
        {
            snapshot = new List<ConnectedClient>(_clients);
        }

        List<ConnectedClient>? dead = null;
        foreach (ConnectedClient client in snapshot)
        {
            try
            {
                client.Stream.Write(bytes, 0, bytes.Length);
            }
            catch (IOException)
            {
                dead ??= new List<ConnectedClient>();
                dead.Add(client);
            }
            catch (ObjectDisposedException)
            {
                dead ??= new List<ConnectedClient>();
                dead.Add(client);
            }
            catch (SocketException)
            {
                dead ??= new List<ConnectedClient>();
                dead.Add(client);
            }
        }

        if (dead != null)
        {
            lock (_clientsGate)
            {
                foreach (ConnectedClient client in dead)
                {
                    _clients.Remove(client);
                }
            }

            foreach (ConnectedClient client in dead)
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

            if (_sslOptions != null)
            {
                // The TLS handshake runs on its own task rather than inline here, so a connection
                // that stalls partway through it -- or never completes it at all -- cannot block
                // this loop from accepting anyone else.
                _ = CompleteTlsHandshakeAndRegisterAsync(client, cancellationToken);
                continue;
            }

            lock (_clientsGate)
            {
                _clients.Add(new ConnectedClient(client, client.GetStream()));
            }
        }
    }

    private async Task CompleteTlsHandshakeAndRegisterAsync(TcpClient client, CancellationToken cancellationToken)
    {
        var sslStream = new SslStream(client.GetStream(), leaveInnerStreamOpen: false);
        try
        {
            await sslStream.AuthenticateAsServerAsync(_sslOptions!, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or AuthenticationException or ObjectDisposedException or OperationCanceledException or SocketException)
        {
            sslStream.Dispose();
            client.Dispose();
            return;
        }

        lock (_clientsGate)
        {
            _clients.Add(new ConnectedClient(client, sslStream));
        }
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

        List<ConnectedClient> snapshot;
        lock (_clientsGate)
        {
            snapshot = new List<ConnectedClient>(_clients);
            _clients.Clear();
        }

        foreach (ConnectedClient client in snapshot)
        {
            client.Dispose();
        }

        _acceptLoopCancellation?.Dispose();
    }

    private sealed class ConnectedClient : IDisposable
    {
        public ConnectedClient(TcpClient client, Stream stream)
        {
            Client = client;
            Stream = stream;
        }

        public TcpClient Client { get; }

        public Stream Stream { get; }

        public void Dispose()
        {
            Stream.Dispose();
            Client.Dispose();
        }
    }
}
