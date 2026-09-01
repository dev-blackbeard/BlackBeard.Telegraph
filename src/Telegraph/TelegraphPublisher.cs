using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
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
/// </remarks>
public sealed class TelegraphPublisher : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true,
    };

    private readonly TcpListener _listener;
    private readonly List<ConnectedSubscriber> _clients = new List<ConnectedSubscriber>();
    private readonly object _clientsGate = new object();
    private CancellationTokenSource? _acceptLoopCancellation;
    private Task? _acceptLoopTask;
    private bool _disposed;

    /// <summary>Creates a publisher bound to a local port.</summary>
    /// <param name="port">The TCP port to listen on. Pass <c>0</c> to let the OS choose one; read it back from <see cref="Port"/> after <see cref="StartAsync(CancellationToken)"/>.</param>
    public TelegraphPublisher(int port)
    {
        _listener = new TcpListener(IPAddress.Any, port);
    }

    /// <summary>The port actually bound. Only meaningful after <see cref="StartAsync(CancellationToken)"/> has returned.</summary>
    public int Port
    {
        get { return ((IPEndPoint)_listener.LocalEndpoint).Port; }
    }

    /// <summary>How many subscribers are currently connected.</summary>
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

    /// <summary>
    /// The subscribers currently connected: a snapshot at the moment of the call, not a live view.
    /// </summary>
    public IReadOnlyList<TelegraphSubscriberInfo> Subscribers
    {
        get
        {
            lock (_clientsGate)
            {
                var infos = new List<TelegraphSubscriberInfo>(_clients.Count);
                foreach (ConnectedSubscriber subscriber in _clients)
                {
                    infos.Add(subscriber.Info);
                }

                return infos;
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

        List<ConnectedSubscriber> snapshot;
        lock (_clientsGate)
        {
            snapshot = new List<ConnectedSubscriber>(_clients);
        }

        List<ConnectedSubscriber>? dead = null;
        foreach (ConnectedSubscriber subscriber in snapshot)
        {
            try
            {
                NetworkStream stream = subscriber.Client.GetStream();
                stream.Write(bytes, 0, bytes.Length);
                subscriber.Info.RecordSent(bytes.Length);
            }
            catch (IOException)
            {
                dead ??= new List<ConnectedSubscriber>();
                dead.Add(subscriber);
            }
            catch (ObjectDisposedException)
            {
                dead ??= new List<ConnectedSubscriber>();
                dead.Add(subscriber);
            }
            catch (SocketException)
            {
                dead ??= new List<ConnectedSubscriber>();
                dead.Add(subscriber);
            }
        }

        if (dead != null)
        {
            lock (_clientsGate)
            {
                foreach (ConnectedSubscriber subscriber in dead)
                {
                    _clients.Remove(subscriber);
                }
            }

            foreach (ConnectedSubscriber subscriber in dead)
            {
                subscriber.Client.Dispose();
            }
        }
    }

    /// <summary>
    /// Disconnects one subscriber without affecting any other, or the publisher itself.
    /// </summary>
    /// <param name="subscriber">A <see cref="TelegraphSubscriberInfo"/> obtained from <see cref="Subscribers"/>.</param>
    /// <returns>
    /// <see langword="true"/> if the subscriber was connected and has now been disconnected;
    /// <see langword="false"/> if it had already disconnected on its own (e.g. a dead connection
    /// dropped during <see cref="Publish{T}(T)"/>), in which case there is nothing left to do.
    /// </returns>
    public bool Disconnect(TelegraphSubscriberInfo subscriber)
    {
        ConnectedSubscriber? match = null;
        lock (_clientsGate)
        {
            foreach (ConnectedSubscriber candidate in _clients)
            {
                if (ReferenceEquals(candidate.Info, subscriber))
                {
                    match = candidate;
                    break;
                }
            }

            // Removed after the loop, not inside it -- mutating _clients mid-enumeration only
            // happened to be safe here because the break on the line above meant MoveNext() was
            // never called again; find-then-remove doesn't depend on that.
            if (match != null)
            {
                _clients.Remove(match);
            }
        }

        if (match == null)
        {
            return false;
        }

        match.Client.Dispose();
        return true;
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

            // A client that connects and drops immediately (a port scanner, a health check, a
            // load-balancer probe) can make RemoteEndPoint throw on the now-defunct socket. That
            // must not escape this loop -- an unhandled exception here would fault AcceptLoopAsync
            // and permanently stop the publisher from accepting any further subscribers, with
            // nothing surfaced anywhere. Treat it the same as any other subscriber that never made
            // it: skip it and keep accepting.
            try
            {
                var info = new TelegraphSubscriberInfo((IPEndPoint)client.Client.RemoteEndPoint!, DateTimeOffset.UtcNow);
                lock (_clientsGate)
                {
                    _clients.Add(new ConnectedSubscriber(client, info));
                }
            }
            catch (ObjectDisposedException)
            {
                client.Dispose();
            }
            catch (SocketException)
            {
                client.Dispose();
            }
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

        List<ConnectedSubscriber> snapshot;
        lock (_clientsGate)
        {
            snapshot = new List<ConnectedSubscriber>(_clients);
            _clients.Clear();
        }

        foreach (ConnectedSubscriber subscriber in snapshot)
        {
            subscriber.Client.Dispose();
        }

        _acceptLoopCancellation?.Dispose();
    }

    private sealed class ConnectedSubscriber
    {
        public ConnectedSubscriber(TcpClient client, TelegraphSubscriberInfo info)
        {
            Client = client;
            Info = info;
        }

        public TcpClient Client { get; }

        public TelegraphSubscriberInfo Info { get; }
    }
}
