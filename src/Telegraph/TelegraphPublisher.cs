using System;
using System.Buffers.Binary;
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
/// per line (or, with <see cref="TelegraphFraming.LengthPrefixed"/>, one length-prefixed JSON
/// object) over TCP.
/// </summary>
/// <remarks>
/// <para>
/// The default wire format is newline-delimited UTF-8 JSON: one <see cref="JsonSerializer"/>-serialised
/// message per line, on an otherwise plain TCP stream. That is a deliberate choice over a custom
/// framing or binary protocol — it is inspectable with <c>nc</c>/<c>netcat</c> and needs no
/// generated client, at the cost of being less compact than a binary format, and of being unsafe
/// for a message whose JSON can itself contain a literal newline (see
/// <see cref="TelegraphFraming.LengthPrefixed"/> for that case). Nothing about it is specific to
/// any one message shape: <see cref="Publish{T}(T)"/> accepts any type
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
/// TLS instead: a connection whose handshake this publisher's own <c>AuthenticateAsServerAsync</c>
/// step rejects (a protocol/cipher mismatch, a required client certificate that's missing) is
/// dropped before it is ever added to the broadcast list, so it never receives a partial message
/// and never counts toward <see cref="SubscriberCount"/>. A subscriber that locally distrusts this
/// publisher's certificate is different: that handshake step can still complete on this side,
/// since trust is the client's own decision and there's no protocol-level step where it reports
/// that back -- that connection is cleaned up the same way any other dead one is, the next time
/// <see cref="Publish{T}(T)"/> tries to write to it and fails. Either way, one slow or stalled
/// handshake cannot hold up accepting anyone else.
/// </para>
/// <para>
/// Accepts a connection from any address by default. Add to <see cref="AllowedRanges"/> -- before
/// <see cref="StartAsync(CancellationToken)"/>, since it is read without synchronisation while the
/// accept loop is running -- to restrict that: a connection from outside every configured range is
/// closed immediately after being accepted, before it is added to the broadcast list, so it never
/// receives a partial message and never counts toward <see cref="SubscriberCount"/>.
/// </para>
/// </remarks>
public sealed class TelegraphPublisher : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true,
    };

    private readonly TcpListener _listener;
    private readonly TelegraphFraming _framing;
    private readonly SslServerAuthenticationOptions? _sslOptions;
    private readonly List<ConnectedSubscriber> _clients = new List<ConnectedSubscriber>();
    private readonly object _clientsGate = new object();
    private CancellationTokenSource? _acceptLoopCancellation;
    private Task? _acceptLoopTask;
    private bool _disposed;

    /// <summary>
    /// Creates a publisher bound to <see cref="IPAddress.Any"/> on a local port, in plaintext,
    /// using <see cref="TelegraphFraming.NewlineDelimited"/> framing.
    /// </summary>
    /// <param name="port">The TCP port to listen on. Pass <c>0</c> to let the OS choose one; read it back from <see cref="Port"/> after <see cref="StartAsync(CancellationToken)"/>.</param>
    public TelegraphPublisher(int port)
        : this(IPAddress.Any, port, TelegraphFraming.NewlineDelimited)
    {
    }

    /// <summary>
    /// Creates a publisher bound to a specific local address and port, in plaintext, using
    /// <see cref="TelegraphFraming.NewlineDelimited"/> framing.
    /// </summary>
    /// <param name="bindAddress">
    /// The local address to bind, e.g. <see cref="IPAddress.Loopback"/> to keep the publisher off
    /// the network entirely, or one interface's address on a multi-homed host.
    /// </param>
    /// <param name="port">The TCP port to listen on. Pass <c>0</c> to let the OS choose one; read it back from <see cref="Port"/> after <see cref="StartAsync(CancellationToken)"/>.</param>
    public TelegraphPublisher(IPAddress bindAddress, int port)
        : this(bindAddress, port, TelegraphFraming.NewlineDelimited)
    {
    }

    /// <summary>Creates a publisher bound to <see cref="IPAddress.Any"/> on a local port, in plaintext, using the given wire framing.</summary>
    /// <param name="port">The TCP port to listen on. Pass <c>0</c> to let the OS choose one; read it back from <see cref="Port"/> after <see cref="StartAsync(CancellationToken)"/>.</param>
    /// <param name="framing">
    /// How messages are delimited on the wire. Every <see cref="TelegraphSubscriber"/> reading
    /// this publisher must be constructed with the same <see cref="TelegraphFraming"/> value --
    /// there is nothing on the wire that identifies which one is in use.
    /// </param>
    public TelegraphPublisher(int port, TelegraphFraming framing)
        : this(IPAddress.Any, port, framing)
    {
    }

    /// <summary>Creates a publisher bound to a specific local address and port, in plaintext, using the given wire framing.</summary>
    /// <param name="bindAddress">
    /// The local address to bind, e.g. <see cref="IPAddress.Loopback"/> to keep the publisher off
    /// the network entirely, or one interface's address on a multi-homed host.
    /// </param>
    /// <param name="port">The TCP port to listen on. Pass <c>0</c> to let the OS choose one; read it back from <see cref="Port"/> after <see cref="StartAsync(CancellationToken)"/>.</param>
    /// <param name="framing">
    /// How messages are delimited on the wire. Every <see cref="TelegraphSubscriber"/> reading
    /// this publisher must be constructed with the same <see cref="TelegraphFraming"/> value --
    /// there is nothing on the wire that identifies which one is in use.
    /// </param>
    public TelegraphPublisher(IPAddress bindAddress, int port, TelegraphFraming framing)
    {
        _listener = new TcpListener(bindAddress, port);
        _framing = framing;
    }

    /// <summary>
    /// Creates a publisher bound to <see cref="IPAddress.Any"/> on a local port, requiring TLS on
    /// every connection, using <see cref="TelegraphFraming.NewlineDelimited"/> framing.
    /// </summary>
    /// <param name="port">The TCP port to listen on. Pass <c>0</c> to let the OS choose one; read it back from <see cref="Port"/> after <see cref="StartAsync(CancellationToken)"/>.</param>
    /// <param name="sslOptions">
    /// Server-side TLS options (certificate, protocols, client-certificate requirements). See the
    /// remarks on <see cref="TelegraphPublisher"/> for exactly what completing this handshake
    /// does and doesn't guarantee before a connection joins the broadcast list.
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
    /// CIDR ranges a connection's remote address must fall within to be accepted. Empty (the
    /// default) means no restriction, matching the behaviour of every <see cref="TelegraphPublisher"/>
    /// before this property existed. Populate it (e.g. via an object initializer) before calling
    /// <see cref="StartAsync(CancellationToken)"/> -- the accept loop reads it without taking a
    /// lock, so mutating it concurrently with a running loop is not supported.
    /// </summary>
    public ICollection<IPNetwork> AllowedRanges { get; } = new List<IPNetwork>();

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

    /// <summary>
    /// How a subscriber whose write would otherwise block is treated by
    /// <see cref="Publish{T}(T)"/>. Defaults to <see cref="TelegraphBackpressurePolicy.BlockUntilDrained"/>,
    /// the behaviour every <see cref="TelegraphPublisher"/> had before this property existed.
    /// </summary>
    public TelegraphBackpressurePolicy BackpressurePolicy { get; set; } = TelegraphBackpressurePolicy.BlockUntilDrained;

    /// <summary>
    /// How long a write may block before the subscriber is disconnected, when
    /// <see cref="BackpressurePolicy"/> is <see cref="TelegraphBackpressurePolicy.DisconnectAfterTimeout"/>.
    /// Ignored for every other policy. Defaults to 30 seconds.
    /// </summary>
    public TimeSpan BackpressureTimeout { get; set; } = TimeSpan.FromSeconds(30);

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
    /// Serialises <paramref name="message"/> to one JSON line (or length-prefixed frame, depending
    /// on the constructor's <see cref="TelegraphFraming"/>) and writes it to every connected
    /// subscriber.
    /// </summary>
    /// <typeparam name="T">The message type. Any type <see cref="JsonSerializer"/> can serialise.</typeparam>
    /// <param name="message">The message.</param>
    /// <remarks>
    /// Writes synchronously and best-effort per subscriber: by default (<see cref="BackpressurePolicy"/>
    /// is <see cref="TelegraphBackpressurePolicy.BlockUntilDrained"/>), a subscriber whose socket
    /// buffer is full blocks this call until it drains or the write fails. Set
    /// <see cref="BackpressurePolicy"/> to change that: <see cref="TelegraphBackpressurePolicy.DropForSlowSubscriber"/>
    /// skips a subscriber whose buffer is already completely full rather than blocking, which
    /// greatly reduces but does not eliminate the chance of this call blocking on it (see the
    /// policy's own remarks), and <see cref="TelegraphBackpressurePolicy.DisconnectAfterTimeout"/>
    /// bounds how long a write may block before that subscriber is disconnected. A subscriber
    /// whose write throws (connection reset, buffer full past the policy's timeout) is dropped
    /// silently rather than taking every other subscriber down with it.
    /// </remarks>
    public void Publish<T>(T message)
    {
        byte[] bytes = _framing == TelegraphFraming.LengthPrefixed
            ? BuildLengthPrefixedFrame(message)
            : Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message, JsonOptions) + "\n");

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
                if (BackpressurePolicy == TelegraphBackpressurePolicy.DropForSlowSubscriber
                    && !subscriber.Client.Client.Poll(0, SelectMode.SelectWrite))
                {
                    continue;
                }

                // Set explicitly on every write, for every policy -- not only when entering
                // DisconnectAfterTimeout -- so a socket that had a finite SendTimeout from an
                // earlier policy gets it reset back to 0 (infinite) as soon as BackpressurePolicy
                // changes away from DisconnectAfterTimeout, rather than keeping a stale timeout.
                subscriber.Client.Client.SendTimeout = BackpressurePolicy == TelegraphBackpressurePolicy.DisconnectAfterTimeout
                    ? (int)Math.Clamp(BackpressureTimeout.TotalMilliseconds, 1, int.MaxValue)
                    : 0;

                subscriber.Stream.Write(bytes, 0, bytes.Length);
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
                subscriber.Dispose();
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

        match.Dispose();
        return true;
    }

    private static byte[] BuildLengthPrefixedFrame<T>(T message)
    {
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(message, JsonOptions);

        var frame = new byte[4 + payload.Length];
        BinaryPrimitives.WriteInt32BigEndian(frame, payload.Length);
        payload.CopyTo(frame, 4);
        return frame;
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

            bool allowed;
            try
            {
                allowed = IsAllowed(client);
            }
            catch (ObjectDisposedException)
            {
                // The connection dropped between being accepted and being checked (a port
                // scanner, a health check, a load-balancer probe) -- treat it the same as a
                // disallowed one rather than letting it escape and kill the accept loop.
                allowed = false;
            }
            catch (SocketException)
            {
                allowed = false;
            }

            if (!allowed)
            {
                client.Dispose();
                continue;
            }

            if (_sslOptions != null)
            {
                // The TLS handshake runs on its own task rather than inline here, so a connection
                // that stalls partway through it -- or never completes it at all -- cannot block
                // this loop from accepting anyone else.
                _ = CompleteTlsHandshakeAndRegisterAsync(client, cancellationToken);
                continue;
            }

            // Same defensive handling as the IsAllowed check above: a client that connects and
            // drops immediately can make RemoteEndPoint throw here too, and that must not escape
            // this loop either.
            try
            {
                var info = new TelegraphSubscriberInfo((IPEndPoint)client.Client.RemoteEndPoint!, DateTimeOffset.UtcNow);
                lock (_clientsGate)
                {
                    _clients.Add(new ConnectedSubscriber(client, client.GetStream(), info));
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

    private bool IsAllowed(TcpClient client)
    {
        if (AllowedRanges.Count == 0)
        {
            return true;
        }

        var remoteEndPoint = (IPEndPoint)client.Client.RemoteEndPoint!;
        foreach (IPNetwork range in AllowedRanges)
        {
            if (range.Contains(remoteEndPoint.Address))
            {
                return true;
            }
        }

        return false;
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

        // Same defensive handling as the plaintext path in AcceptLoopAsync: the handshake having
        // just completed doesn't guarantee the socket is still usable by the time we get here.
        try
        {
            var info = new TelegraphSubscriberInfo((IPEndPoint)client.Client.RemoteEndPoint!, DateTimeOffset.UtcNow);
            lock (_clientsGate)
            {
                _clients.Add(new ConnectedSubscriber(client, sslStream, info));
            }
        }
        catch (ObjectDisposedException)
        {
            sslStream.Dispose();
            client.Dispose();
        }
        catch (SocketException)
        {
            sslStream.Dispose();
            client.Dispose();
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
            subscriber.Dispose();
        }

        _acceptLoopCancellation?.Dispose();
    }

    private sealed class ConnectedSubscriber : IDisposable
    {
        public ConnectedSubscriber(TcpClient client, Stream stream, TelegraphSubscriberInfo info)
        {
            Client = client;
            Stream = stream;
            Info = info;
        }

        public TcpClient Client { get; }

        public Stream Stream { get; }

        public TelegraphSubscriberInfo Info { get; }

        public void Dispose()
        {
            Stream.Dispose();
            Client.Dispose();
        }
    }
}
