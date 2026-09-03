using System;
using System.Buffers.Binary;
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
/// </remarks>
public sealed class TelegraphPublisher : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true,
    };

    private readonly TcpListener _listener;
    private readonly TelegraphFraming _framing;
    private readonly List<TcpClient> _clients = new List<TcpClient>();
    private readonly object _clientsGate = new object();
    private CancellationTokenSource? _acceptLoopCancellation;
    private Task? _acceptLoopTask;
    private bool _disposed;

    /// <summary>Creates a publisher bound to a local port, using <see cref="TelegraphFraming.NewlineDelimited"/> framing.</summary>
    /// <param name="port">The TCP port to listen on. Pass <c>0</c> to let the OS choose one; read it back from <see cref="Port"/> after <see cref="StartAsync(CancellationToken)"/>.</param>
    public TelegraphPublisher(int port)
        : this(port, TelegraphFraming.NewlineDelimited)
    {
    }

    /// <summary>Creates a publisher bound to a local port, using the given wire framing.</summary>
    /// <param name="port">The TCP port to listen on. Pass <c>0</c> to let the OS choose one; read it back from <see cref="Port"/> after <see cref="StartAsync(CancellationToken)"/>.</param>
    /// <param name="framing">
    /// How messages are delimited on the wire. Every <see cref="TelegraphSubscriber"/> reading
    /// this publisher must be constructed with the same <see cref="TelegraphFraming"/> value --
    /// there is nothing on the wire that identifies which one is in use.
    /// </param>
    public TelegraphPublisher(int port, TelegraphFraming framing)
    {
        _listener = new TcpListener(IPAddress.Any, port);
        _framing = framing;
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
        byte[] bytes = _framing == TelegraphFraming.LengthPrefixed
            ? BuildLengthPrefixedFrame(message)
            : Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message, JsonOptions) + "\n");

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

            lock (_clientsGate)
            {
                _clients.Add(client);
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
