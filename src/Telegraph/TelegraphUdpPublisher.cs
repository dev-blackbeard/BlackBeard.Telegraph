using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Telegraph;

/// <summary>
/// Broadcasts messages to every subscriber that has registered with a <see cref="TelegraphUdpSubscriber"/>,
/// as one JSON object per UDP datagram.
/// </summary>
/// <remarks>
/// <para>
/// This is the UDP counterpart to <see cref="TelegraphPublisher"/>, for the cases TCP is the
/// wrong trade-off: high-rate telemetry where a dropped packet is preferable to head-of-line
/// blocking, or a subscriber that only cares about the latest value and would rather skip a stale
/// one than wait for it. It carries none of TCP's guarantees -- no reliability, no ordering, no
/// delivery confirmation -- by design; reach for <see cref="TelegraphPublisher"/> instead if any
/// of those matter. It sits next to <see cref="TelegraphPublisher"/>, not in place of it.
/// </para>
/// <para>
/// A <see cref="TelegraphUdpSubscriber"/> registers by sending one (otherwise empty) datagram
/// when it connects; this type records the sender's address and port and broadcasts every
/// subsequent <see cref="Publish{T}(T)"/> to it. There is no un-registration -- a subscriber that
/// stops listening is simply never heard from again, and datagrams sent to it are dropped by the
/// network the same way any other unreachable UDP peer's would be.
/// </para>
/// </remarks>
public sealed class TelegraphUdpPublisher : IDisposable
{
    /// <summary>
    /// The largest serialised message <see cref="Publish{T}(T)"/> will send. Matches the common
    /// 1500-byte Ethernet MTU minus the IPv4 and UDP headers (1500 - 20 - 8), so a datagram at or
    /// under this size does not need IP fragmentation on a typical local network.
    /// <see cref="Publish{T}(T)"/> throws rather than send anything larger, since sending it would
    /// mean the OS silently fragments it across multiple IP packets instead -- fine on some
    /// paths, but not something this type assumes about a network it has no visibility into.
    /// </summary>
    public const int MaxDatagramSize = 1472;

    private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true,
    };

    private readonly UdpClient _udpClient;
    private readonly HashSet<IPEndPoint> _subscribers = new HashSet<IPEndPoint>();
    private readonly object _subscribersGate = new object();
    private CancellationTokenSource? _listenLoopCancellation;
    private Task? _listenLoopTask;
    private bool _disposed;

    /// <summary>Creates a publisher bound to a local port.</summary>
    /// <param name="port">The UDP port to listen on. Pass <c>0</c> to let the OS choose one; read it back from <see cref="Port"/>.</param>
    public TelegraphUdpPublisher(int port)
    {
        _udpClient = new UdpClient(port);
    }

    /// <summary>The port actually bound.</summary>
    public int Port
    {
        get { return ((IPEndPoint)_udpClient.Client.LocalEndPoint!).Port; }
    }

    /// <summary>How many subscribers have registered.</summary>
    public int SubscriberCount
    {
        get
        {
            lock (_subscribersGate)
            {
                return _subscribers.Count;
            }
        }
    }

    /// <summary>Starts listening for subscriber registrations.</summary>
    /// <param name="cancellationToken">Stops listening for new registrations when cancelled. Already-registered subscribers are unaffected.</param>
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        _listenLoopCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _listenLoopTask = ListenLoopAsync(_listenLoopCancellation.Token);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Serialises <paramref name="message"/> to one JSON datagram and sends it to every registered
    /// subscriber.
    /// </summary>
    /// <typeparam name="T">The message type. Any type <see cref="JsonSerializer"/> can serialise.</typeparam>
    /// <param name="message">The message.</param>
    /// <exception cref="ArgumentException">The serialised message is larger than <see cref="MaxDatagramSize"/>.</exception>
    /// <remarks>
    /// Best-effort per subscriber and fire-and-forget, with none of
    /// <see cref="TelegraphPublisher.Publish{T}(T)"/>'s delivery guarantees: a datagram can be
    /// silently dropped by the network, delivered out of order, or not delivered at all, and this
    /// call has no way to know which. A send that fails immediately and locally (e.g. an ICMP
    /// port-unreachable for a subscriber process that has since exited) unregisters that
    /// subscriber; anything less immediate than that is invisible to this type, same as it would
    /// be to any other UDP sender.
    /// </remarks>
    public void Publish<T>(T message)
    {
        byte[] bytes = JsonSerializer.SerializeToUtf8Bytes(message, JsonOptions);
        if (bytes.Length > MaxDatagramSize)
        {
            throw new ArgumentException(
                $"Serialised message is {bytes.Length} bytes, over the {MaxDatagramSize}-byte single-packet limit -- {nameof(TelegraphUdpPublisher)} fails fast rather than let it be silently fragmented.",
                nameof(message));
        }

        List<IPEndPoint> snapshot;
        lock (_subscribersGate)
        {
            snapshot = new List<IPEndPoint>(_subscribers);
        }

        List<IPEndPoint>? dead = null;
        foreach (IPEndPoint subscriber in snapshot)
        {
            try
            {
                _udpClient.Send(bytes, bytes.Length, subscriber);
            }
            catch (SocketException)
            {
                dead ??= new List<IPEndPoint>();
                dead.Add(subscriber);
            }
            catch (ObjectDisposedException)
            {
                dead ??= new List<IPEndPoint>();
                dead.Add(subscriber);
            }
        }

        if (dead != null)
        {
            lock (_subscribersGate)
            {
                foreach (IPEndPoint subscriber in dead)
                {
                    _subscribers.Remove(subscriber);
                }
            }
        }
    }

    private async Task ListenLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            UdpReceiveResult result;
            try
            {
                result = await _udpClient.ReceiveAsync(cancellationToken).ConfigureAwait(false);
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
                // Unlike OperationCanceledException/ObjectDisposedException, this does not mean
                // the socket is gone -- a connectionless UDP socket can surface an unrelated
                // ICMP port-unreachable (e.g. bounced back from a subscriber process that has
                // since exited) as a SocketException on a later, otherwise-healthy receive call,
                // most commonly on Windows. Treating that as fatal would permanently stop this
                // publisher from hearing any further registrations over one stale peer, so it is
                // skipped rather than allowed to end the loop.
                continue;
            }

            lock (_subscribersGate)
            {
                _subscribers.Add(result.RemoteEndPoint);
            }
        }
    }

    /// <summary>Stops listening and releases the port.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        _listenLoopCancellation?.Cancel();
        _udpClient.Dispose();

        lock (_subscribersGate)
        {
            _subscribers.Clear();
        }

        _listenLoopCancellation?.Dispose();
    }
}
