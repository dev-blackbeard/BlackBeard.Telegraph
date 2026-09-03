namespace Telegraph;

/// <summary>
/// How <see cref="TelegraphPublisher"/> and <see cref="TelegraphSubscriber"/> delimit one message
/// from the next on the wire.
/// </summary>
public enum TelegraphFraming
{
    /// <summary>
    /// One JSON object per line, terminated by <c>\n</c>. The default, and the wire format
    /// documented in the README as inspectable with <c>nc</c>. Not safe for a message whose
    /// serialised JSON can itself contain a literal <c>\n</c> inside a string value -- depending
    /// on <see cref="System.Text.Json.JsonSerializerOptions"/>, that is not always escaped away,
    /// and when it isn't, it corrupts the line framing for every subscriber on the connection, not
    /// just the one message. Use <see cref="LengthPrefixed"/> instead if that is a real
    /// possibility for the message types being sent.
    /// </summary>
    NewlineDelimited,

    /// <summary>
    /// Each message is a 4-byte big-endian length prefix followed by that many bytes of UTF-8
    /// JSON -- no newline involved, so a <c>\n</c> anywhere inside the JSON is no longer special.
    /// Not inspectable with a plain-text tool like <c>nc</c> the way <see cref="NewlineDelimited"/>
    /// is.
    /// </summary>
    LengthPrefixed,
}
