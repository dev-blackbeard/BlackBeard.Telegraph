namespace Telegraph;

/// <summary>
/// An optional, ready-made 6DOF payload shape: position, attitude, linear and angular velocity.
/// </summary>
/// <remarks>
/// <para>
/// This is one opt-in convenience on top of a transport that otherwise carries any JSON-
/// serialisable shape a caller chooses — nothing in <see cref="TelegraphPublisher"/> or
/// <see cref="TelegraphSubscriber"/> requires it. It exists because 6DOF-plus-metadata is common
/// enough to want out of the box, and because the frame, units and rotation representation are
/// exactly the kind of thing that should be decided once, explicitly, rather than reinvented per
/// consumer. Those decisions, stated once:
/// </para>
/// <list type="bullet">
/// <item><description>Position is geodetic WGS84: <see cref="LatitudeDegrees"/>/<see cref="LongitudeDegrees"/>
/// in degrees, <see cref="AltitudeMeters"/> in metres above the reference ellipsoid.</description></item>
/// <item><description>Attitude is a unit quaternion (<see cref="QuaternionX"/>/<see cref="QuaternionY"/>/
/// <see cref="QuaternionZ"/>/<see cref="QuaternionW"/>), the primary representation, with roll/pitch/yaw
/// in degrees carried alongside as a convenience for a reader who does not want to do the conversion.</description></item>
/// <item><description>Linear velocity is local North/East/Down, in metres per second.</description></item>
/// <item><description>Angular velocity is body-frame X/Y/Z, in degrees per second.</description></item>
/// </list>
/// <para>
/// Every field is nullable, and an unsupplied field is <c>null</c>, never <c>0</c> — adopted here
/// as a deliberate convention, not because this package enforces it, because it is what keeps a
/// downstream mapping to a richer domain type a field-for-field copy instead of a guess about
/// whether a zero was measured or missing.
/// </para>
/// </remarks>
public sealed class Pose6Dof
{
    /// <summary>Latitude in degrees, positive north, WGS84.</summary>
    public double? LatitudeDegrees { get; set; }

    /// <summary>Longitude in degrees, positive east, WGS84.</summary>
    public double? LongitudeDegrees { get; set; }

    /// <summary>Altitude in metres above the WGS84 reference ellipsoid.</summary>
    public double? AltitudeMeters { get; set; }

    /// <summary>Quaternion X component. Primary attitude representation, alongside Y/Z/W.</summary>
    public double? QuaternionX { get; set; }

    /// <summary>Quaternion Y component.</summary>
    public double? QuaternionY { get; set; }

    /// <summary>Quaternion Z component.</summary>
    public double? QuaternionZ { get; set; }

    /// <summary>Quaternion W component.</summary>
    public double? QuaternionW { get; set; }

    /// <summary>Roll in degrees. A convenience alongside the quaternion, not a replacement for it.</summary>
    public double? RollDegrees { get; set; }

    /// <summary>Pitch in degrees.</summary>
    public double? PitchDegrees { get; set; }

    /// <summary>Yaw in degrees.</summary>
    public double? YawDegrees { get; set; }

    /// <summary>Linear velocity along local north, in metres per second.</summary>
    public double? VelocityNorthMetersPerSecond { get; set; }

    /// <summary>Linear velocity along local east, in metres per second.</summary>
    public double? VelocityEastMetersPerSecond { get; set; }

    /// <summary>Linear velocity along local down, in metres per second.</summary>
    public double? VelocityDownMetersPerSecond { get; set; }

    /// <summary>Angular velocity about the body X axis, in degrees per second.</summary>
    public double? AngularVelocityXDegreesPerSecond { get; set; }

    /// <summary>Angular velocity about the body Y axis, in degrees per second.</summary>
    public double? AngularVelocityYDegreesPerSecond { get; set; }

    /// <summary>Angular velocity about the body Z axis, in degrees per second.</summary>
    public double? AngularVelocityZDegreesPerSecond { get; set; }
}
