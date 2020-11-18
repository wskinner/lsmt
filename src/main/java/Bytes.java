public class Bytes {
    public static byte[] longToBytes(long lng, boolean littleEndian) {
        if (littleEndian)
            return new byte[]{
                    (byte) lng,
                    (byte) (lng >> 8),
                    (byte) (lng >> 16),
                    (byte) (lng >> 24),
                    (byte) (lng >> 32),
                    (byte) (lng >> 40),
                    (byte) (lng >> 48),
                    (byte) (lng >> 56)};
        else
            return new byte[]{
                    (byte) (lng >> 56),
                    (byte) (lng >> 48),
                    (byte) (lng >> 40),
                    (byte) (lng >> 32),
                    (byte) (lng >> 24),
                    (byte) (lng >> 16),
                    (byte) (lng >> 8),
                    (byte) lng};
    }

    public static long bytesToLong(final byte[] b, boolean littleEndian) {
        if (littleEndian) {
            return ((long) b[7] << 56)
                    | ((long) b[6] & 0xff) << 48
                    | ((long) b[5] & 0xff) << 40
                    | ((long) b[4] & 0xff) << 32
                    | ((long) b[3] & 0xff) << 24
                    | ((long) b[2] & 0xff) << 16
                    | ((long) b[1] & 0xff) << 8
                    | ((long) b[0] & 0xff);
        } else {
            return ((long) b[0] << 56)
                    | ((long) b[1] & 0xff) << 48
                    | ((long) b[2] & 0xff) << 40
                    | ((long) b[3] & 0xff) << 32
                    | ((long) b[4] & 0xff) << 24
                    | ((long) b[5] & 0xff) << 16
                    | ((long) b[6] & 0xff) << 8
                    | ((long) b[7] & 0xff);
        }
    }

    public static byte[] intToBytes(int it, boolean littleEndian) {
        if (littleEndian)
            return new byte[]{
                    (byte) it,
                    (byte) (it >> 8),
                    (byte) (it >> 16),
                    (byte) (it >> 24)
            };
        else
            return new byte[]{
                    (byte) (it >> 24),
                    (byte) (it >> 16),
                    (byte) (it >> 8),
                    (byte) it
            };
    }

    public static int bytesToInt(final byte[] b, boolean littleEndian) {
        if (littleEndian)
            return ((int) b[3] & 0xff) << 24
                    | ((int) b[2] & 0xff) << 16
                    | ((int) b[1] & 0xff) << 8
                    | ((int) b[0] & 0xff);
        else
            return ((int) b[0] & 0xff) << 24
                    | ((int) b[1] & 0xff) << 16
                    | ((int) b[2] & 0xff) << 8
                    | ((int) b[3] & 0xff);
    }

    public static byte[] floatToBytes(float it, boolean littleEndian) {
        return intToBytes(Float.floatToIntBits(it), littleEndian);
    }

    public static float bytesToFloat(final byte[] b, boolean littleEndian) {
        return Float.intBitsToFloat(bytesToInt(b, littleEndian));
    }

    public static byte[] doubleToBytes(double it, boolean littleEndian) {
        return longToBytes(Double.doubleToLongBits(it), littleEndian);
    }

    public static double bytesToDouble(final byte[] b, boolean littleEndian) {
        return Double.longBitsToDouble(bytesToLong(b, littleEndian));
    }
}
