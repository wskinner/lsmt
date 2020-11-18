public class Bytes {
    public static byte[] longToBytes(long lng) {
        return new byte[]{
                (byte) lng,
                (byte) (lng >> 8),
                (byte) (lng >> 16),
                (byte) (lng >> 24),
                (byte) (lng >> 32),
                (byte) (lng >> 40),
                (byte) (lng >> 48),
                (byte) (lng >> 56)};
    }

    public static long bytesToLong(final byte[] b) {
        return ((long) b[7] << 56)
                | ((long) b[6] & 0xff) << 48
                | ((long) b[5] & 0xff) << 40
                | ((long) b[4] & 0xff) << 32
                | ((long) b[3] & 0xff) << 24
                | ((long) b[2] & 0xff) << 16
                | ((long) b[1] & 0xff) << 8
                | ((long) b[0] & 0xff);
    }

    public static byte[] intToBytes(int it) {
        return new byte[]{
                (byte) it,
                (byte) (it >> 8),
                (byte) (it >> 16),
                (byte) (it >> 24)
        };
    }

    public static int bytesToInt(final byte[] b) {
        return ((int) b[3] & 0xff) << 24
                | ((int) b[2] & 0xff) << 16
                | ((int) b[1] & 0xff) << 8
                | ((int) b[0] & 0xff);
    }

    public static byte[] floatToBytes(float it) {
        return intToBytes(Float.floatToIntBits(it));
    }

    public static float bytesToFloat(final byte[] b) {
        return Float.intBitsToFloat(((int) b[3] & 0xff) << 24
                | ((int) b[2] & 0xff) << 16
                | ((int) b[1] & 0xff) << 8
                | ((int) b[0] & 0xff));
    }

    public static byte[] doubleToBytes(double it) {
        return longToBytes(Double.doubleToLongBits(it));
    }

    public static double bytesToDouble(final byte[] b) {
        return Double.longBitsToDouble(((int) b[3] & 0xff) << 24
                | ((int) b[2] & 0xff) << 16
                | ((int) b[1] & 0xff) << 8
                | ((int) b[0] & 0xff));
    }
}
