package util;

import java.nio.ByteBuffer;

public class ByteUtils {
    private static ByteBuffer buffer;

    public static byte[] longToBytes(long x) {
        buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(0, x);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
        buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes, 0, bytes.length);
        buffer.flip();  // need flip
        return buffer.getLong();
    }

    public static String bytesToHexStr(byte[] bytes) {
        StringBuilder buf = new StringBuilder(bytes.length * 2);
        int counter = 0;
        for(byte b : bytes) {
            buf.append(String.format("%02x", b & 0xff));
            ++counter;
            if (counter % 16 == 0 && counter < bytes.length)
                buf.append("\n");
            else
                buf.append(" ");
        }
        return buf.toString();
    }
}
