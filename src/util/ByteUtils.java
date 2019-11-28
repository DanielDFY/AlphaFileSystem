package util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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

    public static byte[] objectToSerialize(Object obj) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream;
        objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(obj);
        byte[] bytes = byteArrayOutputStream.toByteArray();
        objectOutputStream.close();
        byteArrayOutputStream.close();
        return bytes;
    }

    public static Object serializeToObject(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        Object object = objectInputStream.readObject();
        objectInputStream.close();
        byteArrayInputStream.close();
        return object;
    }
}
