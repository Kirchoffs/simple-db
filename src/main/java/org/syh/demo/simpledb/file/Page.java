package org.syh.demo.simpledb.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {
    public static Charset CHARSET = StandardCharsets.US_ASCII;

    private ByteBuffer buffer;

    public Page(int blockSize) {
        buffer = ByteBuffer.allocateDirect(blockSize);
    }

    public Page(byte[] bytes) {
        buffer = ByteBuffer.wrap(bytes);
    }

    public int getInt(int offset) {
        return buffer.getInt(offset);
    }

    public void setInt(int offset, int val) {
        buffer.putInt(offset, val);
    }

    public int getFirstInt() {
        return getInt(0);
    }

    public void setFirstInt(int val) {
        setInt(0, val);
    }

    public byte[] getBytes(int offset) {
        buffer.position(offset);
        int length = buffer.getInt();
        byte[] val = new byte[length];
        buffer.get(val);
        return val;
    }

    public void setBytes(int offset, byte[] val) {
        buffer.position(offset);
        buffer.putInt(val.length);
        buffer.put(val);
    }

    public String getString(int offset) {
        byte[] bytes = getBytes(offset);
        return new String(bytes, CHARSET);
    }

    public void setString(int offset, String val) {
        byte[] bytes = val.getBytes(CHARSET);
        setBytes(offset, bytes);
    }

    public static int maxLength(int strlen) {
        int bytesPerChar = (int) CHARSET.newEncoder().maxBytesPerChar();
        return Integer.BYTES + strlen * bytesPerChar;
    }

    ByteBuffer contents() {
        buffer.position(0);
        return buffer;
    }
}
