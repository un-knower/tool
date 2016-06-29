/**
 * Mar 20, 2013
 */
package com.hiido.hcat.common.io;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;

/**
 * @author lin
 * 
 */
public class ByteArrayOutputStream0 extends ByteArrayOutputStream {
    public ByteArrayOutputStream0() {
        super(32);
    }

    public ByteArrayOutputStream0(int size) {
        super(size);
    }

    public byte[] getByteArray() {
        return buf;
    }

    public int capacity() {
        return buf.length;
    }

    public int remain() {
        return buf.length - size();
    }

    public String toString(Charset charset) {
        return new String(buf, 0, size(), charset);
    }
}
