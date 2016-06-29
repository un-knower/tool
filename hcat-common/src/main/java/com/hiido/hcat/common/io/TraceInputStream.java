/**
 * Aug 2, 2013
 */
package com.hiido.hcat.common.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author lin
 * 
 */
public class TraceInputStream extends FilterInputStream {
    private final ByteArrayOutputStream0 byteArray;

    public TraceInputStream(InputStream in, int size) {
        super(in);
        this.byteArray = new ByteArrayOutputStream0(size);
    }

    public ByteArrayOutputStream0 track() {
        return byteArray;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        int r = in.read(b, off, len);
        if (r != 0) {
            byteArray.write(b, off, r);
        }
        return r;
    }

    @Override
    public int read() throws IOException {
        int r = in.read();
        if (r != -1) {
            byteArray.write(r);
        }
        return r;
    }
}
