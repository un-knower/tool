/**
 * Jul 8, 2013
 */
package com.hiido.hcat.common.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author lin
 * 
 */
public final class LimitInputStream extends FilterInputStream {
    private final long limit;
    private long read;

    public LimitInputStream(long limit, InputStream in) {
        super(in);
        this.limit = limit;
    }

    public long getRead() {
        return read;
    }

    private void checkLimit(long read) throws IOException {
        if ((read) > limit) {
            throw new IOException(String.format("reach limit[%d]:%d", limit, read));
        }
    }

    @Override
    public int read() throws IOException {
        int i = super.read();
        if (i == -1) {
            return i;
        }
        checkLimit(++read);
        return i;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        int i = in.read(b, off, len);
        if (i == -1) {
            return i;
        }
        read += i;
        checkLimit(read);
        return i;
    }

}
