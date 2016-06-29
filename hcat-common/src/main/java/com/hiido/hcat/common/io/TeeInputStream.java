/**
 * Jul 8, 2013
 */
package com.hiido.hcat.common.io;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author lin
 * 
 */
public final class TeeInputStream extends InputStream {
    private final InputStream in1;
    private final InputStream in2;
    private boolean part = true;

    public TeeInputStream(InputStream in1, InputStream in2) {
        this.in1 = in1;
        this.in2 = in2;
    }

    @Override
    public int read() throws IOException {
        int i = 0;
        if (part) {
            i = in1.read();
            if (i == -1) {
                part = false;
                i = in2.read();
            }
        } else {
            i = in2.read();
        }
        return i;
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
        int i = 0;
        if (part) {
            i = in1.read(b, off, len);
            if (i == -1) {
                part = false;
                i = in2.read(b, off, len);
            }
        } else {
            i = in2.read(b, off, len);
        }
        return i;
    }

}
