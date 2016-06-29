/**
 * May 22, 2013
 */
package com.hiido.hcat.common.io;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author lin
 * 
 */
public final class TeeOutputStream extends OutputStream {

    private final OutputStream out1;
    private final OutputStream out2;

    public TeeOutputStream(OutputStream out1, OutputStream out2) {
        this.out1 = out1;
        this.out2 = out2;
    }

    @Override
    public void write(byte[] b) throws IOException {
        if (out1 != null)
            this.out1.write(b);
        if (out2 != null)
            this.out2.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (out1 != null)
            this.out1.write(b, off, len);
        if (out2 != null)
            this.out2.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
        if (out1 != null)
            this.out1.write(b);
        if (out2 != null)
            this.out2.write(b);
    }
}
