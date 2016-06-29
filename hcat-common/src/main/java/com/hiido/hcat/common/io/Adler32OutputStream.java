/**
 * May 16, 2013
 */
package com.hiido.hcat.common.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Adler32;
import java.util.zip.CRC32;

/**
 * @author lin
 * 
 */
public final class Adler32OutputStream extends ChecksumOutputStream<Long> {
    public static final String ALG = "adler32";
    private final Adler32 adler32 = new Adler32();

    public Adler32OutputStream(OutputStream out) {
        super(out);
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
        adler32.update(b);
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
        out.write(b, off, len);
        adler32.update(b, off, len);
    }

    @Override
    public Long getChecksum() {
        return adler32.getValue();
    }
}
