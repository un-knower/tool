/**
 * May 16, 2013
 */
package com.hiido.hcat.common.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.CRC32;

/**
 * @author lin
 * 
 */
public final class CRC32OutputStream extends ChecksumOutputStream<Long> {
    public static final String ALG = "crc32";
    private final CRC32 crc32 = new CRC32();

    public CRC32OutputStream(OutputStream out) {
        super(out);
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
        crc32.update(b);
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
        out.write(b, off, len);
        crc32.update(b, off, len);
    }

    @Override
    public Long getChecksum() {
        return crc32.getValue();
    }
}
