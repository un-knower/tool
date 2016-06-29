/**
 * May 17, 2013
 */
package com.hiido.hcat.common.io;

import java.io.OutputStream;

/**
 * @author lin
 * 
 */
public final class ChecksumHelper {
    private ChecksumHelper() {
    }

    public static OutputStream createCheckedOutputStream(String alg, OutputStream out) {
        if (alg == null) {
            return out;
        }
        if (alg.equals(Md5OutputStream.ALG)) {
            return new Md5OutputStream(out);
        } else if (alg.equals(Adler32OutputStream.ALG)) {
            return new Adler32OutputStream(out);
        } else if (alg.equals(CRC32OutputStream.ALG)) {
            return new CRC32OutputStream(out);
        }
        throw new IllegalArgumentException("not supported checksum alg:" + alg);
    }

    public static String getChecksum(OutputStream out) {
        if (out instanceof ChecksumOutputStream) {
            return String.valueOf(((ChecksumOutputStream) out).getChecksum());
        }
        return null;
    }
}
