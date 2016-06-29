/**
 * May 16, 2013
 */
package com.hiido.hcat.common.io;

import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.hiido.hcat.common.util.StringUtils;

/**
 * @author lin
 * 
 */
public final class Md5OutputStream extends ChecksumOutputStream<String> {
    public static final String ALG = "md5";
    private final MessageDigest md;

    public Md5OutputStream(OutputStream out) {
        super(out);
        try {
            md = java.security.MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e1) {
            throw new RuntimeException(e1);
        }
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
        md.update((byte) b);
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
        out.write(b, off, len);
        md.update(b, off, len);
    }

    @Override
    public String getChecksum() {
        byte[] tmp = md.digest();
        return StringUtils.toHex(tmp);
    }

}
