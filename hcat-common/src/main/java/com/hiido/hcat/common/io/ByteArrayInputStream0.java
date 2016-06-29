package com.hiido.hcat.common.io;

import java.io.ByteArrayInputStream;

public class ByteArrayInputStream0 extends ByteArrayInputStream {

    public ByteArrayInputStream0(byte[] buf) {
        super(buf);
    }

    public ByteArrayInputStream0(byte buf[], int offset, int length) {
        super(buf, offset, length);
    }

    public byte[] getByteArray() {
        return buf;
    }

    public void limit(int count) {
        this.count = count;
    }
}
