/**
 * Jul 27, 2012
 */
package com.hiido.hcat.common.io;

import java.io.BufferedReader;
import java.io.IOException;

import com.hiido.hcat.common.util.IOUtils;

/**
 * @author lin
 * 
 */
public abstract class AbstractLineReader implements LineReader {

    private static final int DEF_BUF_SIZE = 1024 * 512;

    protected int bufSize = DEF_BUF_SIZE;
    protected String charset = "utf-8";

    protected BufferedReader reader;

    protected AbstractLineReader() {
    }

    protected AbstractLineReader(String charset) {
        this.charset = charset;
    }

    @Override
    public String readLine() throws IOException {
        return reader.readLine();
    }

    @Override
    public void close() {
        IOUtils.closeIO(reader);
    }

    public int getBufSize() {
        return bufSize;
    }

    public void setBufSize(int bufSize) {
        this.bufSize = bufSize;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

}
