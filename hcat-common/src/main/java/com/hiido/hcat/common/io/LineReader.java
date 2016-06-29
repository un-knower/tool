package com.hiido.hcat.common.io;

import java.io.Closeable;
import java.io.IOException;

public interface LineReader extends Closeable {
    void open() throws IOException;

    String readLine() throws IOException;

    void close();
}
