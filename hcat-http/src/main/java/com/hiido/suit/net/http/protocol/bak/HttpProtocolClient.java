/**
 * Oct 16, 2012
 */
package com.hiido.suit.net.http.protocol.bak;

import java.io.Closeable;

import com.hiido.hcat.common.err.ErrCodeException;

public interface HttpProtocolClient extends Closeable {
    int getConnectTimeout();

    int getReadTimeout();

    void setConnectTimeout(int time);

    void setReadTimeout(int time);

    boolean isKeepAlive();

    void setKeepAlive(boolean keepAlive);

    <T extends ProtocolReply> T post(String url, HttpProtocol protocol, Class<T> clazz) throws ErrCodeException;

    <T extends ProtocolReply> T post(String host, int port, HttpProtocol protocol, Class<T> clazz)
            throws ErrCodeException;

    <T extends ProtocolReply> T postSSL(String host, int port, HttpProtocol protocol, Class<T> clazz)
            throws ErrCodeException;

    String post(String host, int port, String path, String content) throws ErrCodeException;

    String postSSL(String host, int port, String path, String content) throws ErrCodeException;

    String post(String url, String content) throws ErrCodeException;

    String get(String url) throws ErrCodeException;

    @Override
    void close();

    <T extends ProtocolReply> T post(String host, String clientAddr, int port, HttpProtocol protocol, Class<T> clazz) throws ErrCodeException;

    <T extends ProtocolReply> T post(String url, String clientAddr, HttpProtocol protocol, Class<T> clazz) throws ErrCodeException;

    <T extends ProtocolReply> T postForWeb(String host, String clientAddr, int port, HttpProtocol protocol, Class<T> clazz) throws ErrCodeException;
}
