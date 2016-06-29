/**
 * Nov 21, 2012
 */
package com.hiido.hcat.net;

import com.hiido.hcat.common.err.ErrCodeException;

public class StaticNetAddrLocator implements NetAddrLocator {
    private String host;
    private int port;

    @Override
    public NetAddr query(String name) throws ErrCodeException {
        return NetAddr.valueOf(host, port);
    }

    @Override
    public void register(String name, MonitorCallback callbck) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

}
