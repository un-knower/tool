/**
 * Aug 15, 2012
 */
package com.hiido.hcat.net;

public final class NetAddr {
    private final String host;
    private final int port;

    public NetAddr(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public static NetAddr valueOf(String host, int port) {
        return new NetAddr(host, port);
    }

    public static NetAddr valueOf(String s) {
        String[] ss = s.split(":");
        if (ss.length != 2) {
            throw new IllegalArgumentException("illegal format,must be host:port");
        }
        return new NetAddr(ss[0], Integer.valueOf(ss[1]));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof NetAddr)) {
            return false;
        }

        NetAddr that = (NetAddr) obj;
        return that.host.equals(host) && that.port == port;
    }

    @Override
    public int hashCode() {
        return host.hashCode() + port;
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
