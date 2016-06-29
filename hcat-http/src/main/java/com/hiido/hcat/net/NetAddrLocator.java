/**
 * Aug 15, 2012
 */
package com.hiido.hcat.net;

import java.io.Closeable;

import com.hiido.hcat.common.err.ErrCodeException;

public interface NetAddrLocator extends Closeable {
    public enum Event {
        Err, Modify
    }

    interface MonitorCallback {
        void callback(Event event);
    }

    // could not return null
    NetAddr query(String name) throws ErrCodeException;

    void register(String name, MonitorCallback callbck);

    void close();
}
