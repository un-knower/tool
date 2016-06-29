package com.hiido.hcat.http.server;

public interface JettyConstants {
    String FILTER_INITIALIZER_PROPERTY = "hiido.http.filter.initializers";
    String k_jetty_lowResourceMaxIdleTime = "jetty.lowResourceMaxIdleTime";
    int v_jetty_lowResourceMaxIdleTime = 20 * 1000;

    String k_jetty_lowResourcesConnections = "jetty.LowResourcesConnections";
    int v_jetty_LowResourcesConnections = 200;

    String k_jetty_acceptQueueSize = "jetty.acceptQueueSize";
    int v_jetty_acceptQueueSize = 100;

    String k_jetty_maxIdleTime = "jetty.maxIdleTime";
    int v_jetty_maxIdleTime = 1000 * 60;

    String k_jetty_maxThreads = "jetty.maxThreads";
    int v_jetty_maxThreads = 256;

    String k_jetty_minThreads = "jetty.minThreads";
    int v_jetty_minThreads = 16;

    String k_jetty_cipherSuits = "jetty.cipherSuits";

    public enum ConnectorType {
        NIOSelect, NIOBlocking, BIOSocket
    }
}
