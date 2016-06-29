package com.hiido.hcat.http.server;

import org.apache.hadoop.conf.Configuration;
import org.eclipse.jetty.server.AbstractConnector;
import org.eclipse.jetty.server.bio.SocketConnector;
import org.eclipse.jetty.server.nio.AbstractNIOConnector;
import org.eclipse.jetty.server.nio.BlockingChannelConnector;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSocketConnector;

import com.hiido.hcat.common.util.ReflectionUtils;

public final class JettyUtils implements JettyConstants {
    private JettyUtils() {
    }

    public static void initDefaultConnector(AbstractConnector ret, Configuration conf) {
        ret.setLowResourceMaxIdleTime(conf.getInt(k_jetty_lowResourceMaxIdleTime, v_jetty_lowResourceMaxIdleTime));
        ret.setAcceptQueueSize(conf.getInt(k_jetty_acceptQueueSize, v_jetty_acceptQueueSize));
        if (ret instanceof SelectChannelConnector) {
            ((SelectChannelConnector) ret).setLowResourcesConnections(conf.getInt(k_jetty_lowResourcesConnections,
                    v_jetty_LowResourcesConnections));
        }
        ret.setMaxIdleTime(conf.getInt(k_jetty_maxIdleTime, v_jetty_maxIdleTime));

        ret.setResolveNames(false);
        if (ret instanceof AbstractNIOConnector) {
            ((AbstractNIOConnector) ret).setUseDirectBuffers(false);
        }
        String[] cipherSuits = conf.getStrings(k_jetty_cipherSuits);
        if (cipherSuits != null) {
            if (ret instanceof SslSocketConnector) {
                ((SslSocketConnector) ret).setIncludeCipherSuites(cipherSuits);
            } else if (ret instanceof SslSelectChannelConnector) {
                ((SslSelectChannelConnector) ret).setIncludeCipherSuites(cipherSuits);
            }
        }
    }

    public static SelectChannelConnector createSelectChannelConnector(Configuration conf) {
        SelectChannelConnector ret = new SelectChannelConnector();
        initDefaultConnector(ret, conf);
        return ret;
    }

    public static BlockingChannelConnector createBlockingChannelConnector(Configuration conf) {
        BlockingChannelConnector ret = new BlockingChannelConnector();
        initDefaultConnector(ret, conf);
        return ret;
    }

    public static SocketConnector createSocketConnector(Configuration conf) {
        SocketConnector ret = new SocketConnector();
        initDefaultConnector(ret, conf);
        return ret;
    }

    public static AbstractConnector createSSLConnector(ConnectorType typ, String keystore, String keyPassword,
            String keystorePassword, boolean needClientAuth, boolean wantClientAuth) {
        switch (typ) {
        case BIOSocket:
            SslSocketConnector sc = new SslSocketConnector();
            sc.setKeystore(keystore);
            sc.setKeyPassword(keyPassword);
            sc.setPassword(keystorePassword);
            sc.setNeedClientAuth(needClientAuth);
            sc.setWantClientAuth(wantClientAuth);
            return sc;

        case NIOSelect:
            SslSelectChannelConnector select = new SslSelectChannelConnector();
            select.setKeystore(keystore);
            select.setKeyPassword(keyPassword);
            select.setPassword(keystorePassword);
            select.setNeedClientAuth(needClientAuth);
            select.setWantClientAuth(wantClientAuth);
            return select;
        case NIOBlocking:
        default:
            throw new IllegalArgumentException("unsupported ssl:" + typ);
        }
    }

    /**
     * Get an array of FilterConfiguration specified in the conf
     * 
     * @throws ClassNotFoundException
     */
    public static FilterInitializer[] getFilterInitializers(Configuration conf) {
        if (conf == null) {
            return null;
        }

        String[] classes = conf.getStrings(FILTER_INITIALIZER_PROPERTY);
        if (classes == null) {
            return null;
        }

        FilterInitializer[] initializers = new FilterInitializer[classes.length];
        try {
            for (int i = 0; i < classes.length; i++) {
                Class<?> clazz = Class.forName(classes[i]);
                initializers[i] = (FilterInitializer) ReflectionUtils.newInstance(clazz);
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return initializers;
    }

}
