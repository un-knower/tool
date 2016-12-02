package com.hiido.hcat.databus.network;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hiido.hcat.databus.Config;
import com.hiido.hcat.databus.protocol.RpcProtocol;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * Created by zrc on 16-11-30.
 */
public class RpcServer implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(RpcServer.class);

    private final String address;
    private final int port;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workGroup;
    private final Channel channel;

    int rpcBossThreadCount = 0;
    int rpcWorkThreadCount = 0;

    public RpcServer(Config mapConf) throws IOException, InterruptedException {
        this.address = mapConf.getStrVar(Config.Vars.RPCSERVER_ADDRESS);
        this.port = mapConf.getIntVar(Config.Vars.PRCSERVER_PORT);
        this.rpcBossThreadCount = mapConf.getIntVar(Config.Vars.RPCSERVER_BOSS_THREADCOUNT);
        this.rpcWorkThreadCount = mapConf.getIntVar(Config.Vars.RPCSERVER_WORK_THREADCOUNT);

        bossGroup = new NioEventLoopGroup(rpcBossThreadCount,
                new ThreadFactoryBuilder()
                    .setNameFormat("RPC-Boss-Handler-%d")
                    .setDaemon(true)
                    .build());

        workGroup = new NioEventLoopGroup(rpcWorkThreadCount,
                new ThreadFactoryBuilder()
                    .setNameFormat("RPC-Work-Handler-%d")
                    .setDaemon(true)
                    .build());

        this.channel = new ServerBootstrap()
                .group(bossGroup, workGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        //TODO add SaslServerHandler
                        ch.pipeline().addLast(new ProtobufVarint32FrameDecoder());
                        //ch.pipeline().addLast(new ProtobufDecoder(RpcProtocol.))
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 100)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .bind(address, port)
                .sync()
                .channel();
    }

    private class SaslServerHandler extends SaslHandler implements CallbackHandler {

        private final SaslServer server;

        SaslServerHandler(Map<String, String> mapConf) throws SaslException {
            super(LOG);
            this.server = Sasl.createSaslServer("DIGEST-MD5", "rsc", "rsc", mapConf, this);
        }

        @Override
        protected boolean isComplete() {
            return server.isComplete();
        }

        @Override
        protected String getNegotiatedProperty(String name) {
            return server.getNegotiatedProperty(name).toString();
        }

        @Override
        protected Rpc.SaslMessage update(Rpc.SaslMessage challenge) throws IOException {
            return new Rpc.SaslMessage(server.evaluateResponse(challenge.payload));
        }

        @Override
        protected void onComplete() throws Exception {
            //do nothing
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {

        }
    }

    @Override
    public void close() throws IOException {
        try{
            channel.close();
        } finally {
            bossGroup.shutdownGracefully();
            workGroup.shutdownGracefully();
        }
    }
}
