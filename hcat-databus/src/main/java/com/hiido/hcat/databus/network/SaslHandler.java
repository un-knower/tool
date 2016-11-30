package com.hiido.hcat.databus.network;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by zrc on 16-11-30.
 */
public abstract class SaslHandler extends SimpleChannelInboundHandler<Rpc.SaslMessage> {

    private final Logger LOG;
    private boolean hasAuthResponse = false;

    protected SaslHandler() {
        this.LOG = LoggerFactory.getLogger(SaslHandler.class);
    }

    protected  SaslHandler(Logger log) {
        this.LOG = log;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Rpc.SaslMessage msg) throws Exception {
        LOG.debug("Handling SASL challenge message...");
        Rpc.SaslMessage response = update(msg);
        if(response != null) {
            LOG.debug("Sending SASL challenge response...");
            hasAuthResponse = true;
            ctx.channel().writeAndFlush(response).sync();
        }

        if(!isComplete()) {
            return;
        }

        ctx.channel().pipeline().remove(this);
        onComplete();
    }

    protected abstract boolean isComplete();

    protected abstract String getNegotiatedProperty(String name);

    protected abstract Rpc.SaslMessage update(Rpc.SaslMessage challenge) throws IOException;

    protected abstract void onComplete() throws Exception;

}
