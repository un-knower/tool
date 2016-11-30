package com.hiido.hcat.databus.network;

/**
 * Created by zrc on 16-11-30.
 */
public class Rpc {


    static class SaslMessage {
        final byte[] payload;

        SaslMessage() {
            payload = null;
        }

        SaslMessage(byte[] payload) {
            this.payload = payload;
        }
    }

    static class HeartBeatMessage {
    }

}
