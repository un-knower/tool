package com.hiido.hcat.databus.network;

/**
 * Created by zrc on 16-11-30.
 */
public class Rpc {


    public static class MessageHeader {
        byte type;
        long size;
        long reqId;
        long respId;
    }

    public static class MessageBody {
        long messageSize;
        byte[] data;
    }

    public static class RpcMessage {
        MessageHeader header;
        MessageBody body;
    }



}
