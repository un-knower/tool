package com.hiido.hcat.common.util;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.nio.charset.Charset;

/**
 * Created by zrc on 17-6-20.
 */
public final class ZKStringSerializer implements ZkSerializer {
    public static final ZKStringSerializer instance = new ZKStringSerializer();
    public static final Charset UTF8 = Charset.forName("utf-8");

    private ZKStringSerializer() {
    }

    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
        String str = (String) data;
        return str.getBytes(UTF8);
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        return new String(bytes, UTF8);
    }

}
