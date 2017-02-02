package com.hiido.hcat.databus;

import com.hiido.suit.net.http.protocol.HttpApacheClient;
import com.hiido.suit.net.http.protocol.ha.HttpHAPoolClient;
import org.junit.Test;

/**
 * Created by zrc on 16-12-13.
 */
public class TestFetchScheduler {

    @Test
    public void testFetchTask() {


    }

    @Test
    public void testPushData() {
        HttpHAPoolClient client = new HttpHAPoolClient();
        client.setHttpProtocolClient(new HttpApacheClient());
        client.setAddrList("");
        client.setPoolOneTryCount(2);

        /*
        try {
            HttpProtocol protocol = new HttpProtocol();
            protocol.setV("0.0.1");
            protocol.setAppId(appId);
            protocol.setAppKey(appKey);
            protocol.setServiceTypeKey(service_type_key);
            protocol.setMetaExt(new HttpProtocol.MetaExt(null, rewrite_meta ? HttpProtocol.MetaExt.REWRITE_META_COLUMNS_Y
                    : HttpProtocol.MetaExt.REWRITE_META_COLUMNS_N, delete_record ? HttpProtocol.MetaExt.DELETE_RECORD_Y : HttpProtocol.MetaExt.DELETE_RECORD_N));
            protocol.setValues(lines);

            HttpProtocol.Reply reply = client.post(protocol, HttpProtocol.Reply.class);
            if(!reply.isSuccess())
                throw new ErrCodeException(ErrCode.valueOfInt(reply.getErrcode()), reply.getMsg() == null ? "http error code " + reply.getErrcode() : reply.getMsg());
        } catch (ErrCodeException e) {
            LOG.error("Error Code {}, Msg: {}", e.errCode().name(), e.getMessage());
        } catch (Exception e) {
            LOG.error("Runtime Err :", e);
        }
        */

    }


}
