package org.apache.hive.spark.client;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.spark.client.rpc.RpcServer;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Created by zrc on 16-11-22.
 */
public class TestRpcServer {
    static String INDEX_KEY = "key";

    static final Map<String, RpcServer> set = new HashMap<String, RpcServer>();

    public static void tartRpcServer() throws IOException, InterruptedException {

        final Thread server = new Thread() {
            @Override
            public void run() {
                try {
                    Map<String, String> conf = new HashMap<String, String>();
                    conf.put(HiveConf.ConfVars.SPARK_RPC_SERVER_ADDRESS.varname, "eth0");
                    conf.put(HiveConf.ConfVars.SPARK_RPC_SECRET_RANDOM_BITS.varname, String.valueOf(HiveConf.ConfVars.SPARK_RPC_SECRET_RANDOM_BITS.defaultIntVal));
                    RpcServer rpcServer = new RpcServer(conf);
                    set.put(INDEX_KEY, rpcServer);
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        server.start();
    }

    @Test
    public void testRegisterClient() throws ExecutionException, InterruptedException, IOException {

        Map<String, String> conf = new HashMap<String, String>();
        conf.put(HiveConf.ConfVars.SPARK_RPC_SERVER_ADDRESS.varname, "eth0");
        conf.put(HiveConf.ConfVars.SPARK_RPC_SECRET_RANDOM_BITS.varname, String.valueOf(HiveConf.ConfVars.SPARK_RPC_SECRET_RANDOM_BITS.defaultIntVal));
        RpcServer rpcServer = new RpcServer(conf);
        String clientId = UUID.randomUUID().toString();
        String secret = rpcServer.createSecret();
        ClientProtocol protocol = new ClientProtocol();
        rpcServer.registerClient(clientId, secret, protocol).get();

    }

    @Test
    public void testMap() {
        Map<String, String> map = new ConcurrentHashMap<String, String>();
        map.remove("abc");
    }

    class ClientProtocol extends BaseProtocol {

    }

}
