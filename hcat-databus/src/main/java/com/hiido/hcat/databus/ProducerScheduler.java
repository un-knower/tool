package com.hiido.hcat.databus;

import com.hiido.hcat.common.util.StringUtils;
import com.hiido.hcat.databus.network.HttpProtocol;
import com.hiido.suit.err.ErrCode;
import com.hiido.suit.err.ErrCodeException;
import com.hiido.suit.net.http.protocol.HttpApacheClient;
import com.hiido.suit.net.http.protocol.ha.HttpHAPoolClient;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by zrc on 16-12-19.
 */
public class ProducerScheduler implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerScheduler.class);

    private volatile boolean close = false;
    private final String serverAddress;
    private final LinkedBlockingQueue<Message> queue;
    private final Set<Key> registeredServerKey = new ConcurrentHashSet<Key>();
    private final Map<Key, OnceFailureListener> onceFailureListeners = new ConcurrentHashMap<Key, OnceFailureListener>();
    private final Map<Key, SuccessListener> successListeners = new ConcurrentHashMap<Key, SuccessListener>();
    private final Producer[] producers;
    private final CountDownLatch countDownLatch;

    public static class Key {
        String serviceKey = "";
        String serverAddress = "";

        public Key(String serviceKey, String serverAddress) {
            if (StringUtils.isEmpty(serviceKey) || StringUtils.isEmpty(serverAddress))
                throw new IllegalArgumentException("Task servicekey and serverAddress should not be empty string.");
            this.serviceKey = serviceKey;
            this.serverAddress = serverAddress;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Key) {
                Key key = (Key) o;
                return (serviceKey.equals(key.serviceKey) && serverAddress.equals(key.serverAddress));
            } else
                return false;
        }

        @Override
        public int hashCode() {
            return serviceKey.hashCode() * 31 + serverAddress.hashCode();
        }
    }


    private class Message {
        Key key;
        boolean reset_date_and_put;
        long uid;
        String store_time;
        List<Map<String, Object>> lines;

        public Message(Key key, long uid, List<Map<String, Object>> lines) {
            this.key = key;
            this.uid = uid;
            this.lines = lines;
        }

        public Message(Key key, long uid, List<Map<String, Object>> lines, boolean reset_date_and_put) {
            this.key = key;
            this.uid = uid;
            this.lines = lines;
            this.reset_date_and_put = reset_date_and_put;
        }

        public Message(Key key, long uid, List<Map<String, Object>> lines, String store_time) {
            this.key = key;
            this.uid = uid;
            this.lines = lines;
            this.store_time = store_time;
        }
    }

    public ProducerScheduler(String serverAddress, int queueCapacity, int parallelism) {
        this.countDownLatch = new CountDownLatch(parallelism);
        this.serverAddress = serverAddress;
        this.queue = new LinkedBlockingQueue<Message>(queueCapacity);
        producers = new Producer[parallelism];
    }


    public class Producer extends Thread {

        private HttpHAPoolClient databusClient;

        public Producer() {
            databusClient = new HttpHAPoolClient();
            databusClient.setHttpProtocolClient(new HttpApacheClient());
            databusClient.setAddrList(serverAddress);
            databusClient.setPoolOneTryCount(2);
        }

        @Override
        public void run() {
            try {
                while (!close || queue.peek() != null) {
                    Message mss = null;
                    try {
                        mss = queue.poll(10000, TimeUnit.MILLISECONDS);
                        if (mss == null)
                            continue;
                    } catch (InterruptedException e) {
                        continue;
                    }

                    if (!registeredServerKey.contains(mss.key))
                        continue;

                    if (!mss.key.serverAddress.equals(databusClient.getAddrList()))
                        databusClient.setAddrList(mss.key.serverAddress);

                    HttpProtocol protocol = new HttpProtocol();
                    protocol.setV("0.1");
                    protocol.setAppId("yyLiveIndexRecom_zhoupeiyuan");
                    protocol.setAppKey("oi2340sdfklkjdljlksjdasfjklkj");
                    protocol.setServiceTypeKey(mss.key.serviceKey);
                    protocol.setMetaExt(new HttpProtocol.MetaExt(mss.store_time, HttpProtocol.MetaExt.REWRITE_META_COLUMNS_N,
                            HttpProtocol.MetaExt.DELETE_RECORD_N,
                            mss.reset_date_and_put ? HttpProtocol.MetaExt.RESET_DATE_AND_PUT_Y : HttpProtocol.MetaExt.RESET_DATE_AND_PUT_N));
                    protocol.setValues(mss.lines);

                    HttpProtocol.Reply reply = null;
                    try {
                        long start = System.currentTimeMillis();
                        reply = databusClient.post(protocol, HttpProtocol.Reply.class);
                        long end = System.currentTimeMillis();
                        if (!reply.isSuccess()) {
                            OnceFailureListener listener = onceFailureListeners.get(mss.key);
                            if (listener != null)
                                listener.handle(mss.uid, new ErrCodeException(ErrCode.IOErr, reply.getMsg() == null ? "http error code " + reply.getErrcode() : reply.getMsg()));
                        } else {
                            SuccessListener listener = successListeners.get(mss.key);
                            if (listener != null)
                                listener.handle(mss.uid, mss.lines.size(), end - start);
                        }
                    } catch (Exception e) {
                        OnceFailureListener listener = onceFailureListeners.get(mss.key);
                        if (listener != null)
                            listener.handle(mss.uid, e);
                    }
                }
            } catch (Exception e) {
                LOG.warn("producer err :", e);
            } finally {
                countDownLatch.countDown();
                LOG.warn("One produce Processor is finished.");
            }
        }
    }

    public Key register(String service_type_key, String serverAddress, SuccessListener successListener, OnceFailureListener onceFailureListener) throws RepeatedKeyException {
        Key key = new Key(service_type_key, serverAddress);
        if (registeredServerKey.contains(key))
            throw new RepeatedKeyException();
        registeredServerKey.add(key);
        if (successListener != null)
            successListeners.put(key, successListener);
        if (onceFailureListener != null)
            onceFailureListeners.put(key, onceFailureListener);
        return key;
    }

    public void unregister(Key key) {
        registeredServerKey.remove(key);
        successListeners.remove(key);
        onceFailureListeners.remove(key);
    }

    public void pushData(Key key, long uid, List<Map<String, Object>> lines, boolean delete_record, boolean reset_date_and_put) throws InterruptedException, UnRegisteredException {
        if (registeredServerKey.contains(key)) {
            queue.put(new Message(key, uid, lines, reset_date_and_put));
        } else
            throw new UnRegisteredException();
    }

    //push data with expire_time
    public void pushData(Key key, long uid, List<Map<String, Object>> lines, String storeTimeCol) throws InterruptedException, UnRegisteredException {
        if (registeredServerKey.contains(key)) {
            //按照存储时间分拣
            Map<String, List<Map<String, Object>>> storeTime2Rows = new HashMap<>();
            for(Map<String, Object> row : lines) {
                String storeTime = row.get(storeTimeCol).toString();
                List<Map<String, Object>> list = storeTime2Rows.get(storeTime);
                if(list == null) {
                    list = new LinkedList<>();
                    storeTime2Rows.put(storeTime, list);
                }
                list.add(row);
            }
            for(Map.Entry<String, List<Map<String, Object>>> rows : storeTime2Rows.entrySet())
                queue.put(new Message(key, uid, rows.getValue(), rows.getKey()));
        } else
            throw new UnRegisteredException();
    }

    public void start() {
        for (int i = 0; i < producers.length; i++) {
            Producer p = new Producer();
            producers[i] = p;
            p.setDaemon(true);
            p.start();
        }
    }

    @Override
    public void close() throws IOException {
        this.close = true;
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
        }
    }

    public static abstract class OnceFailureListener {
        protected abstract void handle(long uid, Exception exception);
    }

    public static abstract class SuccessListener {
        protected abstract void handle(long uid, long rows, long rate);
    }

    public static class RepeatedKeyException extends Exception {
    }

    public static class UnRegisteredException extends Exception {
    }

}
