package com.hiido.hcat.databus;

import com.hiido.hcat.databus.network.HttpProtocol;
import com.hiido.hcat.thrift.protocol.Field;
import com.hiido.suit.err.ErrCode;
import com.hiido.suit.err.ErrCodeException;
import com.hiido.suit.net.http.protocol.HttpApacheClient;
import com.hiido.suit.net.http.protocol.ha.HttpHAPoolClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hive.service.cli.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by zrc on 16-12-12.
 */
public class FetchScheduler implements Closeable {

    private static Logger LOG = LoggerFactory.getLogger(FetchScheduler.class);

    public static enum STATUS {
        SUCCESS(0), PENDING(1), RUNNING(2), FAILUE(3);
        int code;
        private STATUS(int value){
            this.code = value;
        }
    }

    private final BlockingQueue<Task> fifoQueue = new LinkedBlockingQueue<Task>();
    private final Map<String, Task> tasks = new ConcurrentHashMap<String, Task>();
    private final long rateLimit;
    private final AtomicBoolean close = new AtomicBoolean(false);
    private final AtomicInteger running = new AtomicInteger(0);

    private final Thread scheduleThread = new Thread() {
        public void run() {
            while (!close.get() && fifoQueue.peek() != null) {
                Task task = null;
                try {
                    task = fifoQueue.poll(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                }
                if(task != null) {
                    String qid = task.jobId;
                    Thread t = new Thread(task, "qid=" + qid);
                    t.setDaemon(true);
                    t.start();
                    LOG.info("start to run:" + qid);
                }
            }
        }
    };

    private class Task implements Runnable {
        final String jobId;
        String serializedTask;
        List<Field> schema;
        String appId;
        String appKey;
        String service_type_key;
        boolean rewrite_meta;

        HttpHAPoolClient client;

        volatile STATUS status = STATUS.PENDING;
        volatile long totalRows = 0l;

        public Task(String jobId, String serializedTask, List<Field> schema, String appId, String appKey, String service_type_key, boolean rewrite_meta,
                    String address) {
            this.jobId = jobId;
            this.serializedTask = serializedTask;
            this.schema = schema;
            this.appId = appId;
            this.appKey = appKey;
            this.service_type_key = service_type_key;
            this.rewrite_meta = rewrite_meta;


            client = new HttpHAPoolClient();
            client.setHttpProtocolClient(new HttpApacheClient());
            client.setAddrList(address);
            client.setPoolOneTryCount(2);
        }

        public void run() {
            running.incrementAndGet();
            try {
                status = STATUS.RUNNING;
                List<FieldSchema> fieldSchemas = new LinkedList<FieldSchema>();
                for(Field field : schema)
                    fieldSchemas.add(new FieldSchema(field.getName(), field.getType(), null));

                TableSchema resultSchema = new TableSchema(fieldSchemas);

                List<Map<String,Object>> list = new LinkedList<Map<String, Object>>();
                FetchTask fetch = SerializationUtilities.deserializeObject(serializedTask, FetchTask.class);
                HiveConf conf = new HiveConf(new Configuration(false), FetchScheduler.class);

                conf.set(SerDeUtils.LIST_SINK_OUTPUT_FORMATTER, "com.hiido.hcat.databus.io.DatabusFormatter");

                fetch.initialize(new QueryState(conf), null, null, new CompilationOpContext());
                fetch.setMaxRows(100);

                while(fetch.fetch(list)) {
                    pushData(list, false);
                    list.clear();
                    totalRows += list.size();
                }


                status = STATUS.SUCCESS;
            } catch (IOException | CommandNeedRetryException e) {
                status = STATUS.FAILUE;
            } finally {
                LOG.info("Finish jobId {} with status {}", jobId, status.name());
                running.decrementAndGet();
                if(client != null)
                    client.close();
            }
        }

        public long currentRows() {
            return totalRows;
        }


        private void pushData(List<Map<String, Object>> lines, boolean delete_record) {
            if (lines == null || lines.isEmpty()) {
                return;
            }
            //TODO
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
        }

    }

    public static class TaskProgress {
        long rows;
        STATUS status;

    }


    public FetchScheduler(long rateLimit) {
        this.rateLimit = rateLimit;
    }

    public void start() {
        scheduleThread.start();
    }


    public void push(String qid, String serializedTask, List<Field> schema, String appId, String appKey, String service_type_key, boolean rewrite_meta, String address) {
        Task task = new Task(qid, serializedTask, schema, appId, appKey, service_type_key, rewrite_meta, address);
        fifoQueue.add(task);
    }

    @Override
    public void close() throws IOException {
        if(close.get())
            return;
        close.set(true);
        try {
            scheduleThread.join();
        } catch (InterruptedException e) {
            LOG.warn("There are {} task running.", running.get());
        }

    }
}
