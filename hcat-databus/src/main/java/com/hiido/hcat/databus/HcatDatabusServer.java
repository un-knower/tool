package com.hiido.hcat.databus;

import com.hiido.hcat.common.util.StringUtils;
import com.hiido.hcat.common.util.SystemUtils;
import com.hiido.hcat.databus.io.DatabusFormatter;
import com.hiido.hcat.databus.network.HttpServer;
import com.hiido.hcat.thrift.protocol.*;
import com.hiido.hcat.thrift.protocol.RuntimeException;
import com.hiido.hva.thrift.protocol.Recode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;
import org.apache.thrift.transport.THttpClient;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zrc on 16-12-13.
 */
public class HcatDatabusServer implements CliService.Iface {
    private static final Logger LOG = LoggerFactory.getLogger(HcatDatabusServer.class);


    private final ProducerScheduler producerScheduler;

    private List<String> hcatServer;
    private int maxRows = 100;
    private final String defaultServiceAddress;

    private final HttpServer httpServer;
    private final Map<String, Task> qid2Task = new ConcurrentHashMap<String, Task>();
    private final BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<Task>();
    private final AtomicBoolean close = new AtomicBoolean(false);
    private final AtomicBoolean closeSignal = new AtomicBoolean(false);
    private final AtomicInteger runningTask = new AtomicInteger(0);

    private final long historyTaskLife = 60 * 60;
    private final long maxHistoryTask = 100;

    public HcatDatabusServer(String host, int port, int maxThreads, int minThreads, int maxIdleTimeMs
            , String serverAddress, int queueCapacity, int parallelism) throws IOException {
        HttpServer.Builder builder = new HttpServer.Builder();
        httpServer = builder.setName("hiido").setHost(host).setPort(port).setMaxThreads(maxThreads).setMinThreads(minThreads).setMaxIdleTimeMs(maxIdleTimeMs)
                .setUseSSL(false).build();
        httpServer.addServlet("query", "/query", new QueryServlet(new CliService.Processor<HcatDatabusServer>(this),
                new TBinaryProtocol.Factory(true, true)));
        httpServer.addServlet("reject", "/reject", new RejectServlet());
        this.defaultServiceAddress = serverAddress;
        producerScheduler = new ProducerScheduler(serverAddress, queueCapacity, parallelism);
    }

    public void start() throws Exception {
        producerScheduler.start();
        Thread disper = new Thread(new Disper(), "task-dipser");
        disper.start();
        httpServer.start();
    }

    public void close() throws Exception {
        closeSignal.set(true);
        httpServer.stop();
        producerScheduler.close();
    }

    public void setHcatServer(List<String> list) {
        hcatServer = list;
    }

    public List<String> getHcatServer() {
        return hcatServer;
    }

    public void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    public int getMaxRows() {
        return maxRows;
    }

    private final class Disper implements Runnable {

        private long cleanTaskInterval = 1000 * 60 * 60;
        private long lastCleanTime = System.currentTimeMillis();

        public void run() {
            while (!closeSignal.get() || taskQueue.peek() != null) {
                Task task = null;
                try {
                    task = taskQueue.poll(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                }
                if (task != null) {
                    String qid = task.qid;
                    Thread t = new Thread(task, "qid=" + qid);
                    t.setDaemon(true);
                    t.start();
                    LOG.info("start to run:" + qid);
                }
                if (System.currentTimeMillis() - cleanTaskInterval > lastCleanTime)
                    cleanTaskHistory(true);
            }
        }

        private void cleanTaskHistory(boolean force) {
            long now = System.currentTimeMillis();
            if (qid2Task.size() > maxHistoryTask) {
                Set<String> tkeys = new HashSet<String>(qid2Task.keySet());
                for (String k : tkeys) {
                    Task t = qid2Task.get(k);
                    if (t == null) {
                        continue;
                    }
                    boolean clean = t.finish && (force || t.endTime * 1000 - now >= historyTaskLife);
                    if (clean) {
                        qid2Task.remove(k);
                    }
                }
            }
        }
    }


    final class Task implements Runnable {

        private String qid;
        private String address;
        private Map<String, String> conf;
        private QueryProgress progress = new QueryProgress();
        private Set<Long> pending = new ConcurrentHashSet<Long>();
        private AtomicLong currentUid = new AtomicLong(0l);

        long endTime;
        volatile boolean finish;
        volatile AtomicBoolean error = new AtomicBoolean(false);
        final StringBuilder errMss = new StringBuilder();
        AtomicLong transfered = new AtomicLong(0l);

        public Task(String qid, String address, Map conf) {
            this.qid = qid;
            this.address = address;
            this.conf = conf;
            this.progress.setJobId(new LinkedList<String>()).setState(JobStatus.READY.getValue()).setN(1)
                    .setEngine("mapreduce").setErrmsg("").setIsFetchTask(false).setProgress(0.0);
        }

        public void run() {
            runningTask.incrementAndGet();
            try {
                THttpClient thc = new THttpClient(address);
                TProtocol lopFactory = new TBinaryProtocol(thc);
                CliService.Client client = new CliService.Client(lopFactory);
                QueryStatusReply status = client.queryJobStatus(new QueryStatus(conf, qid));
                while (status.retCode == 0 && status.getQueryProgress().state <= 1) {
                    this.progress = status.getQueryProgress();
                    SystemUtils.sleep(5000);
                    status = client.queryJobStatus(new QueryStatus(conf, qid));
                }

                //TODO 状态暂时不为2
                //this.progress = status.queryProgress;
                LOG.info("job {} has been done with status {}.", qid, status.getQueryProgress().state);

                if (status.getQueryProgress().state == 2) {
                    status.getQueryProgress().setState(1);
                    this.progress = status.queryProgress;

                    long size = progress.resSize;
                    List<Field> schema = progress.getFields();
                    List<String> fetchDirs = progress.getFetchDirs();


                    if (size == 0)
                        return;

                    if (schema == null || schema.size() == 0 || fetchDirs == null)
                        throw new IllegalArgumentException("Lack schema or fetch task.");

                    String serializedTask = fetchDirs.get(0);
                    final String serverTypeKey = fetchDirs.get(1);
                    String serverAddress = fetchDirs.size() > 2 ? fetchDirs.get(2) : this.address;
                    if(StringUtils.isEmpty(serverAddress))
                        serverAddress = HcatDatabusServer.this.defaultServiceAddress;

                    LOG.info("job {} start to send data to {}", qid, serverAddress);
                    if (serverTypeKey == null)
                        throw new IllegalArgumentException("service type key should not be 'null'.");

                    int i = 0;
                    StringBuilder builder = new StringBuilder();
                    do {
                        builder.append(schema.get(i).getName());
                        builder.append(";");
                        i++;
                    } while (i < schema.size());
                    HiveConf conf = new HiveConf(new Configuration(true), FetchScheduler.class);
                    conf.set(DatabusFormatter.FIELDS, builder.toString());
                    conf.set(SerDeUtils.LIST_SINK_OUTPUT_FORMATTER, "com.hiido.hcat.databus.io.DatabusFormatter");

                    FetchTask fetch = SerializationUtilities.deserializeObject(serializedTask, FetchTask.class);
                    fetch.initialize(new QueryState(conf), null, null, new CompilationOpContext());
                    fetch.setMaxRows(maxRows);

                    final ProducerScheduler.OnceFailureListener failureListener = new ProducerScheduler.OnceFailureListener() {
                        @Override
                        protected void handle(long uid, Exception e) {
                            if (!error.get()) {
                                synchronized (error) {
                                    if (error.get())
                                        return;
                                    error.set(true);
                                    errMss.append(e.toString());
                                    LOG.error("error when transfer data.", e);
                                }
                            }
                        }
                    };
                    final ProducerScheduler.SuccessListener successListener = new ProducerScheduler.SuccessListener() {
                        //rate
                        AtomicInteger count = new AtomicInteger(0);

                        @Override
                        protected void handle(long uid, long rows, long rate) {
                            transfered.addAndGet(rows);
                            if(count.incrementAndGet() % 20 == 0)
                                LOG.info("producer send 100 rows spend {}", rate);
                            pending.remove(uid);
                        }
                    };

                    ProducerScheduler.Key key = null;
                    try {
                        key = producerScheduler.register(serverTypeKey, serverAddress, successListener, failureListener);
                        List<Map<String, Object>> list = new LinkedList<Map<String, Object>>();
                        while ((!error.get()) && fetch.fetch(list)) {
                            producerScheduler.pushData(key, currentUid.get(), list, false);
                            pending.add(currentUid.get());
                            currentUid.incrementAndGet();
                            list = new LinkedList<Map<String, Object>>();
                        }
                        while (pending.size() > 0 && !error.get()) {
                            SystemUtils.sleep(5000);
                        }
                    }catch(ProducerScheduler.UnRegisteredException e) {
                        LOG.warn("task {} had been unregisted or never register before transfer.", qid);
                    } finally {
                        if(key != null)
                            producerScheduler.unregister(key);
                    }
                } else {
                    error.set(true);
                    this.progress = status.queryProgress;
                }
            } catch (Exception e) {
                error.set(true);
                this.progress.setErrmsg(e.toString());
                LOG.error("Err when waiting job complete.", e);
            } finally {
                pending.clear();
                this.progress.setState(error.get() ? 3 : 2);
                this.progress.setEndTime(System.currentTimeMillis());
                if(errMss.length() > 0)
                    this.progress.setErrmsg(errMss.toString());
                endTime = System.currentTimeMillis();
                finish = true;
                runningTask.decrementAndGet();
                LOG.info("Task {} finished with state {}, transferd row {}", this.qid, this.progress.getState(), this.transfered.get());
            }
        }
    }

    public class RejectServlet extends HttpServlet {

        public void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            InputStreamReader reader = new InputStreamReader(req.getInputStream());

            String key = "hiidosys_closeserverkey";
            char[] array = new char[key.length()];
            reader.read(array, 0, key.length());
            if (!key.equals(String.valueOf(array)))
                return;
            if (!close.get())
                close.set(true);
            else
                return;

            resp.getOutputStream().flush();
            LOG.info("closing HttpHiveServer......");
            final Thread t = new Thread(new SoftReject());
            t.setDaemon(true);
            t.start();
            try {
                t.join();
            } catch (InterruptedException e) {
            }
        }
    }

    private class SoftReject implements Runnable {

        @Override
        public void run() {
            while (close.get()) {
                SystemUtils.sleep(5000);
                if (runningTask.get() <= 0 && taskQueue.peek() == null)
                    break;
                else {
                    LOG.info(String.format("waiting for %d tasks to be done.",
                            runningTask.get()));
                }
            }
            LOG.warn("HttpHiveServer closed.");
            try {
                close();
            } catch (Exception e) {
                LOG.warn("Err when closing server.", e);
            }

            System.exit(0);
        }
    }


    public class QueryServlet extends TServlet {

        public QueryServlet(TProcessor processor, TProtocolFactory protocolFactory) {
            super(processor, protocolFactory);
        }
    }

    @Override
    public CommitQueryReply commit(CommitQuery cq) throws AuthorizationException, RuntimeException, TException {
        Map<String, String> conf = cq.getConf();
        if(conf == null) {
            conf = new HashMap<String ,String>();
            cq.setConf(conf);
        }
        conf.put("hcat.query.return.fetch", "true");
        int retry = 0;
        CommitQueryReply reply = null;
        for (String address : hcatServer) {
            try {
                THttpClient thc = new THttpClient(address);
                TProtocol lopFactory = new TBinaryProtocol(thc);
                CliService.Client client = new CliService.Client(lopFactory);
                reply = client.commit(cq);
                Task task = new Task(reply.getHandle().getQueryId(), address, cq.getConf());
                qid2Task.put(task.qid, task);
                taskQueue.add(task);
                break;
            } catch (RuntimeException e) {
                retry++;
                if (retry <= hcatServer.size())
                    continue;
                throw new AuthorizationException(e.getMessage());
            } catch (Exception e) {
                throw new AuthorizationException(e.getMessage());
            }
        }
        return reply;
    }

    @Override
    public QueryStatusReply queryJobStatus(QueryStatus qs) throws NotFoundException, TException {
        Task task = qid2Task.get(qs.queryId);
        if (task == null) {
            LOG.warn("server not found qid {} ", qs.queryId);
            throw new NotFoundException(String.format("Not found qid %s.", qs.queryId));
        }
        QueryStatusReply reply = new QueryStatusReply();
        reply.setQueryProgress(task.progress);
        return reply;
    }

    @Override
    public CancelQueryReply cancelJob(CancelQuery cq) throws AuthorizationException, NotFoundException, TException {
        CancelQueryReply reply = new CancelQueryReply();

        Task task = qid2Task.get(cq.queryId);
        if (task == null)
            throw new NotFoundException("Databus Not found qid " + cq.queryId);

        if(task.finish)
            return new CancelQueryReply().setReCode(Recode.FAILURE.getValue()).setRetMessage("Job had finished.");
        else {
            task.error.set(true);
            LOG.info("Cancel job {} by requested.", task.qid);
            THttpClient thc = new THttpClient(task.address);
            TProtocol lopFactory = new TBinaryProtocol(thc);
            CliService.Client client = new CliService.Client(lopFactory);
            return client.cancelJob(cq);
        }
    }

    @Override
    public LoadFileReply laodData(LoadFile lf) throws AuthorizationException, RuntimeException, TException {
        throw new AuthorizationException("Unsupport loadData.");
    }
}
