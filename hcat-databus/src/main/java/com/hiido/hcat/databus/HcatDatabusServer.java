package com.hiido.hcat.databus;

import com.hiido.hcat.common.util.StringUtils;
import com.hiido.hcat.common.util.SystemUtils;
import com.hiido.hcat.databus.io.DatabusFormatter;
import com.hiido.hcat.databus.network.HttpServer;
import com.hiido.hcat.thrift.protocol.*;
import com.hiido.hcat.thrift.protocol.RuntimeException;
import com.hiido.hva.thrift.protocol.Recode;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.HttpClients;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;
import org.apache.thrift.transport.THttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zrc on 16-12-13.
 */
public class HcatDatabusServer implements CliService.Iface {
    private static final Logger LOG = LoggerFactory.getLogger(HcatDatabusServer.class);

    private static final String INSERT_SQL = "insert into bees.databus_statistics (qid, server,schedule_id, exec_start, exec_end, row_count, state, server_type, err_msg) values(?,?,?,?,?,?,?,?,?)";
    private static final String UPDATE_SQL = "update bees.databus_statistics set schedule_id=?, exec_end=?, row_count=?, state=?, err_msg=? where qid=?";
    private static final String SELECT_SQL = "select state, exec_start, exec_end,err_msg from bees.databus_statistics where qid=?";

    //private final ProducerScheduler producerScheduler;

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
    private String serverTag = null;

    //private final BasicDataSource dataSource;
    private Class<?> dirver;

    public HcatDatabusServer(String host, int port, int maxThreads, int minThreads, int maxIdleTimeMs
            , String serverAddress, int queueCapacity, int parallelism) throws IOException {
        HttpServer.Builder builder = new HttpServer.Builder();
        httpServer = builder.setName("hiido").setHost(host).setPort(port).setMaxThreads(maxThreads).setMinThreads(minThreads).setMaxIdleTimeMs(maxIdleTimeMs)
                .setUseSSL(false).build();
        httpServer.addServlet("query", "/query", new QueryServlet(new CliService.Processor<HcatDatabusServer>(this),
                new TBinaryProtocol.Factory(true, true)));
        httpServer.addServlet("reject", "/reject", new RejectServlet());
        this.defaultServiceAddress = serverAddress;
        this.serverTag = createServerTag("0.0.0.0", port, "eth0");
        //producerScheduler = new ProducerScheduler(serverAddress, queueCapacity, parallelism);
        //dataSource = new BasicDataSource();
    }

    public void start() throws Exception {
        try {
            dirver = Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new java.lang.RuntimeException(e);
        }

        /*
        dataSource.setDriverClassName(driverName);
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setMaxIdle(maxIdle);
        dataSource.setMinIdle(minIdle);
        dataSource.setMaxActive(maxTotal);
        dataSource.setMaxWait(maxWaitMillis);
        dataSource.setTestOnBorrow(true);

        dataSource.setTestWhileIdle(true);
        dataSource.setTestOnReturn(true);
        dataSource.setNumTestsPerEvictionRun(2);
        dataSource.setMinEvictableIdleTimeMillis(1800000);
        dataSource.setDefaultAutoCommit(true);
        */
        //producerScheduler.start();

        Thread disper = new Thread(new Disper(), "task-dipser");
        disper.start();
        httpServer.start();
    }

    public void close() throws Exception {
        closeSignal.set(true);
        httpServer.stop();
        //dataSource.close();
        //producerScheduler.close();
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

    /**
     *
     * @param host default host
     * @param port
     * @param ifname
     * @return
     */
    protected String createServerTag(String host, int port, String ifname) {
        String tag = null;
        if (ifname != null) {
            tag = SystemUtils.getNetInterface(ifname);
        }
        if (tag == null) {
            tag = host;
        }
        return String.format("%s_%d", tag.replaceAll("\\.", "_"), port);
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
        private LinkedBlockingQueue<Long> pending = new LinkedBlockingQueue<Long>(8);
        private AtomicLong currentUid = new AtomicLong(0l);

        long endTime;
        volatile boolean finish;

        //rate
        AtomicLong count = new AtomicLong(0);
        AtomicLong spended = new AtomicLong(0);
        long maxRate = 0l;
        long currentRate = 0l;

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
            String server_type = null;
            String schedule_id = null;
            runningTask.incrementAndGet();

            try {
                QueryStatusReply status = null;
                try(CloseableHttpClient httpclient = HttpClients.custom()
                        .setKeepAliveStrategy(DefaultConnectionKeepAliveStrategy.INSTANCE)
                        .build();THttpClient thc = new THttpClient(address, httpclient);) {
                    TProtocol lopFactory = new TBinaryProtocol(thc);
                    CliService.Client client = new CliService.Client(lopFactory);
                    status = client.queryJobStatus(new QueryStatus(conf, qid));
                    while (status.retCode == 0 && status.getQueryProgress().state <= 1) {
                        this.progress = status.getQueryProgress();
                        SystemUtils.sleep(5000);
                        status = client.queryJobStatus(new QueryStatus(conf, qid));
                    }
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


                    if (schema == null || schema.size() == 0 || fetchDirs == null)
                        throw new IllegalArgumentException("Lack schema or fetch task.");

                    String serializedTask = fetchDirs.get(0);

                    Map<String,String> customizedConf = new HashMap<>();
                    for(int i = 1; i < fetchDirs.size(); i++) {
                        String[] kv = fetchDirs.get(i).split("=");
                        customizedConf.put(kv[0], kv[1]);
                    }

                    schedule_id = customizedConf.get("hiido.scheduleid");

                    //final String serverTypeKey = fetchDirs.get(1);
                    final String serverTypeKey = customizedConf.get(Config.HCAT_DATABUS_SERVICE_TYPE_KEY);
                    server_type = serverTypeKey;

                    //String serverAddress = fetchDirs.size() > 2 ? fetchDirs.get(2) : this.address;
                    String serverAddress = customizedConf.get(Config.HCAT_DATABUS_SERVICE_ADDRESS);
                    if (StringUtils.isEmpty(serverAddress))
                        serverAddress = HcatDatabusServer.this.defaultServiceAddress;

                    //String storeTimeCol = fetchDirs.size() > 3 ? fetchDirs.get(3) : null;
                    String storeTimeCol = customizedConf.get(Config.HCAT_DATABUS_DATA_STORETIME_COLUMN);

                    LOG.info("job {} start to send data to {}", qid, serverAddress);
                    if (serverTypeKey == null)
                        throw new IllegalArgumentException("service type key should not be 'null'.");

                    if (size == 0)
                        return;

                    int i = 0;
                    StringBuilder builder = new StringBuilder();
                    do {
                        builder.append(schema.get(i).getName());
                        builder.append(";");
                        i++;
                    } while (i < schema.size());
                    HiveConf conf = new HiveConf(new Configuration(true), FetchScheduler.class);
                    conf.set(SerDeUtils.LIST_SINK_OUTPUT_FORMATTER, "com.hiido.hcat.databus.io.DatabusFormatter");
                    conf.set(DatabusFormatter.FIELDS, builder.toString());

                    FetchTask fetch = SerializationUtilities.deserializeObject(serializedTask, FetchTask.class);
                    fetch.initialize(new QueryState(conf), null, null, new CompilationOpContext());


                    int batchRow = maxRows;
                    try{
                        String rowSize = customizedConf.get(Config.HCAT_DATABUS_BATCH_ROW);
                        if(rowSize !=null)
                            batchRow = Integer.parseInt(rowSize);
                    } catch (NumberFormatException e) {
                        batchRow = maxRows;
                    }
                    fetch.setMaxRows(batchRow);

                    final ProducerScheduler.OnceFailureListener failureListener = new ProducerScheduler.OnceFailureListener() {
                        @Override
                        protected                                                                                                                                                                                                                                                                                                                                                                       void handle(long uid, Exception e) {
                            if (!error.get()) {
                                synchronized (error) {
                                    if (error.get())
                                        return;
                                    error.set(true);
                                    errMss.append(e.toString());
                                    LOG.error("error when transfer data.", e);
                                }
                            }
                            pending.remove(uid);
                        }
                    };
                    final ProducerScheduler.SuccessListener successListener = new ProducerScheduler.SuccessListener() {

                        @Override
                        protected void handle(long uid, long rows, long rate) {
                            transfered.addAndGet(rows);
                            count.incrementAndGet();
                            spended.addAndGet(rate);
                            pending.remove(uid);
                            currentRate = rate;
                            if (rate > maxRate)
                                maxRate = rate;
                        }
                    };

                    int parallelism = 4;
                    String parallelismStr = customizedConf.get(Config.HCAT_DATABUS_PRODUCER_PARALLELISM);
                    if(parallelismStr != null)
                        try{
                            parallelism = Integer.parseInt(parallelismStr);
                        }catch (NumberFormatException e){
                            parallelism = 4;
                        }

                    final ProducerScheduler producerScheduler = new ProducerScheduler(serverAddress, 4, parallelism);
                    producerScheduler.start();

                    ProducerScheduler.Key key = null;
                    try {
                        key = producerScheduler.register(qid, serverTypeKey, serverAddress, successListener, failureListener);
                        List<Map<String, Object>> list = new LinkedList<Map<String, Object>>();

                        if(customizedConf.get(Config.HCAT_DATABUS_PUSHALL_ONCE_REQUEST) != null
                                && customizedConf.get(Config.HCAT_DATABUS_PUSHALL_ONCE_REQUEST).equalsIgnoreCase("true")) {
                            while ((!error.get()) && fetch.fetch(list))
                                ;
                            try {
                                pending.put(currentUid.get());
                                producerScheduler.pushData(key, currentUid.get(), list, false, true);
                            } catch (InterruptedException e) {
                                LOG.warn("Interrupted when putting uid into task.pendingQueue.");
                            }
                            currentUid.incrementAndGet();
                        } else if(customizedConf.get(Config.HCAT_DATABUS_RESET_DATA) != null
                                && customizedConf.get(Config.HCAT_DATABUS_RESET_DATA).equalsIgnoreCase("true")) {
                            boolean isFirst = true;
                            //第一次reset只发送少量数据
                            fetch.setMaxRows(10);
                            while ((!error.get()) && fetch.fetch(list)) {
                                try {
                                    pending.put(currentUid.get());
                                    producerScheduler.pushData(key, currentUid.get(), list, false, isFirst);
                                    if(isFirst) {
                                        while(pending.size() != 0) {
                                            Thread.sleep(5 * 1000);
                                        }
                                        isFirst = false;
                                        fetch.setMaxRows(batchRow);
                                    }

                                } catch (InterruptedException e) {
                                    LOG.warn("Interrupted when putting uid into task.pendingQueue.");
                                }
                                currentUid.incrementAndGet();
                                list = new LinkedList<Map<String, Object>>();
                            }
                        } else {
                            while ((!error.get()) && fetch.fetch(list)) {
                                //阻塞
                                try {
                                    pending.put(currentUid.get());
                                    if(StringUtils.isEmpty(storeTimeCol))
                                        producerScheduler.pushData(key, currentUid.get(), list, false, false);
                                    else
                                        producerScheduler.pushData(key, currentUid.get(), list, storeTimeCol);
                                } catch (InterruptedException e) {
                                    LOG.warn("Interrupted when putting uid into task.pendingQueue.");
                                }
                                currentUid.incrementAndGet();
                                list = new LinkedList<Map<String, Object>>();
                            }
                        }

                        while (pending.size() > 0 && !error.get()) {
                            SystemUtils.sleep(5000);
                        }
                    } catch (ProducerScheduler.UnRegisteredException e) {
                        LOG.warn("task {} had been unregisted or never register before transfer.", qid);
                    } finally {
                        if (key != null)
                            producerScheduler.unregister(key);
                        producerScheduler.close();
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
                finish = true;
                runningTask.decrementAndGet();
                this.progress.setState(error.get() ? 3 : 2);
                this.progress.setEndTime(System.currentTimeMillis() / 1000);
                if (errMss.length() > 0)
                    this.progress.setErrmsg(errMss.toString());
                endTime = System.currentTimeMillis();

                //update bees.databus_statistics set schedule_id=?, exec_end=?, row_count=?, state=?, err_msg=? where qid=?
                try (Connection conn = DriverManager.getConnection(url, username, password);) {
                    PreparedStatement statement = conn.prepareStatement(UPDATE_SQL);
                    statement.setString(1, schedule_id);
                    statement.setTimestamp(2,  new Timestamp(progress.endTime * 1000));
                    statement.setLong(3, transfered.get());
                    statement.setInt(4, this.progress.getState());
                    statement.setString(5, progress.errmsg == null ? null : progress.errmsg.substring(0, Math.min(progress.errmsg.length(), 512)));
                    statement.setString(6, qid);
                    int success = statement.executeUpdate();
                    statement.close();
                } catch (SQLException e) {
                    LOG.warn("faled to update log into mysql.", e);
                }
                LOG.info("Task {} finished with state {}, transferd row {}, rate {}.", this.qid, this.progress.getState(), this.transfered.get(), count.get() == 0 ? 0 : spended.get() / count.get());
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
        if (conf == null) {
            conf = new HashMap<String, String>();
            cq.setConf(conf);
        }
        conf.put("hcat.query.return.fetch", "true");
        int retry = 0;
        CommitQueryReply reply = null;
        for (String address : hcatServer) {
            try(THttpClient thc = new THttpClient(address);) {
                TProtocol lopFactory = new TBinaryProtocol(thc);
                CliService.Client client = new CliService.Client(lopFactory);
                reply = client.commit(cq);
                Task task = new Task(reply.getHandle().getQueryId(), address, cq.getConf());
                qid2Task.put(task.qid, task);
                taskQueue.add(task);

                try (Connection conn = DriverManager.getConnection(url, username, password);) {
                    PreparedStatement statement = conn.prepareStatement(INSERT_SQL);
                    statement.setString(1, reply.handle.queryId);
                    statement.setString(2, serverTag);
                    statement.setString(3,null);
                    statement.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
                    statement.setTimestamp(5, null);
                    statement.setLong(6, 0);
                    statement.setInt(7, 1);
                    statement.setString(8, null);
                    statement.setString(9, null);
                    boolean success = statement.execute();
                    statement.close();
                } catch (SQLException e) {
                    LOG.warn("faled to insert log into mysql.", e);
                }
                break;
            } catch (RuntimeException e) {
                retry++;
                if (retry < hcatServer.size())
                    continue;
                throw new AuthorizationException(e.getMessage());
            } catch (AuthorizationException e) {
                throw e;
            } catch(Exception e) {
                retry++;
                if (retry < hcatServer.size())
                    continue;
                throw new AuthorizationException(e.toString());
            }
        }
        return reply;
    }

    @Override
    public QueryStatusReply queryJobStatus(QueryStatus qs) throws NotFoundException, TException {
        QueryStatusReply reply = new QueryStatusReply();
        Task task = qid2Task.get(qs.queryId);
        if (task == null) {
            for (int i = 0; i < hcatServer.size(); i++) {
                try(THttpClient thc = new THttpClient(hcatServer.get(i));
                    Connection conn = DriverManager.getConnection(url, username, password)) {
                    TProtocol lopFactory = new TBinaryProtocol(thc);
                    CliService.Client client = new CliService.Client(lopFactory);
                    reply = client.queryJobStatus(qs);
                    PreparedStatement statement = conn.prepareStatement(SELECT_SQL);
                    statement.setString(1, qs.getQueryId());
                    ResultSet resultSet = statement.executeQuery();
                    if(resultSet.next()) {
                        int state = resultSet.getInt(1);
                        Timestamp startTime = resultSet.getTimestamp(2);
                        Timestamp endTime = resultSet.getTimestamp(3);
                        String errMsg = resultSet.getString(4);
                        reply.queryProgress.setState(state);
                        reply.queryProgress.setStartTime(startTime.getTime()/1000);
                        reply.queryProgress.setEndTime(endTime.getTime()/1000);
                        reply.queryProgress.setErrmsg(errMsg == null ? "" : errMsg);
                    }
                    return reply;
                } catch (Throwable e) {
                    if (i == hcatServer.size() - 1)
                        throw new TException(e);
                }
            }
        }

        if(!task.error.get())
            task.progress.setErrmsg(String.format("sended %d, currentRate %d ms, maxRate %d ms, total time %d s.", task.transfered.get(), task.currentRate, task.maxRate, task.spended.get()/1000));
        reply.setQueryProgress(task.progress);
        return reply;
    }

    @Override
    public CancelQueryReply cancelJob(CancelQuery cq) throws AuthorizationException, NotFoundException, TException {
        CancelQueryReply reply = new CancelQueryReply();

        Task task = qid2Task.get(cq.queryId);
        if (task == null)
            throw new NotFoundException("Databus Not found qid " + cq.queryId);

        if (task.finish)
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

    private String driverName;
    private String url;
    private String username;
    private String password;

    private int maxIdle = GenericObjectPool.DEFAULT_MAX_IDLE;
    private int minIdle = GenericObjectPool.DEFAULT_MIN_IDLE;
    private int maxTotal = GenericObjectPool.DEFAULT_MAX_ACTIVE;
    private long maxWaitMillis = GenericObjectPool.DEFAULT_MAX_WAIT;


    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(int max) {
        maxIdle = max;
    }

    public int getMinIdle() {
        return minIdle;
    }

    public void setMinIdle(int min) {
        minIdle = min;
    }

    public long getMaxWaitMillis() {
        return maxWaitMillis;
    }

    public void setMaxWaitMillis(long maxWaitMillis) {
        this.maxWaitMillis = maxWaitMillis;
    }

    public int getMaxTotal() {
        return maxTotal;
    }

    public void setMaxTotal(int maxTotal) {
        this.maxTotal = maxTotal;
    }

}
