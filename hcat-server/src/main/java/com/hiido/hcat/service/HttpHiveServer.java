package com.hiido.hcat.service;

import java.io.*;
import java.lang.reflect.Field;
import java.security.*;
import java.sql.*;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.hiido.hcat.CompanyInfo;
import com.hiido.hcat.common.util.IOUtils;
import com.hiido.hcat.hive.HiveConfConstants;
import com.hiido.hcat.thrift.protocol.*;
import com.hiido.hcat.thrift.protocol.AuthorizationException;
import com.hiido.hcat.thrift.protocol.RuntimeException;
import com.hiido.hva.thrift.protocol.*;
import com.hiido.suit.common.util.ConnectionPool;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.http.HttpHost;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.processors.HiveCommand;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.operation.Operation;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.cli.operation.SQLOperation;
import org.apache.http.HttpResponse;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.nio.DefaultHttpClientIODispatch;
import org.apache.http.impl.nio.pool.BasicNIOConnPool;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.nio.protocol.BasicAsyncRequestProducer;
import org.apache.http.nio.protocol.BasicAsyncResponseConsumer;
import org.apache.http.nio.protocol.HttpAsyncRequestExecutor;
import org.apache.http.nio.protocol.HttpAsyncRequester;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOEventDispatch;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.params.SyncBasicHttpParams;
import org.apache.http.protocol.*;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import com.hiido.hcat.common.util.SystemUtils;
import com.hiido.hcat.service.cli.HcatSession;
import com.hiido.suit.TokenVerifyStone;
import org.apache.thrift.transport.THttpClient;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.apache.http.conn.ssl.SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER;

public class HttpHiveServer implements CliService.Iface, SignupService.Iface {
    private static final Logger LOG = Logger.getLogger(HttpHiveServer.class);
    static String KEY_STORE_CLIENT_PATH = "hvaclient.keystore";
    static String KEY_STORE_TRUST_PATH = "tclient.keystore";
    static String KEY_STORE_PASSWORD = "hiidosyshva";
    static String KEY_STORE_TRUST_PASSWORD = "hiidosyshva";

    public static final SSLConnectionSocketFactory sslsf;

    static {
        try(InputStream ksIn = Thread.currentThread().getContextClassLoader().getResourceAsStream(KEY_STORE_CLIENT_PATH);
            InputStream tsIn = Thread.currentThread().getContextClassLoader().getResourceAsStream(KEY_STORE_TRUST_PATH);) {
            KeyStore keyStore  = KeyStore.getInstance("JKS");
            KeyStore trustStore  = KeyStore.getInstance("JKS");
            keyStore.load(ksIn, KEY_STORE_PASSWORD.toCharArray());
            trustStore.load(tsIn, KEY_STORE_TRUST_PASSWORD.toCharArray());
            SSLContextBuilder builder = new SSLContextBuilder();
            builder.loadKeyMaterial(keyStore, KEY_STORE_TRUST_PASSWORD.toCharArray());
            builder.loadTrustMaterial(trustStore, new TrustSelfSignedStrategy());
            sslsf = new SSLConnectionSocketFactory(
                    builder.build(), ALLOW_ALL_HOSTNAME_VERIFIER);
        } catch (Exception e) {
            throw new java.lang.RuntimeException("failed to init HttpHiveServer: " + e.toString());
        }
    }

    private final int port;
    private final String serverTag;
    private final long maxHistoryTask = 1000 * 60 * 60;
    private final AtomicLong qidSeq = new AtomicLong();
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");

    private long historyTaskLife = 3600000;
    private int maxThreads = 10;
    private int minThreads = 2;
    private int maxIdleTimeMs = 30000;

    private HttpServer server;
    private TokenVerifyStone tokenVerifyStone;
    private Configuration conf;
    private Map<String, Task> qid2Task = new ConcurrentHashMap<String, Task>();
    private BlockingQueue<Task> taskBlockQueue = new LinkedBlockingQueue<Task>();
    private final BlockingQueue<HcatQuery> sqlQueue = new LinkedBlockingQueue<HcatQuery>();
    private final HttpAsyncRequester requester;
    private final BasicNIOConnPool pool;
    private final HttpHost httpHost;
    private final HttpParams params;

    private final AtomicBoolean close = new AtomicBoolean(false);
    private final AtomicBoolean closeSignal = new AtomicBoolean(false);
    private final AtomicInteger runningTask = new AtomicInteger(0);

    private static Map<Integer, CompanyInfo> id2Company = new ConcurrentHashMap<Integer, CompanyInfo>();

    public void setConnPool(ConnectionPool connPool) {
        this.connPool = connPool;
    }

    private ConnectionPool connPool;

    public int getPort() {
        return port;
    }

    public int getMaxThreads() {
        return maxThreads;
    }

    public void setMaxThreads(int maxThreads) {
        this.maxThreads = maxThreads;
    }

    public int getMinThreads() {
        return minThreads;
    }

    public void setMinThreads(int minThreads) {
        this.minThreads = minThreads;
    }

    public int getMaxIdleTimeMs() {
        return maxIdleTimeMs;
    }

    public void setMaxIdleTimeMs(int maxIdleTimeMs) {
        this.maxIdleTimeMs = maxIdleTimeMs;
    }

    public String getServerTag() {
        return serverTag;
    }

    public long getMaxHistoryTask() {
        return maxHistoryTask;
    }

    public long getHistoryTaskLife() {
        return historyTaskLife;
    }

    public void setHistoryTaskLife(long historyTaskLife) {
        this.historyTaskLife = historyTaskLife;
    }

    private TokenVerifyStone VerifyStone;

    private final OperationManager operationManager;

    public HttpHiveServer(String tag, int port, String billHost) throws IOReactorException {
        this.port = port;
        serverTag = createServerTag("0.0.0.0", port, tag);
        operationManager = new OperationManager();
        params = new SyncBasicHttpParams();
        params.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 5000);
        params.setParameter(CoreConnectionPNames.SO_TIMEOUT, 5000);
        params.setParameter(CoreConnectionPNames.SO_KEEPALIVE, false);
        httpHost = new HttpHost(billHost, 8080, "http");
        HttpProcessor httpproc = HttpProcessorBuilder.create()
                // Use standard client-side protocol interceptors
                .add(new RequestContent())
                .add(new RequestTargetHost())
                .add(new RequestConnControl())
                .add(new RequestUserAgent("http/1.1"))
                .add(new RequestExpectContinue(true)).build();
        // Create client-side HTTP protocol handler
        HttpAsyncRequestExecutor protocolHandler = new HttpAsyncRequestExecutor();
        // Create client-side I/O event dispatch
        final IOEventDispatch ioEventDispatch = new DefaultHttpClientIODispatch(protocolHandler,
                ConnectionConfig.DEFAULT);
        // Create client-side I/O reactor
        final ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
        // Create HTTP connection pool
        pool = new BasicNIOConnPool(ioReactor, params);
        // Limit total number of connections to just two
        pool.setDefaultMaxPerRoute(10);
        pool.setMaxTotal(20);
        // Run the I/O reactor in a separate thread
        Thread t = new Thread(new Runnable() {

            public void run() {
                try {
                    ioReactor.execute(ioEventDispatch);
                } catch (Exception e) {
                    LOG.error("I/O error: " + e.getMessage());
                }
                LOG.info("shutdown ioReactor thread.");
            }

        });
        t.start();
        requester = new HttpAsyncRequester(httpproc);
    }

    private final AtomicLong commitTotal = new AtomicLong();

    public void start() throws Exception {
        conf = new Configuration();
        conf.addResource("spark-site.xml");

        HttpServer.Builder builder = new HttpServer.Builder();
        server = builder.setName("hiido").setHost("0.0.0.0").setPort(this.port).setMaxThreads(maxThreads).setMinThreads(minThreads).setMaxIdleTimeMs(maxIdleTimeMs)
                .setConf(new HiveConf(conf, this.getClass())).setUseSSL(false).build();
        server.addServlet("query", "/query", new QueryServlet(new CliService.Processor<HttpHiveServer>(this),
                new TBinaryProtocol.Factory(true, true)));
        server.addServlet("signup", "/signup", new SignUpServlet(new SignupService.Processor<HttpHiveServer>(this),
                new TBinaryProtocol.Factory(true, true)));
        server.addServlet("reject", "/reject", new RejectServlet());
        Thread disper = new Thread(new Disper(), "task-dipser");
        Thread queryDB = new Thread(new QueryDB(), "queryDb");

        HiveConf conf = new HiveConf();
        conf.addResource("spark-site.xml");
        SparkSessionManagerImpl.getInstance().setup(conf);

        disper.start();
        queryDB.start();
        server.start();
    }

    private void close() throws Exception {
        closeSignal.set(true);
        SparkSessionManagerImpl.getInstance().shutdown();
        server.stop();
    }

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

    final class Task implements Runnable {
        final String qid;
        final boolean quick;
        final boolean reqFetch;
        final BitSet bitSet;
        final List<String> query;
        Map<String, String> confOverlay;
        HcatSession session;
        String companyId;
        String userId;
        CompanyInfo companyInfo;
        String engine = "mapreduce";

        volatile int running = 0;
        QueryProgress qp = new QueryProgress();

        RuntimeException serverException;

        public Task(String qid, CompanyInfo companyInfo, HcatSession session, List<String> query, boolean quick, BitSet bitSet,
                    Map<String, String> confOverlay) {
            this.qid = qid;
            this.companyInfo = companyInfo;
            this.session = session;
            this.query = query;
            this.quick = quick;
            this.bitSet = bitSet;
            this.confOverlay = confOverlay;
            this.qp.setJobId(new LinkedList<String>()).setState(JobStatus.READY.getValue()).setN(query.size())
                    .setEngine("mapreduce").setErrmsg("").setIsFetchTask(false).setProgress(0.0);

            //for hcat-databus
            if(confOverlay != null & confOverlay.containsKey("hcat.query.return.fetch"))
                reqFetch = true;
            else
                reqFetch = false;
        }

        boolean isFinished() {
            int state = qp.getState();
            return state > 1;
        }

        public QueryProgress getProgress() {
            if (qp.state != JobStatus.RUNNING.getValue())
                qp.setProgress(0.0);
            else {
                int c1 = 0, c2 = 0;
                for (int i = 0; i < running; i++)
                    if (bitSet.get(i)) c1++;
                    else c2++;

                double p = 0.0;
                if (c1 == 0)
                    p = (session.getSessionState().getCurProgress() + c2) / (query.size());
                else
                    p = (session.getSessionState().getCurProgress() + c2) * 0.9 / (query.size() - bitSet.cardinality()) + (c1 * 0.1 / bitSet.cardinality());
                qp.setProgress(p == Double.NaN ? 0.0 : (p > 1.0 ? 0.99 : p));
                qp.setJobId(session.getSessionState().getJobs());
            }

            if (StringUtils.isEmpty(qp.errmsg) && serverException != null)
                qp.setErrmsg(serverException.msg);
            return qp;
        }

        public void cancel() throws HiveSQLException {
            synchronized (qp) {
                qp.state = JobStatus.CANCEL.getValue();
            }
            session.cancel();
        }

        @Override
        public void run() {
            try {
                HiveConf hiveConf = session.getHiveConf();
                hiveConf.setVar(HiveConf.ConfVars.HIVEQUERYID, qid);
                if (companyInfo != null && companyInfo.getJobQueue() != null)
                    hiveConf.set("mapred.job.queue.name", companyInfo.getJobQueue());
                session.open(confOverlay);
                session.getSessionState().setHiidoUserId(userId == null ? 0 : Integer.parseInt(userId));
                session.getSessionState().setHiidoCompanyId((companyId == null ? 0 : Integer.parseInt(companyId)));
                if (companyInfo != null) {
                    session.getSessionState().setCompanyName(companyInfo.getName());
                    session.getSessionState().setCompanyHdfs(companyInfo.getHdfs());
                }
                synchronized (qp) {
                    if (qp.state == JobStatus.CANCEL.getValue())
                        return;
                    qp.state = JobStatus.RUNNING.getValue();
                    qp.startTime = System.currentTimeMillis() / 1000;
                }

                boolean loadedSparkConf = false;
                for (String q : query) {
                    // 前端不支持输出多个select查询结果
                    if (qp.isFetchTask)
                        break;

                    synchronized (qp) {
                        if (qp.state == JobStatus.CANCEL.getValue())
                            break;
                    }
                    OperationHandle handle = session.executeStatement(q, confOverlay);
                    running++;
                    if (handle == null)
                        continue;

                    if (!loadedSparkConf) {
                        String engine = hiveConf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);
                        if (engine.equals("spark")) {
                            this.engine = engine;
                            hiveConf.addResource("spark-site.xml");
                            if (companyInfo != null && companyInfo.getJobQueue() != null)
                                hiveConf.set("spark.yarn.queue", companyInfo.getJobQueue());
                            loadedSparkConf = true;
                        }
                    }
                    Operation operation = operationManager.getOperation(handle);
                    if (operation instanceof SQLOperation) {
                        SQLOperation sqlOpt = (SQLOperation) operation;
                        if (!sqlOpt.hasResultSet())
                            continue;

                        Field f = SQLOperation.class.getDeclaredField("driver");
                        f.setAccessible(true);
                        Driver d = (Driver) f.get(sqlOpt);

                        if (!(qp.isFetchTask = d.isFetchingTable()))
                            continue;

                        d.resetFetch();
                        Field field = Driver.class.getDeclaredField("fetchTask");
                        field.setAccessible(true);
                        FetchTask fetch = (FetchTask) field.get(d);

                        FetchWork work = fetch.getWork();
                        Path tblDir = work.getTblDir();
                        LOG.debug("fetch's path is " + (tblDir == null ? "null" : tblDir.toString()));

                        boolean needToMove = true;
                        Path scratchdir = new Path(session.getHiveConf().get(HiveConf.ConfVars.SCRATCHDIR.varname));
                        String scratchdirStr = Path.getPathWithoutSchemeAndAuthority(scratchdir).toString();

                        if (work.getPathLists() == null)
                            continue;

                        for (Path p : work.getPathLists()) {
                            if (!Path.getPathWithoutSchemeAndAuthority(p).toString().startsWith(scratchdirStr))
                                needToMove = false;
                            FileSystem fs = p.getFileSystem(conf);
                            FileStatus[] stats = fs.listStatus(p);
                            if (stats != null)
                                for (FileStatus s : stats)
                                    qp.resSize += s.getLen();
                        }

                        if ((!reqFetch) && needToMove) {
                            FileSystem fs = work.getPathLists().get(0).getFileSystem(session.getHiveConf());
                            Path parent = new Path(new Path(tblDir.toUri().getScheme(), tblDir.toUri().getAuthority(), conf.get("hcat.mr.resultDir")), this.qid);
                            if (!fs.exists(parent))
                                fs.mkdirs(parent);

                            if (!work.isPartitioned()) {
                                fs.rename(work.getTblDir(), parent);
                                work.setTblDir(new Path(parent, work.getTblDir().getName()));
                                qp.res = work.getTblDir().toString();
                                LOG.debug(String.format("%s result is %s", qid, qp.res));
                            } else if (work.getPartDir() != null) {
                                ArrayList<Path> partDir = new ArrayList<Path>(work.getPartDir().size());
                                for (Path child : work.getPartDir()) {
                                    fs.rename(child, parent);
                                    partDir.add(new Path(parent, child.getName()));
                                }
                                work.setPartDir(partDir);
                            }
                        //for hcat-databus
                        } else if(reqFetch) {
                            qp.fetchDirs = new LinkedList<String>();
                            qp.fetchDirs.add(SerializationUtilities.serializeObject(fetch));
                        }

                        TableSchema schema = sqlOpt.getResultSetSchema();
                        qp.fields = new LinkedList<com.hiido.hcat.thrift.protocol.Field>();
                        for (ColumnDescriptor column : schema.getColumnDescriptors()) {
                            com.hiido.hcat.thrift.protocol.Field col = new com.hiido.hcat.thrift.protocol.Field(
                                    column.getName(), column.getTypeName());
                            qp.fields.add(col);
                        }
                    }
                }
                qp.state = JobStatus.COMPLETE.getValue();
            } catch (HiveSQLException e) {
                if (session.getErr() != null)
                    qp.setErrmsg(session.getErr().returnAndClear());
                if (StringUtils.isEmpty(qp.errmsg))
                    qp.setErrmsg(e.toString());
                qp.setState(JobStatus.FAILURE.getValue());
                LOG.error("[" + qid + "]", e);
            } catch (SecurityException e) {
                String msg = e.getMessage();
                if (msg != null && (msg.equals(HcatSecurityManager.ExitStatus.NORMAL.name()))) {
                    LOG.warn("Task thread do not support System.exit with '0' argument.", e);
                } else {
                    qp.setState(JobStatus.FAILURE.getValue());
                    LOG.warn("Task thread do not support System.exit function.", e);
                }

            } catch (Exception e) {
                LOG.error("[" + qid + "]", e);
                qp.setState(JobStatus.FAILURE.getValue());
                qp.setErrmsg("The server threw an exception, please contact the administrator.");
                this.serverException = new RuntimeException(
                        "The server threw an exception, please contact the administrator.");
            } finally {
                try {
                    updateQueryRecord(qid, qp.state, qp.getRes(), qp.resSize, qp.jobId, qp.fields);
                    qp.endTime = System.currentTimeMillis() / 1000;
                    this.confOverlay = null;
                    session.close();
                    session = null;
                    LOG.info(String.format("finish %s with state %d", qid, this.qp.state));

                    for (int i = 0; i < qp.jobId.size(); i++) {
                        URIBuilder builder = new URIBuilder();
                        builder.setPath("/api/postFinishJob.do").addParameter("company_id", companyId == null ? "1" : companyId)
                                .addParameter("user_id", userId == null ? "1" : userId)
                                .addParameter("jobid", qp.jobId.get(i).replace("job", "application"))
                                .addParameter("type", engine)
                                .addParameter("qid", this.qid);
                        BasicHttpRequest request = new BasicHttpRequest("GET", builder.toString());
                        HttpCoreContext coreContext = HttpCoreContext.create();
                        requester.execute(
                                new BasicAsyncRequestProducer(httpHost, request),
                                new BasicAsyncResponseConsumer(),
                                pool,
                                coreContext,
                                new FutureCallback<HttpResponse>() {

                                    public void completed(final HttpResponse response) {
                                        LOG.debug(response.toString());
                                    }

                                    public void failed(final Exception ex) {
                                        LOG.error("failed to connect to bill host.", ex);
                                    }

                                    public void cancelled() {
                                    }
                                });
                    }
                } catch (HiveSQLException e) {
                    LOG.error("close session wrong.", e);
                } finally {
                    runningTask.decrementAndGet();
                }
            }
        }
    }

    private final class Disper implements Runnable {

        private long cleanTaskInterval = 1000 * 60 * 60;
        private long lastCleanTime = System.currentTimeMillis();

        public void run() {
            while (!closeSignal.get()) {
                Task task = null;
                try {
                    task = taskBlockQueue.poll(3, TimeUnit.SECONDS);
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
    }

    public class SignUpServlet extends AbstractTServlet {
        public SignUpServlet(TProcessor processor, TProtocolFactory protocolFactory) {
            super(processor, protocolFactory);
        }
    }

    public class QueryServlet extends AbstractTServlet {

        private static final long serialVersionUID = -1894457636155490510L;

        public QueryServlet(TProcessor processor, TProtocolFactory protocolFactory) {
            super(processor, protocolFactory);
        }
    }

    public class RejectServlet extends HttpServlet {

        public void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            InputStreamReader reader = new InputStreamReader(req.getInputStream());

            String key = "hiidosys_closeserverkey";
            char[] array = new char[key.length()];
            reader.read(array, 0, key.length());
            if(!key.equals(String.valueOf(array)))
                return;
            if(!close.get())
                close.set(true);
            else
                return;

            resp.getOutputStream().flush();
            LOG.info("closing HttpHiveServer......");
            final Thread t = new Thread(new SoftReject());
            t.start();
            try {
                t.join();
            } catch (InterruptedException e) {
            }
        }

    }

    private int cleanTaskHistory(boolean force) {
        int count = 0;
        long now = System.currentTimeMillis();
        if (qid2Task.size() > maxHistoryTask) {
            Set<String> tkeys = new HashSet<String>(qid2Task.keySet());
            for (String k : tkeys) {
                Task t = qid2Task.get(k);
                if (t == null) {
                    continue;
                }
                boolean clean = t.isFinished() && (force || t.qp.endTime * 1000 - now >= historyTaskLife);
                if (clean) {
                    qid2Task.remove(k);
                    count++;
                }
            }
        }
        return count;
    }


    private QueryProgress getStatusFromDb(String qid) {
        HcatQuery query = new HcatQuery();
        query.setQid(qid);
        query.setOperation(HcatQuery.DbOperation.SELECT);
        boolean err = false;
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet set = null;
        QueryProgress progress = new QueryProgress();
        //required fields
        progress.setN(0);
        progress.setEngine("mapreduce");
        progress.setErrmsg("");
        progress.setProgress(0.0f);
        try {
            conn = connPool.acquire();
            pstmt = HcatQuery.createStatement(conn, query);
            set = pstmt.executeQuery();
            boolean hasRecord = false;
            while (set.next()) {
                hasRecord = true;
                progress.setEndTime(set.getTimestamp(1) == null ? 0l : set.getTimestamp(1).getTime());
                progress.setState(set.getInt(2));
                progress.setRes(set.getString(3));
                progress.setJobId(HcatQuery.convertJobIds(set.getString(4)));
                progress.setFields(HcatQuery.convertFieldList(qid, set.getString(5)));
                progress.setIsFetchTask(set.getBoolean(6));
                progress.setResSize(set.getLong(7));
            }
            return hasRecord ? progress : null;
        } catch (Exception e) {
            LOG.error(String.format("failed search qid %s from database.", qid), e);
            err = true;
            return null;
        } finally {
            IOUtils.closeIO(set);
            IOUtils.closeIO(pstmt);
            connPool.release(conn, err);
        }

    }


    private void commitQueryRecord(String qid, String qStr, String bususer, boolean isQuick) {
        HcatQuery query = new HcatQuery();
        query.setQid(qid);
        query.setQuick(isQuick);
        query.setCommitter(serverTag);
        query.setUser(bususer);
        query.setState(1);
        query.setOperation(HcatQuery.DbOperation.INSERT);
        sqlQueue.add(query);
    }

    private void updateQueryRecord(String qid, int state, String resourcedir, long resSize, List<String> jobs, List<com.hiido.hcat.thrift.protocol.Field> fields) {
        HcatQuery query = new HcatQuery();
        query.setQid(qid);
        query.setState(state);
        query.setExec_end(new Timestamp(System.currentTimeMillis()));
        query.setResourcedir(resourcedir);
        query.setResSize(resSize);
        if (jobs != null && jobs.size() > 3)
            query.setJobIds(jobs.subList(jobs.size() - 2, jobs.size()));
        else
            query.setJobIds(jobs);
        query.setFieldList(fields);
        query.setOperation(HcatQuery.DbOperation.UPDATE);
        sqlQueue.add(query);
    }

    private final class QueryDB implements Runnable {
        java.sql.Connection conn = null;
        java.sql.PreparedStatement pstmt = null;
        int commitQueryTryCount = 2;
        List<HcatQuery> list = new LinkedList<HcatQuery>();

        public QueryDB() {
        }

        public void run() {
            while (closeSignal.get()) {
                try {
                    list.clear();
                    HcatQuery query = sqlQueue.poll(5, TimeUnit.SECONDS);
                    if (query == null)
                        continue;
                    list.add(query);
                    if (sqlQueue.size() > 10) {
                        for (int i = 0; i < 10; i++) {
                            if (list.get(0).getoperation() == sqlQueue.peek().getoperation())
                                list.add(sqlQueue.poll());
                            else
                                break;
                        }
                    }
                } catch (InterruptedException e) {
                    LOG.warn("QueryDB thread is interrupted :" + e.toString());
                    list.clear();
                    continue;
                }

                int count = 0;
                boolean err = false;
                while ((++count) <= commitQueryTryCount) {
                    try {
                        conn = connPool.acquire();
                        pstmt = conn.prepareStatement(list.get(0).getoperation().sql);
                        for (HcatQuery q : list) {
                            HcatQuery.prepareStatement(pstmt, q);
                            pstmt.addBatch();
                        }
                        pstmt.executeBatch();
                    } catch (BatchUpdateException e) {
                        err = true;
                        int[] results = e.getUpdateCounts();
                        for (int i = results.length - 1; i >= 0; i--)
                            if (results[i] != java.sql.Statement.EXECUTE_FAILED)
                                list.remove(i);
                        StringBuilder str = new StringBuilder();
                        for (HcatQuery q : list) {
                            str.append(q.getQid());
                            str.append(";");
                        }
                        LOG.error(String.format("[fatal] Failed to commit sql[%s] tryed[%d]", str.toString(), count), e);
                    } catch (Throwable e) {
                        err = true;
                        StringBuilder str = new StringBuilder();
                        for (HcatQuery q : list) {
                            str.append(q.getQid());
                            str.append(";");
                        }
                        LOG.error(String.format("[fatal] Failed to commit sql[%s] tryed[%d]", str.toString(), count), e);
                    } finally {
                        IOUtils.closeIO(pstmt);
                        connPool.release(conn, err);
                        if (!err) {
                            list.clear();
                            break;
                        }
                    }
                }
            }
        }
    }

    @Override
    public SignupReply signup(int companyId, String companyName, int userId) throws AuthorizationException, RuntimeException {
        Connection conn = null;
        boolean err = false;
        String namenode = null;
        try (CloseableHttpClient httpclient = HttpClients.custom().setSSLSocketFactory(HttpHiveServer.sslsf).build()) {
            THttpClient thc = new THttpClient(HiveConfConstants.getHcatHvaserver(conf), httpclient);
            TProtocol lopFactory = new TBinaryProtocol(thc);
            HvaService.Client hvaClient =new HvaService.Client(lopFactory);

            Set<Obj> authSet = new HashSet<Obj>();
            authSet.add(new Obj(companyName, "company", (byte) 15));
            Reply reply = hvaClient.validate("hcat", new HiidoUser(userId, companyId), authSet);
            if (reply.getRecode() != Recode.SUCESS) {
                LOG.warn("failed sign up, message : " + reply.message);
                return new SignupReply(Recode.FAILURE.getValue(), reply.message);
            }

            conn = connPool.acquire();
            PreparedStatement statement = conn.prepareStatement("select company_name from hiidoid.company_hcat where company_name = ?");
            statement.setString(1, companyName);
            ResultSet result = statement.executeQuery();
            if (result.next())
                return new SignupReply(Recode.FAILURE.getValue(), "Already exists same name of company.");
            result.close();
            result = statement.executeQuery("select namenode_fs_uri from hiidoid.hive_namenodes where enable = 'Y'");
            if (result.next()) {
                namenode = result.getString(1);
            } else {
                String message = "There is no available namenode for new company.";
                LOG.error(message);
                return new SignupReply(Recode.FAILURE.getValue(), message);
            }
            result.close();
            Path parent = new Path(namenode, "company");
            FileSystem fs = parent.getFileSystem(conf);
            if (!fs.exists(parent))
                fs.mkdirs(parent);
            Path dir = new Path(parent, companyName);
            if (fs.exists(dir))
                return new SignupReply(Recode.FAILURE.getValue(), "company hdfs is exists.");
            fs.mkdirs(dir);
            fs.mkdirs(new Path(dir, HiveConfConstants.getHcatWarehosueDirName(conf)));
            fs.mkdirs(new Path(dir, ".tmp"));
            statement = conn.prepareStatement("insert into hiidoid.company_hcat (company_id, company_name, hdfs) values(?, ?, ?)");
            statement.setInt(1, companyId);
            statement.setString(2, companyName);
            statement.setString(3, dir.toUri().toString());
            statement.execute();
            statement.close();

            reply = hvaClient.setPrivileges("hcat", userId, companyName, "company", (byte) 15);
            if (reply.getRecode() != Recode.SUCESS) {
                LOG.error("new validation message : " + reply.message);
                return new SignupReply(Recode.FAILURE.getValue(), reply.message);
            } else
                return new SignupReply(Recode.SUCESS.getValue(), "");
        } catch (SQLException e) {
            err = true;
            LOG.error("failed when connect to mysql server.", e);
            throw new RuntimeException("failed when connect to mysql server.");
        } catch (Exception e) {
            LOG.error("failed to sign up.", e);
            throw new RuntimeException("failed to signup, remote server runtime error.");
        } finally {
            connPool.release(conn, err);
        }
    }

    @Override
    public CommitQueryReply commit(CommitQuery cq) throws AuthorizationException, RuntimeException {
        if(close.get())
            throw new RuntimeException("Server is closing.");
        // 1. 权限验证
        String companyId = cq.cipher.get("company_id");
        String userId = cq.cipher.get("user_id");
        String bususer = cq.cipher.get("bususer");

        if (id2Company.get(Integer.valueOf(companyId)) == null || System.currentTimeMillis() - id2Company.get(Integer.valueOf(companyId)).getUpdateTime() > 24 * 60 * 60 * 1000) {
            synchronized (id2Company) {
                if (id2Company.get(Integer.valueOf(companyId)) == null || System.currentTimeMillis() - id2Company.get(Integer.valueOf(companyId)).getUpdateTime() > 24 * 60 * 60 * 1000) {
                    java.sql.Connection conn = null;
                    boolean err = false;
                    try {
                        conn = connPool.acquire();
                        PreparedStatement stmt = conn.prepareStatement("select company_id, company_name,job_queue, hdfs FROM hiidoid.`company_hcat`");
                        ResultSet result = stmt.executeQuery();
                        while (result.next()) {
                            int cid = result.getInt(1);
                            String name = result.getString(2);
                            String queue = result.getString(3);
                            String hdfs = result.getString(4);
                            CompanyInfo companyInfo = new CompanyInfo(cid, name, queue, hdfs);
                            id2Company.put(cid, companyInfo);
                        }
                    } catch (Exception e) {
                        err = true;
                        LOG.error("failed to achieve hive_namenodes info.", e);
                    } finally {
                        connPool.release(conn, err);
                    }
                }
            }
        }
        CompanyInfo companyInfo = id2Company.get(Integer.valueOf(companyId));
        CommitQueryReply reply = new CommitQueryReply();
        String queryStr = cq.getQuery();
        String line;
        BufferedReader r = new BufferedReader(new StringReader(queryStr), 128);
        StringBuilder qsb = new StringBuilder();
        try {
            while ((line = r.readLine()) != null) {
                // Skipping through comments
                if (!line.startsWith("--")) {
                    qsb.append(line + "\n");
                }
            }
        } catch (IOException e) {
            LOG.error("failed when committing query.", e);
            throw new AuthorizationException("server-side exception when committing query.");
        }

        boolean quick = true;
        List<String> cmds = new LinkedList<String>();
        try {
            runningTask.incrementAndGet();
            Context ctx = new Context(conf, false);
            ParseDriver pd = new ParseDriver();
            String command = "";

            int pos = 0;
            BitSet bitSet = new BitSet();

            for (String oneCmd : qsb.toString().split(";")) {
                if (StringUtils.endsWith(oneCmd, "\\")) {
                    command += StringUtils.chop(oneCmd) + ";";
                    continue;
                } else {
                    command += oneCmd;
                }
                if (StringUtils.isBlank(command)) {
                    continue;
                }
                command = command.trim();
                boolean isQ = isQuickCmd(command, ctx, pd);
                bitSet.set(pos++, isQ);
                quick = quick & isQ;
                cmds.add(command);
                command = "";
            }

            String qid = String.format("%s_%s_%d", serverTag, sdf.format(new Date(System.currentTimeMillis())),
                    qidSeq.getAndIncrement());
            commitQueryRecord(qid, queryStr, bususer, quick);
            HiveConf hiveConf = new HiveConf();
            hiveConf.addToRestrictList(hiveConf.get("hcat.conf.restricted.list"));
            HcatSession session = new HcatSession(bususer, hiveConf);

            session.setOperationManager(operationManager);
            Task task = new Task(qid, companyInfo, session, cmds, quick, bitSet, cq.getConf());
            task.companyId = companyId;
            task.userId = userId;
            qid2Task.put(qid, task);
            if (quick) {
                task.run();
            } else {
                taskBlockQueue.add(task);
            }
            Handle handle = new Handle().setQuick(quick).setQueryId(qid).setTotalN(cmds.size()).setRunning(false).setStderr(task.getProgress().errmsg);
            reply.setHandle(handle);
            return reply;
        } catch (Exception e1) {
            runningTask.decrementAndGet();
            if(e1 instanceof AuthorizationException)
                throw (AuthorizationException)e1;
            else if (e1 instanceof RuntimeException)
                throw new AuthorizationException(((RuntimeException)e1).getMsg());
            else {
                throw new AuthorizationException().setMsg(e1.toString());
            }
        } finally {
        }
    }

    @Override
    public QueryStatusReply queryJobStatus(QueryStatus qs) throws NotFoundException, TException {
        Task task = qid2Task.get(qs.queryId);
        QueryProgress progress = null;
        if (task == null) {
            progress = getStatusFromDb(qs.getQueryId());
            if (progress == null) {
                LOG.warn("server not found qid : " + qs.queryId);
                throw new NotFoundException(String.format("Not found qid %s.", qs.queryId));
            }
        }
        try {
            QueryStatusReply reply = new QueryStatusReply();
            reply.setQueryProgress(task != null ? task.getProgress() : progress);
            return reply;
        } catch (Throwable e) {
            throw new RuntimeException(e.toString());
        }
    }

    @Override
    public CancelQueryReply cancelJob(CancelQuery cq) throws AuthorizationException, TException {
        CancelQueryReply reply = new CancelQueryReply();

        Task task = qid2Task.get(cq.queryId);
        if (task == null)
            throw new NotFoundException("Not found qid " + cq.queryId);
        try {
            task.cancel();
        } catch (HiveSQLException e) {
            LOG.error("[cancel] fail to cancel job +" + task.qid, e);
        }
        return reply;
    }

    @Override
    public LoadFileReply laodData(LoadFile lf) throws AuthorizationException, RuntimeException, TException {
        LOG.info("receive load data");
        throw new RuntimeException("not support load data.");
    }

    public void setTokenVerifyStone(TokenVerifyStone verifyStone) {
        this.tokenVerifyStone = verifyStone;
    }

    public TokenVerifyStone getTokenVerifyStone() {
        return tokenVerifyStone;
    }

    public static boolean isQuickCmd(String command, Context ctx, ParseDriver pd)
            throws AuthorizationException, RuntimeException {
        String[] tokens = command.split("\\s+");
        HiveCommand hiveCommand = HiveCommand.find(tokens, false);

        // not Driver
        if (hiveCommand != null)
            return true;
        boolean isQuick = true;
        try {
            ASTNode tree = pd.parse(command, ctx);
            tree = ParseUtils.findRootNonNullToken(tree);

            switch (tree.getType()) {
                case HiveParser.TOK_EXPLAIN:
                case HiveParser.TOK_EXPLAIN_SQ_REWRITE: // TODO
                case HiveParser.TOK_EXPORT:
                case HiveParser.TOK_IMPORT:
                case HiveParser.TOK_ROLLBACK:
                case HiveParser.TOK_SET_AUTOCOMMIT:
                case HiveParser.TOK_LOCKTABLE:
                case HiveParser.TOK_UNLOCKTABLE:
                case HiveParser.TOK_LOCKDB:
                case HiveParser.TOK_UNLOCKDB:
                case HiveParser.TOK_CREATEROLE:
                case HiveParser.TOK_DROPROLE:
                case HiveParser.TOK_GRANT:
                case HiveParser.TOK_LOAD:
                case HiveParser.TOK_SHOW_GRANT:
                case HiveParser.TOK_GRANT_ROLE:
                case HiveParser.TOK_REVOKE_ROLE:
                case HiveParser.TOK_SHOW_ROLE_GRANT:
                case HiveParser.TOK_SHOW_ROLE_PRINCIPALS:
                case HiveParser.TOK_SHOW_ROLES:
                    throw new AuthorizationException("not support operation : " + tree.getType());
                case HiveParser.TOK_ALTERTABLE:
                case HiveParser.TOK_ALTERVIEW:
                case HiveParser.TOK_CREATEDATABASE:
                case HiveParser.TOK_SWITCHDATABASE:
                case HiveParser.TOK_DROPTABLE:
                case HiveParser.TOK_DROPVIEW:
                case HiveParser.TOK_DESCDATABASE:
                case HiveParser.TOK_DESCTABLE:
                case HiveParser.TOK_DESCFUNCTION:
                case HiveParser.TOK_MSCK:
                case HiveParser.TOK_ALTERINDEX_REBUILD:
                case HiveParser.TOK_ALTERINDEX_PROPERTIES:
                case HiveParser.TOK_SHOWDATABASES:
                case HiveParser.TOK_SHOWTABLES:
                case HiveParser.TOK_SHOWCOLUMNS:
                case HiveParser.TOK_SHOW_TABLESTATUS:
                case HiveParser.TOK_SHOW_TBLPROPERTIES:
                case HiveParser.TOK_SHOW_CREATEDATABASE:
                case HiveParser.TOK_SHOW_CREATETABLE:
                case HiveParser.TOK_SHOWFUNCTIONS:
                case HiveParser.TOK_SHOWPARTITIONS:
                case HiveParser.TOK_SHOWINDEXES:
                case HiveParser.TOK_SHOWLOCKS:
                case HiveParser.TOK_SHOWDBLOCKS:
                case HiveParser.TOK_SHOW_COMPACTIONS:
                case HiveParser.TOK_SHOW_TRANSACTIONS:
                case HiveParser.TOK_SHOWCONF:
                case HiveParser.TOK_CREATEINDEX:
                case HiveParser.TOK_DROPINDEX:
                case HiveParser.TOK_ALTERTABLE_CLUSTER_SORT:
                case HiveParser.TOK_REVOKE:
                case HiveParser.TOK_ALTERDATABASE_PROPERTIES:
                case HiveParser.TOK_ALTERDATABASE_OWNER:
                case HiveParser.TOK_TRUNCATETABLE:
                case HiveParser.TOK_SHOW_SET_ROLE:
                case HiveParser.TOK_CREATEFUNCTION:
                case HiveParser.TOK_DROPFUNCTION:
                case HiveParser.TOK_RELOADFUNCTION:
                case HiveParser.TOK_ANALYZE:
                case HiveParser.TOK_CREATEMACRO:
                case HiveParser.TOK_DROPMACRO:
                case HiveParser.TOK_UPDATE_TABLE:
                case HiveParser.TOK_DELETE_FROM:
                case HiveParser.TOK_START_TRANSACTION:
                case HiveParser.TOK_COMMIT:
                    isQuick = true;
                    break;
                case HiveParser.TOK_CREATETABLE:
                    for (int i = 0; i < tree.getChildCount(); i++)
                        if (tree.getChild(i).getType() == HiveParser.TOK_QUERY) {
                            isQuick = false;
                            break;
                        }
                    // 默认是true，所以不需要判断
                    break;
                default:
                    isQuick = false;
            }
        } catch (AuthorizationException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("failed in parse sql .", e);
            throw new RuntimeException("failed in parse sql :" + e.toString());
        }
        return isQuick;
    }

    private class SoftReject implements Runnable {

        @Override
        public void run() {
            while(close.get()) {
                SystemUtils.sleep(5000);
                if(runningTask.get() <= 0  && sqlQueue.peek() == null && taskBlockQueue.peek() == null)
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
}
