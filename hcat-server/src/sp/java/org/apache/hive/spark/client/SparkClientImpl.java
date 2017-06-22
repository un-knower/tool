/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.spark.client;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;

import com.hiido.hcat.common.util.SystemUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.*;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.net.URI;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.Future;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hive.spark.client.rpc.Rpc;
import org.apache.hive.spark.client.rpc.RpcConfiguration;
import org.apache.hive.spark.client.rpc.RpcServer;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkException;
import org.apache.spark.deploy.HiidoSparkSubmit;
import org.apache.spark.deploy.yarn.HiidoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SparkClientImpl implements SparkClient {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(SparkClientImpl.class);
    //private static final Lock lock = new ReentrantLock();

    private static final long DEFAULT_SHUTDOWN_TIMEOUT = 10000; // In milliseconds
    private static final long MAX_ERR_LOG_LINES_FOR_RPC = 1000;

    private static final String OSX_TEST_OPTS = "SPARK_OSX_TEST_OPTS";
    private static final String SPARK_HOME_ENV = "SPARK_HOME";
    private static final String SPARK_HOME_KEY = "spark.home";
    private static final String DRIVER_OPTS_KEY = "spark.driver.extraJavaOptions";
    private static final String EXECUTOR_OPTS_KEY = "spark.executor.extraJavaOptions";
    private static final String DRIVER_EXTRA_CLASSPATH = "spark.driver.extraClassPath";
    private static final String EXECUTOR_EXTRA_CLASSPATH = "spark.executor.extraClassPath";

    private final Map<String, String> conf;
    private final HiveConf hiveConf;
    private final HiidoClient hiidoClient;
    private final Map<String, JobHandleImpl<?>> jobs;
    private final Rpc driverRpc;
    private final ClientProtocol protocol;
    private volatile boolean isAlive;

    SparkClientImpl(RpcServer rpcServer, Map<String, String> conf, HiveConf hiveConf) throws IOException, SparkException {
        this.conf = conf;
        this.hiveConf = hiveConf;
        this.jobs = Maps.newConcurrentMap();

        String clientId = UUID.randomUUID().toString();
        String secret = rpcServer.createSecret();
        this.protocol = new ClientProtocol();
        io.netty.util.concurrent.Future<Rpc> future = rpcServer.registerClient(clientId, secret, protocol);
        this.hiidoClient = startDriver(rpcServer, clientId, secret);

        String value = conf.get(HiveConf.ConfVars.SPARK_RPC_CLIENT_HANDSHAKE_TIMEOUT.varname);
        // The RPC server will take care of timeouts here.
        try {
            this.driverRpc = future.get(value != null ? Integer.parseInt(value) : 60000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            if(hiidoClient != null)
                hiidoClient.stop();
            rpcServer.cancelClient(clientId, "Error when client wait remote driver connect.");
            throw new IOException("Error when client wait remote driver connect.", e);
        } finally {
        }

        driverRpc.addListener(new Rpc.Listener() {
            @Override
            public void rpcClosed(Rpc rpc) {
                if (isAlive) {
                    LOG.error("Client RPC channel closed unexpectedly.");
                    isAlive = false;
                }
            }
        });
        isAlive = true;
    }

    @Override
    public <T extends Serializable> JobHandle<T> submit(Job<T> job) {
        return protocol.submit(job);
    }

    @Override
    public <T extends Serializable> Future<T> run(Job<T> job) {
        return protocol.run(job);
    }

    @Override
    public void stop() {
        if (isAlive) {
            isAlive = false;
            try {
                protocol.endSession();
            } catch (Exception e) {
                LOG.warn("Exception while waiting for end session reply.", e);
            } finally {
                driverRpc.close();
            }
        }
        if (hiidoClient.appId() != null) {
            try {
                ApplicationReport report = hiidoClient.getApplicationReport(hiidoClient.appId());
                YarnApplicationState state = report.getYarnApplicationState();
                if (state == YarnApplicationState.ACCEPTED || state == YarnApplicationState.NEW ||
                        state == YarnApplicationState.RUNNING || state == YarnApplicationState.SUBMITTED)
                    SystemUtils.sleep(DEFAULT_SHUTDOWN_TIMEOUT);
                LOG.info(String.format("Application report for %s(state: %s)", hiidoClient.appId(), report.getYarnApplicationState().name()));
            } catch (Exception e) {
                LOG.warn("close appid failed.", e);
            } finally {
                hiidoClient.stop();
            }
        }
    }

    @Override
    public Future<?> addJar(URI uri) {
        return run(new AddJarJob(uri.toString()));
    }

    @Override
    public Future<?> addFile(URI uri) {
        return run(new AddFileJob(uri.toString()));
    }

    @Override
    public Future<Integer> getExecutorCount() {
        return run(new GetExecutorCountJob());
    }

    @Override
    public Future<Integer> getDefaultParallelism() {
        return run(new GetDefaultParallelismJob());
    }

    @Override
    public boolean isActive() {
        return isAlive && driverRpc.isActive();
    }

    void cancel(String jobId) {
        protocol.cancel(jobId);
    }

    private HiidoClient startDriver(final RpcServer rpcServer, final String clientId, final String secret)
            throws IOException {
        final String serverAddress = rpcServer.getAddress();
        final String serverPort = String.valueOf(rpcServer.getPort());

        // If a Spark installation is provided, use the spark-submit script. Otherwise, call the
        // SparkSubmit class directly, which has some caveats (like having to provide a proper
        // version of Guava on the classpath depending on the deploy mode).
        String sparkHome = conf.get(SPARK_HOME_KEY);
        if (sparkHome == null) {
            sparkHome = System.getenv(SPARK_HOME_ENV);
        }
        if (sparkHome == null) {
            sparkHome = System.getProperty(SPARK_HOME_KEY);
        }
        String sparkLogDir = conf.get("hive.spark.log.dir");
        if (sparkLogDir == null) {
            if (sparkHome == null) {
                sparkLogDir = "./target/";
            } else {
                sparkLogDir = sparkHome + "/logs/";
            }
        }

        String osxTestOpts = "";
        if (Strings.nullToEmpty(System.getProperty("os.name")).toLowerCase().contains("mac")) {
            osxTestOpts = Strings.nullToEmpty(System.getenv(OSX_TEST_OPTS));
        }

        String driverJavaOpts = Joiner.on(" ").skipNulls().join(
                "-Dhive.spark.log.dir=" + sparkLogDir, osxTestOpts, conf.get(DRIVER_OPTS_KEY));
        String executorJavaOpts = Joiner.on(" ").skipNulls().join(
                "-Dhive.spark.log.dir=" + sparkLogDir, osxTestOpts, conf.get(EXECUTOR_OPTS_KEY));

        // Create a file with all the job properties to be read by spark-submit. Change the
        // file's permissions so that only the owner can read it. This avoid having the
        // connection secret show up in the child process's command line.
        final File properties = File.createTempFile("spark-submit.", ".properties");
        if (!properties.setReadable(false) || !properties.setReadable(true, true)) {
            throw new IOException("Cannot change permissions of job properties file.");
        }
        //FIXME
        //properties.deleteOnExit();

        Properties allProps = new Properties();
        // first load the defaults from spark-defaults.conf if available
        try {
            URL sparkDefaultsUrl = Thread.currentThread().getContextClassLoader().getResource("spark-defaults.conf");
            if (sparkDefaultsUrl != null) {
                LOG.info("Loading spark defaults: " + sparkDefaultsUrl);
                allProps.load(new ByteArrayInputStream(Resources.toByteArray(sparkDefaultsUrl)));
            }
        } catch (Exception e) {
            String msg = "Exception trying to load spark-defaults.conf: " + e;
            throw new IOException(msg, e);
        }
        // then load the SparkClientImpl config
        for (Map.Entry<String, String> e : conf.entrySet()) {
            allProps.put(e.getKey(), conf.get(e.getKey()));
        }

        //FIXME
        //allProps.put(SparkClientFactory.CONF_CLIENT_ID, clientId);
        //allProps.put(SparkClientFactory.CONF_KEY_SECRET, secret);
        allProps.put(DRIVER_OPTS_KEY, driverJavaOpts);
        allProps.put(EXECUTOR_OPTS_KEY, executorJavaOpts);

        String isTesting = conf.get("spark.testing");
        if (isTesting != null && isTesting.equalsIgnoreCase("true")) {
            String hiveHadoopTestClasspath = Strings.nullToEmpty(System.getenv("HIVE_HADOOP_TEST_CLASSPATH"));
            if (!hiveHadoopTestClasspath.isEmpty()) {
                String extraDriverClasspath = Strings.nullToEmpty((String) allProps.get(DRIVER_EXTRA_CLASSPATH));
                if (extraDriverClasspath.isEmpty()) {
                    allProps.put(DRIVER_EXTRA_CLASSPATH, hiveHadoopTestClasspath);
                } else {
                    extraDriverClasspath = extraDriverClasspath.endsWith(File.pathSeparator) ? extraDriverClasspath : extraDriverClasspath + File.pathSeparator;
                    allProps.put(DRIVER_EXTRA_CLASSPATH, extraDriverClasspath + hiveHadoopTestClasspath);
                }

                String extraExecutorClasspath = Strings.nullToEmpty((String) allProps.get(EXECUTOR_EXTRA_CLASSPATH));
                if (extraExecutorClasspath.isEmpty()) {
                    allProps.put(EXECUTOR_EXTRA_CLASSPATH, hiveHadoopTestClasspath);
                } else {
                    extraExecutorClasspath = extraExecutorClasspath.endsWith(File.pathSeparator) ? extraExecutorClasspath : extraExecutorClasspath + File.pathSeparator;
                    allProps.put(EXECUTOR_EXTRA_CLASSPATH, extraExecutorClasspath + hiveHadoopTestClasspath);
                }
            }
        }

        Writer writer = new OutputStreamWriter(new FileOutputStream(properties), Charsets.UTF_8);
        try {
            allProps.store(writer, "Spark Context configuration");
        } finally {
            writer.close();
        }

        // Define how to pass options to the child process. If launching in client (or local)
        // mode, the driver options need to be passed directly on the command line. Otherwise,
        // SparkSubmit will take care of that for us.
        String master = conf.get("spark.master");
        Preconditions.checkArgument(master != null, "spark.master is not defined.");

        final List<String> argv = Lists.newArrayList();

        if (hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION).equalsIgnoreCase("kerberos")) {
            argv.add("kinit");
            String principal = SecurityUtil.getServerPrincipal(hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL),
                    "0.0.0.0");
            String keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB);
            argv.add(principal);
            argv.add("-k");
            argv.add("-t");
            argv.add(keyTabFile + ";");
        }
        if (master.equals("yarn-cluster")) {
            String executorCores = conf.get("spark.executor.cores");
            if (executorCores != null) {
                argv.add("--executor-cores");
                argv.add(executorCores);
            }

            String executorMemory = conf.get("spark.executor.memory");
            if (executorMemory != null) {
                argv.add("--executor-memory");
                argv.add(executorMemory);
            }

            String numOfExecutors = conf.get("spark.executor.instances");
            if (numOfExecutors != null) {
                argv.add("--num-executors");
                argv.add(numOfExecutors);
            }
        }

        if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS)) {
            try {
                String currentUser = Utils.getUGI().getShortUserName();
                // do not do impersonation in CLI mode
                if (!currentUser.equals(System.getProperty("user.name"))) {
                    LOG.info("Attempting impersonation of " + currentUser);
                    argv.add("--proxy-user");
                    argv.add(currentUser);
                }
            } catch (Exception e) {
                String msg = "Cannot obtain username: " + e;
                throw new IllegalStateException(msg, e);
            }
        }

        argv.add("--properties-file");
        argv.add(properties.getAbsolutePath());
        argv.add("--class");
        argv.add(RemoteDriver.class.getName());

        //FIXME
        String jar = "spark-internal";
        if (SparkContext.jarOfClass(this.getClass()).isDefined()) {
            jar = SparkContext.jarOfClass(this.getClass()).get();
        }
        argv.add(hiveConf.get("hcat.spark.run.jar", jar));
            /*
            argv.add(jar);
            */

        argv.add("--remote-host");
        argv.add(serverAddress);
        argv.add("--remote-port");
        argv.add(serverPort);

        //FIXME
        argv.add("--client-id");
        argv.add(clientId);
        argv.add("--secret");
        argv.add(secret);

        //hive.spark.* keys are passed down to the RemoteDriver via --conf,
        //as --properties-file contains the spark.* keys that are meant for SparkConf object.
        for (String hiveSparkConfKey : RpcConfiguration.HIVE_SPARK_RSC_CONFIGS) {
            String value = RpcConfiguration.getValue(hiveConf, hiveSparkConfKey);
            argv.add("--conf");
            argv.add(String.format("%s=%s", hiveSparkConfKey, value));
        }

        //FIXME for hopc
        if(hiveConf.get("hiido.scheduleid") != null) {
            argv.add("--conf");
            argv.add(String.format("%s=%s", "hiido.scheduleid", hiveConf.get("hiido.scheduleid")));
        }
        if(SessionState.get().getUserName() != null) {
            argv.add("--conf");
            argv.add(String.format("%s=%s", "hive.hiido.charge", SessionState.get().getUserName()));
        }
        if(SessionState.get().getCurrUser() != null) {
            argv.add("--conf");
            argv.add(String.format("%s=%s", "currUser", SessionState.get().getCurrUser()));
        }
        if(SessionState.get().getLogSysUser() != null) {
            argv.add("--conf");
            argv.add(String.format("%s=%s", "logSysUser", SessionState.get().getLogSysUser()));
        }
        argv.add("--conf");
        argv.add(String.format("%s=%s", "hcat.qid", hiveConf.get("hcat.qid", "")));

        String cmd = Joiner.on(" ").join(argv);
        //SparkSubmit.main(argv.toArray(new String[argv.size()]));

        HiidoClient client = null;
        try {
            client = HiidoSparkSubmit.submitToCluster(argv.toArray(new String[argv.size()]));
            client.run();
            return client;
        } finally {
            if(properties != null && properties.exists())
                properties.delete();
            if(client != null && client.confArchive() != null && client.confArchive().exists())
                client.confArchive().delete();
        }
    }

    private void redirect(String name, Redirector redirector) {
        Thread thread = new Thread(redirector);
        thread.setName(name);
        thread.setDaemon(true);
        thread.start();
    }

    private class ClientProtocol extends BaseProtocol {

        <T extends Serializable> JobHandleImpl<T> submit(Job<T> job) {
            final String jobId = UUID.randomUUID().toString();
            final Promise<T> promise = driverRpc.createPromise();
            final JobHandleImpl<T> handle = new JobHandleImpl<T>(SparkClientImpl.this, promise, jobId);
            jobs.put(jobId, handle);

            final io.netty.util.concurrent.Future<Void> rpc = driverRpc.call(new JobRequest(jobId, job));
            LOG.debug("Send JobRequest[{}].", jobId);

            // Link the RPC and the promise so that events from one are propagated to the other as
            // needed.
            rpc.addListener(new GenericFutureListener<io.netty.util.concurrent.Future<Void>>() {
                @Override
                public void operationComplete(io.netty.util.concurrent.Future<Void> f) {
                    if (f.isSuccess()) {
                        handle.changeState(JobHandle.State.QUEUED);
                    } else if (!promise.isDone()) {
                        promise.setFailure(f.cause());
                    }
                }
            });
            promise.addListener(new GenericFutureListener<Promise<T>>() {
                @Override
                public void operationComplete(Promise<T> p) {
                    if (jobId != null) {
                        jobs.remove(jobId);
                    }
                    if (p.isCancelled() && !rpc.isDone()) {
                        rpc.cancel(true);
                    }
                }
            });
            return handle;
        }

        <T extends Serializable> Future<T> run(Job<T> job) {
            @SuppressWarnings("unchecked")
            final io.netty.util.concurrent.Future<T> rpc = (io.netty.util.concurrent.Future<T>)
                    driverRpc.call(new SyncJobRequest(job), Serializable.class);
            return rpc;
        }

        void cancel(String jobId) {
            driverRpc.call(new CancelJob(jobId));
        }

        Future<?> endSession() {
            return driverRpc.call(new EndSession());
        }

        private void handle(ChannelHandlerContext ctx, Error msg) {
            LOG.warn("Error reported from remote driver.", msg.cause);
        }

        private void handle(ChannelHandlerContext ctx, JobMetrics msg) {
            JobHandleImpl<?> handle = jobs.get(msg.jobId);
            if (handle != null) {
                handle.getMetrics().addMetrics(msg.sparkJobId, msg.stageId, msg.taskId, msg.metrics);
            } else {
                LOG.warn("Received metrics for unknown job {}", msg.jobId);
            }
        }

        private void handle(ChannelHandlerContext ctx, JobResult msg) {
            JobHandleImpl<?> handle = jobs.remove(msg.id);
            if (handle != null) {
                LOG.info("Received result for {}", msg.id);
                handle.setSparkCounters(msg.sparkCounters);
                Throwable error = msg.error != null ? new SparkException(msg.error) : null;
                if (error == null) {
                    handle.setSuccess(msg.result);
                } else {
                    handle.setFailure(error);
                }
            } else {
                LOG.warn("Received result for unknown job {}", msg.id);
            }
        }

        private void handle(ChannelHandlerContext ctx, JobStarted msg) {
            JobHandleImpl<?> handle = jobs.get(msg.id);
            if (handle != null) {
                handle.changeState(JobHandle.State.STARTED);
            } else {
                LOG.warn("Received event for unknown job {}", msg.id);
            }
        }

        private void handle(ChannelHandlerContext ctx, JobSubmitted msg) {
            JobHandleImpl<?> handle = jobs.get(msg.clientJobId);
            if (handle != null) {
                LOG.info("Received spark job ID: {} for {}", msg.sparkJobId, msg.clientJobId);
                handle.addSparkJobId(msg.sparkJobId);
            } else {
                LOG.warn("Received spark job ID: {} for unknown job {}", msg.sparkJobId, msg.clientJobId);
            }
        }

    }

    private class Redirector implements Runnable {

        private final BufferedReader in;
        private List<String> errLogs;
        private int numErrLogLines = 0;

        Redirector(InputStream in) {
            this.in = new BufferedReader(new InputStreamReader(in));
        }

        Redirector(InputStream in, List<String> errLogs) {
            this.in = new BufferedReader(new InputStreamReader(in));
            this.errLogs = errLogs;
        }

        @Override
        public void run() {
            try {
                String line = null;
                while ((line = in.readLine()) != null) {
                    LOG.info(line);
                    if (errLogs != null) {
                        if (numErrLogLines++ < MAX_ERR_LOG_LINES_FOR_RPC) {
                            errLogs.add(line);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("Error in redirector thread.", e);
            }
        }

    }

    private static class AddJarJob implements Job<Serializable> {
        private static final long serialVersionUID = 1L;

        private final String path;

        AddJarJob() {
            this(null);
        }

        AddJarJob(String path) {
            this.path = path;
        }

        @Override
        public Serializable call(JobContext jc) throws Exception {
            jc.sc().addJar(path);
            // Following remote job may refer to classes in this jar, and the remote job would be executed
            // in a different thread, so we add this jar path to JobContext for further usage.
            jc.getAddedJars().put(path, System.currentTimeMillis());
            return null;
        }

    }

    private static class AddFileJob implements Job<Serializable> {
        private static final long serialVersionUID = 1L;

        private final String path;

        AddFileJob() {
            this(null);
        }

        AddFileJob(String path) {
            this.path = path;
        }

        @Override
        public Serializable call(JobContext jc) throws Exception {
            jc.sc().addFile(path);
            return null;
        }

    }

    private static class GetExecutorCountJob implements Job<Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer call(JobContext jc) throws Exception {
            // minus 1 here otherwise driver is also counted as an executor
            int count = jc.sc().sc().getExecutorMemoryStatus().size() - 1;
            return Integer.valueOf(count);
        }

    }

    private static class GetDefaultParallelismJob implements Job<Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer call(JobContext jc) throws Exception {
            return jc.sc().sc().defaultParallelism();
        }
    }

}
