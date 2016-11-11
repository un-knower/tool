package org.apache.hive.spark.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by zrc on 16-10-24.
 */
public class HcatSparkClient implements SparkClient{
    private static final Logger LOG = Logger.getLogger(HcatSparkClient.class);

    private final Map<String, String> conf;
    private final HiveConf hiveConf;
    private final Map<String, JobHandleImpl<?>> jobs;

    private volatile JobContextImpl jc;

    public HcatSparkClient(Map<String, String> conf, HiveConf hiveConf) {
        this.conf = conf;
        this.hiveConf = hiveConf;
        this.jobs = Maps.newConcurrentMap();
    }

    private void startDriver(final String clientId, final String secret) {
        List<String> args = Lists.newArrayList();
        for (Map.Entry<String, String> e : conf.entrySet()) {
            args.add("--conf");
            args.add(String.format("%s=%s", e.getKey(), conf.get(e.getKey())));
        }
    }

    @Override
    public <T extends Serializable> JobHandle<T> submit(Job<T> job) {
        return null;
    }

    @Override
    public <T extends Serializable> Future<T> run(Job<T> job) {

        return null;
    }

    @Override
    public void stop() {

    }

    @Override
    public Future<?> addJar(URI uri) {
        jc.sc().addJar(uri.toString());
        // Following remote job may refer to classes in this jar, and the remote job would be executed
        // in a different thread, so we add this jar path to JobContext for further usage.
        jc.getAddedJars().put(uri.toString(), System.currentTimeMillis());
        return null;
    }

    @Override
    public Future<?> addFile(URI uri) {
        jc.sc().addFile(uri.toString());
        return null;
    }

    @Override
    public Future<Integer> getExecutorCount() {
        int count = jc.sc().sc().getExecutorMemoryStatus().size() - 1;
        return new SimpleFuture<Integer>(count);
    }

    @Override
    public Future<Integer> getDefaultParallelism() {
        return null;
    }

    @Override
    public boolean isActive() {
        return false;
    }

    static class SimpleFuture<T> implements Future {

        T value;

        public SimpleFuture(T t) {
            value = t;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            return value;
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return value;
        }
    }
}
