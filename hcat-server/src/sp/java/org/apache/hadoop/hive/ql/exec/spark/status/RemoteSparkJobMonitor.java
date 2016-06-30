/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.spark.status;

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.spark.status.impl.RemoteSparkJobStatus;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.spark.client.JobHandle;
import org.apache.spark.JobExecutionStatus;

/**
 * RemoteSparkJobMonitor monitor a RSC remote job status in a loop until job finished/failed/killed.
 * It print current job status to console and sleep current thread between monitor interval.
 *
 * created by zrc on 2016-06-30
 */
public class RemoteSparkJobMonitor extends SparkJobMonitor {

    private String jobId;
    private RemoteSparkJobStatus sparkJobStatus;

    public RemoteSparkJobMonitor(HiveConf hiveConf, String jobId, RemoteSparkJobStatus sparkJobStatus) {
        super(hiveConf);
        this.jobId = jobId;
        this.sparkJobStatus = sparkJobStatus;
    }

    @Override
    public int startMonitor() {
        boolean running = false;
        boolean done = false;
        int rc = 0;
        Map<String, SparkStageProgress> lastProgressMap = null;

        perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_RUN_JOB);
        perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_SUBMIT_TO_RUNNING);

        long startTime = System.currentTimeMillis();

        SessionState.get().putIfAbsent(jobId);
        SessionState.get().putRunningTask(jobId);
        while (true) {
            try {
                JobHandle.State state = sparkJobStatus.getRemoteJobState();
                if (LOG.isDebugEnabled()) {
                    console.printInfo("state = " + state);
                }

                switch (state) {
                    case SENT:
                    case QUEUED:
                        long timeCount = (System.currentTimeMillis() - startTime) / 1000;
                        if ((timeCount > monitorTimeoutInteval)) {
                            //LOG.info("Job hasn't been submitted after " + timeCount + "s. Aborting it.");
                            //console.printError("Status: " + state);
                            console.printError("Status: " + state, "Job hasn't been submitted after " + timeCount + "s. Aborting it.");
                            running = false;
                            done = true;
                            rc = 2;
                        }
                        break;
                    case STARTED:
                        JobExecutionStatus sparkJobState = sparkJobStatus.getState();
                        if (sparkJobState == JobExecutionStatus.RUNNING) {
                            Map<String, SparkStageProgress> progressMap = sparkJobStatus.getSparkStageProgress();
                            if (!running) {
                                perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_SUBMIT_TO_RUNNING);
                                // print job stages.
                                //console.printInfo("\nQuery Hive on Spark job["
                                  //      + sparkJobStatus.getJobId() + "] stages:");
                                for (int stageId : sparkJobStatus.getStageIds()) {
                                    console.printInfo(Integer.toString(stageId));
                                }

                                console.printInfo("\nStatus: Running (Hive on Spark job["
                                        + sparkJobStatus.getJobId() + "])");
                                running = true;

                                console.printInfo("Job Progress Format\nCurrentTime StageId_StageAttemptId: "
                                        + "SucceededTasksCount(+RunningTasksCount-FailedTasksCount)/TotalTasksCount [StageCost]");
                            }

                            //printStatus(progressMap, lastProgressMap);
                            //lastProgressMap = progressMap;
                            updateJobStatus(progressMap);
                        }
                        break;
                    case SUCCEEDED:
                        Map<String, SparkStageProgress> progressMap = sparkJobStatus.getSparkStageProgress();
                        printStatus(progressMap, lastProgressMap);
                        lastProgressMap = progressMap;
                        double duration = (System.currentTimeMillis() - startTime) / 1000.0;
                        console.printInfo("Status: Finished successfully in "
                                + String.format("%.2f seconds", duration));
                        running = false;
                        done = true;
                        break;
                    case FAILED:
                        console.printError("Status: Failed");
                        running = false;
                        done = true;
                        rc = 3;
                        break;
                }

                if (!done) {
                    Thread.sleep(checkInterval);
                }
            } catch (Exception e) {
                String msg = " with exception '" + Utilities.getNameMessage(e) + "'";
                msg = "Failed to monitor Job[ " + sparkJobStatus.getJobId() + "]" + msg;

                // Has to use full name to make sure it does not conflict with
                // org.apache.commons.lang.StringUtils
                LOG.error(msg, e);
                console.printError(msg, "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
                rc = 1;
                done = true;
            } finally {
                if (done) {
                    SessionState.get().updateFinishTask(jobId);
                    break;
                }
            }
        }

        perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_RUN_JOB);
        return rc;
    }

    private void updateJobStatus(Map<String, SparkStageProgress> progressMap) {
        SortedSet<String> keys = new TreeSet<String>(progressMap.keySet());
        int total = 0, complete = 0;
        for(String s : keys) {
            SparkStageProgress progress = progressMap.get(s);
            complete += progress.getSucceededTaskCount();
            total += progress.getTotalTaskCount();
        }
        if(total < 0)
            return;
        else {
            SessionState.get().updateRunningTask(this.jobId, ((float)complete)/total);
        }
    }
}
