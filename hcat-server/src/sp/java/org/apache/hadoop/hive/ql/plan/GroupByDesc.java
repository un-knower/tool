/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hive.common.util.AnnotationUtils;
import org.apache.hadoop.hive.ql.plan.Explain.Level;


/**
 * GroupByDesc.
 *
 */
@Explain(displayName = "Group By Operator", explainLevels = {Level.USER, Level.DEFAULT, Level.EXTENDED})
public class GroupByDesc extends AbstractOperatorDesc {
    /**
     * Group-by Mode: COMPLETE: complete 1-phase aggregation: iterate, terminate
     * PARTIAL1: partial aggregation - first phase: iterate, terminatePartial
     * PARTIAL2: partial aggregation - second phase: merge, terminatePartial
     * PARTIALS: For non-distinct the same as PARTIAL2, for distinct the same as
     * PARTIAL1
     * FINAL: partial aggregation - final phase: merge, terminate
     * HASH: For non-distinct the same as PARTIAL1 but use hash-table-based aggregation
     * MERGEPARTIAL: FINAL for non-distinct aggregations, COMPLETE for distinct
     * aggregations.
     */
    private static long serialVersionUID = 1L;

    /**
     * Mode.
     *
     */
    public static enum Mode {
        COMPLETE, PARTIAL1, PARTIAL2, PARTIALS, FINAL, HASH, MERGEPARTIAL
    }

    ;

    private Mode mode;

    // no hash aggregations for group by
    private boolean bucketGroup;

    private ArrayList<ExprNodeDesc> keys;
    private List<Integer> listGroupingSets;
    private boolean groupingSetsPresent;
    private int groupingSetPosition = -1;
    private ArrayList<org.apache.hadoop.hive.ql.plan.AggregationDesc> aggregators;
    private ArrayList<java.lang.String> outputColumnNames;
    private float groupByMemoryUsage;
    private float memoryThreshold;
    transient private boolean isDistinct;
    private boolean dontResetAggrsDistinct;

    // Extra parameters only for vectorization.
    private VectorGroupByDesc vectorDesc;

    public GroupByDesc() {
        vectorDesc = new VectorGroupByDesc();
    }

    public GroupByDesc(
            final Mode mode,
            final ArrayList<java.lang.String> outputColumnNames,
            final ArrayList<ExprNodeDesc> keys,
            final ArrayList<org.apache.hadoop.hive.ql.plan.AggregationDesc> aggregators,
            final float groupByMemoryUsage,
            final float memoryThreshold,
            final List<Integer> listGroupingSets,
            final boolean groupingSetsPresent,
            final int groupingSetsPosition,
            final boolean isDistinct) {
        this(mode, outputColumnNames, keys, aggregators,
                false, groupByMemoryUsage, memoryThreshold, listGroupingSets,
                groupingSetsPresent, groupingSetsPosition, isDistinct);
    }

    public GroupByDesc(
            final Mode mode,
            final ArrayList<java.lang.String> outputColumnNames,
            final ArrayList<ExprNodeDesc> keys,
            final ArrayList<org.apache.hadoop.hive.ql.plan.AggregationDesc> aggregators,
            final boolean bucketGroup,
            final float groupByMemoryUsage,
            final float memoryThreshold,
            final List<Integer> listGroupingSets,
            final boolean groupingSetsPresent,
            final int groupingSetsPosition,
            final boolean isDistinct) {
        vectorDesc = new VectorGroupByDesc();
        this.mode = mode;
        this.outputColumnNames = outputColumnNames;
        this.keys = keys;
        this.aggregators = aggregators;
        this.bucketGroup = bucketGroup;
        this.groupByMemoryUsage = groupByMemoryUsage;
        this.memoryThreshold = memoryThreshold;
        this.listGroupingSets = listGroupingSets;
        this.groupingSetsPresent = groupingSetsPresent;
        this.groupingSetPosition = groupingSetsPosition;
        this.isDistinct = isDistinct;
    }

    public void setVectorDesc(VectorGroupByDesc vectorDesc) {
        this.vectorDesc = vectorDesc;
    }

    public VectorGroupByDesc getVectorDesc() {
        return vectorDesc;
    }

    public Mode getMode() {
        return mode;
    }

    @Explain(displayName = "mode")
    public String getModeString() {
        switch (mode) {
            case COMPLETE:
                return "complete";
            case PARTIAL1:
                return "partial1";
            case PARTIAL2:
                return "partial2";
            case PARTIALS:
                return "partials";
            case HASH:
                return "hash";
            case FINAL:
                return "final";
            case MERGEPARTIAL:
                return "mergepartial";
        }

        return "unknown";
    }

    public void setMode(final Mode mode) {
        this.mode = mode;
    }

    @Explain(displayName = "keys")
    public String getKeyString() {
        return PlanUtils.getExprListString(keys);
    }

    @Explain(displayName = "keys", explainLevels = {Level.USER})
    public String getUserLevelExplainKeyString() {
        return PlanUtils.getExprListString(keys, true);
    }

    public ArrayList<ExprNodeDesc> getKeys() {
        return keys;
    }

    public void setKeys(final ArrayList<ExprNodeDesc> keys) {
        this.keys = keys;
    }

    @Explain(displayName = "outputColumnNames")
    public ArrayList<java.lang.String> getOutputColumnNames() {
        return outputColumnNames;
    }

    @Explain(displayName = "Output", explainLevels = {Level.USER})
    public ArrayList<java.lang.String> getUserLevelExplainOutputColumnNames() {
        return outputColumnNames;
    }

    @Explain(displayName = "pruneGroupingSetId", displayOnlyOnTrue = true)
    public boolean pruneGroupingSetId() {
        return groupingSetPosition >= 0 &&
                outputColumnNames.size() != keys.size() + aggregators.size();
    }

    public void setOutputColumnNames(
            ArrayList<java.lang.String> outputColumnNames) {
        this.outputColumnNames = outputColumnNames;
    }

    public float getGroupByMemoryUsage() {
        return groupByMemoryUsage;
    }

    public void setGroupByMemoryUsage(float groupByMemoryUsage) {
        this.groupByMemoryUsage = groupByMemoryUsage;
    }

    public float getMemoryThreshold() {
        return memoryThreshold;
    }

    public void setMemoryThreshold(float memoryThreshold) {
        this.memoryThreshold = memoryThreshold;
    }

    @Explain(displayName = "aggregations", explainLevels = {Level.USER, Level.DEFAULT, Level.EXTENDED})
    public List<String> getAggregatorStrings() {
        List<String> res = new ArrayList<String>();
        for (AggregationDesc agg : aggregators) {
            res.add(agg.getExprString());
        }
        return res;
    }

    public ArrayList<org.apache.hadoop.hive.ql.plan.AggregationDesc> getAggregators() {
        return aggregators;
    }

    public void setAggregators(
            final ArrayList<org.apache.hadoop.hive.ql.plan.AggregationDesc> aggregators) {
        this.aggregators = aggregators;
    }

    public boolean isAggregate() {
        if (this.aggregators != null && !this.aggregators.isEmpty()) {
            return true;
        }
        return false;
    }

    @Explain(displayName = "bucketGroup", displayOnlyOnTrue = true)
    public boolean getBucketGroup() {
        return bucketGroup;
    }

    public void setBucketGroup(boolean bucketGroup) {
        this.bucketGroup = bucketGroup;
    }

    /**
     * Checks if this grouping is like distinct, which means that all non-distinct grouping
     * columns behave like they were distinct - for example min and max operators.
     */
    public boolean isDistinctLike() {
        ArrayList<AggregationDesc> aggregators = getAggregators();
        for (AggregationDesc ad : aggregators) {
            if (!ad.getDistinct()) {
                GenericUDAFEvaluator udafEval = ad.getGenericUDAFEvaluator();
                UDFType annot = AnnotationUtils.getAnnotation(udafEval.getClass(), UDFType.class);
                if (annot == null || !annot.distinctLike()) {
                    return false;
                }
            }
        }
        return true;
    }

    // Consider a query like:
    // select a, b, count(distinct c) from T group by a,b with rollup;
    // Assume that hive.map.aggr is set to true and hive.groupby.skewindata is false,
    // in which case the group by would execute as a single map-reduce job.
    // For the group-by, the group by keys should be: a,b,groupingSet(for rollup), c
    // So, the starting position of grouping set need to be known
    public List<Integer> getListGroupingSets() {
        return listGroupingSets;
    }

    public void setListGroupingSets(final List<Integer> listGroupingSets) {
        this.listGroupingSets = listGroupingSets;
    }

    public boolean isGroupingSetsPresent() {
        return groupingSetsPresent;
    }

    public void setGroupingSetsPresent(boolean groupingSetsPresent) {
        this.groupingSetsPresent = groupingSetsPresent;
    }

    public int getGroupingSetPosition() {
        return groupingSetPosition;
    }

    public void setGroupingSetPosition(int groupingSetPosition) {
        this.groupingSetPosition = groupingSetPosition;
    }

    public boolean isDontResetAggrsDistinct() {
        return dontResetAggrsDistinct;
    }

    public void setDontResetAggrsDistinct(boolean dontResetAggrsDistinct) {
        this.dontResetAggrsDistinct = dontResetAggrsDistinct;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    //FIXME HIVE-16654
    @Override
    public Object clone() {
        ArrayList<java.lang.String> outputColumnNames = new ArrayList<>();
        outputColumnNames.addAll(this.outputColumnNames);
        ArrayList<ExprNodeDesc> keys = new ArrayList<>();
        keys.addAll(this.keys);
        ArrayList<org.apache.hadoop.hive.ql.plan.AggregationDesc> aggregators = new ArrayList<>();
        aggregators.addAll(this.aggregators);
        List<Integer> listGroupingSets = new ArrayList<>();
        listGroupingSets.addAll(this.listGroupingSets);
        return new GroupByDesc(this.mode, outputColumnNames, keys, aggregators,
                this.groupByMemoryUsage, this.memoryThreshold, listGroupingSets, this.groupingSetsPresent,
                this.groupingSetPosition, this.isDistinct);
    }

}
