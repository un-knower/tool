/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Planner rule that expands distinct aggregates
 * (such as {@code COUNT(DISTINCT x)}) from a
 * {@link org.apache.calcite.rel.logical.LogicalAggregate}.
 *
 * <p>How this is done depends upon the arguments to the function. If all
 * functions have the same argument
 * (e.g. {@code COUNT(DISTINCT x), SUM(DISTINCT x)} both have the argument
 * {@code x}) then one extra {@link org.apache.calcite.rel.core.Aggregate} is
 * sufficient.
 *
 * <p>If there are multiple arguments
 * (e.g. {@code COUNT(DISTINCT x), COUNT(DISTINCT y)})
 * the rule creates separate {@code Aggregate}s and combines using a
 * {@link org.apache.calcite.rel.core.Join}.
 */
public final class HiveExpandDistinctAggregatesRule extends RelOptRule {

    //~ Static fields/initializers ---------------------------------------------

    /** The default instance of the rule; operates only on logical expressions. */
    public static final HiveExpandDistinctAggregatesRule INSTANCE =
            new HiveExpandDistinctAggregatesRule(HiveAggregate.class, false,
                    HiveRelFactories.HIVE_BUILDER);

    /** Instance of the rule that operates only on logical expressions and
     * generates a join. */
    /* FIXME
    public static final HiveExpandDistinctAggregatesRule JOIN =
            new HiveExpandDistinctAggregatesRule(LogicalAggregate.class, false,
                    HiveRelFactories.HIVE_BUILDER);
    */
    public static final BigDecimal TWO = BigDecimal.valueOf(2L);

    public final boolean useGroupingSets;

    //~ Constructors -----------------------------------------------------------

    public HiveExpandDistinctAggregatesRule(
            Class<? extends Aggregate> clazz,
            boolean useGroupingSets,
            RelBuilderFactory relBuilderFactory) {
        super(operand(clazz, any()), relBuilderFactory, null);
        this.useGroupingSets = useGroupingSets;
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call) {
        final Aggregate aggregate = call.rel(0);
        if (!aggregate.containsDistinctCall()) {
            return;
        }

        // Find all of the agg expressions. We use a LinkedHashSet to ensure
        // determinism.
        int nonDistinctCount = 0;
        final Set<Pair<List<Integer>, Integer>> argLists = new LinkedHashSet<>();
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            if (!aggCall.isDistinct()) {
                ++nonDistinctCount;
                continue;
            }
            argLists.add(Pair.of(aggCall.getArgList(), aggCall.filterArg));
        }
        Preconditions.checkState(argLists.size() > 0, "containsDistinctCall lied");

        // If all of the agg expressions are distinct and have the same
        // arguments then we can use a more efficient form.
        if (nonDistinctCount == 0 && argLists.size() == 1) {
            final Pair<List<Integer>, Integer> pair =
                    Iterables.getOnlyElement(argLists);
            final RelBuilder relBuilder = call.builder();
            convertMonopole(relBuilder, aggregate, pair.left, pair.right);
            call.transformTo(relBuilder.build());
            return;
        }

        //FIXME see AggregateExpandDistinctAggregatesRule.onMatch()
        if (useGroupingSets) {
            rewriteUsingGroupingSets(call, aggregate, argLists);
            return;
        }

        // Create a list of the expressions which will yield the final result.
        // Initially, the expressions point to the input field.
        final List<RelDataTypeField> aggFields =
                aggregate.getRowType().getFieldList();
        final List<RexInputRef> refs = new ArrayList<>();
        final List<String> fieldNames = aggregate.getRowType().getFieldNames();
        final ImmutableBitSet groupSet = aggregate.getGroupSet();
        final int groupAndIndicatorCount =
                aggregate.getGroupCount() + aggregate.getIndicatorCount();
        for (int i : Util.range(groupAndIndicatorCount)) {
            refs.add(RexInputRef.of(i, aggFields));
        }

        // Aggregate the original relation, including any non-distinct aggregates.
        final List<AggregateCall> newAggCallList = new ArrayList<>();
        int i = -1;
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            ++i;
            if (aggCall.isDistinct()) {
                refs.add(null);
                continue;
            }
            refs.add(
                    new RexInputRef(
                            groupAndIndicatorCount + newAggCallList.size(),
                            aggFields.get(groupAndIndicatorCount + i).getType()));
            newAggCallList.add(aggCall);
        }

        // In the case where there are no non-distinct aggregates (regardless of
        // whether there are group bys), there's no need to generate the
        // extra aggregate and join.
        final RelBuilder relBuilder = call.builder();
        relBuilder.push(aggregate.getInput());
        int n = 0;
        if (!newAggCallList.isEmpty()) {
            final RelBuilder.GroupKey groupKey =
                    relBuilder.groupKey(groupSet, aggregate.indicator, aggregate.getGroupSets());
            relBuilder.aggregate(groupKey, newAggCallList);
            ++n;
        }

        // For each set of operands, find and rewrite all calls which have that
        // set of operands.
        for (Pair<List<Integer>, Integer> argList : argLists) {
            doRewrite(relBuilder, aggregate, n++, argList.left, argList.right, refs);
        }

        relBuilder.project(refs, fieldNames);
        call.transformTo(relBuilder.build());
    }

    private void rewriteUsingGroupingSets(RelOptRuleCall call,
                                          Aggregate aggregate, Set<Pair<List<Integer>, Integer>> argLists) {
        final Set<ImmutableBitSet> groupSetTreeSet =
                new TreeSet<>(ImmutableBitSet.ORDERING);
        groupSetTreeSet.add(aggregate.getGroupSet());
        for (Pair<List<Integer>, Integer> argList : argLists) {
            groupSetTreeSet.add(
                    ImmutableBitSet.of(argList.left)
                            .setIf(argList.right, argList.right >= 0)
                            .union(aggregate.getGroupSet()));
        }

        final ImmutableList<ImmutableBitSet> groupSets =
                ImmutableList.copyOf(groupSetTreeSet);
        final ImmutableBitSet fullGroupSet = ImmutableBitSet.union(groupSets);

        final List<AggregateCall> distinctAggCalls = new ArrayList<>();
        for (Pair<AggregateCall, String> aggCall : aggregate.getNamedAggCalls()) {
            if (!aggCall.left.isDistinct()) {
                distinctAggCalls.add(aggCall.left.rename(aggCall.right));
            }
        }

        final RelBuilder relBuilder = call.builder();
        relBuilder.push(aggregate.getInput());
        relBuilder.aggregate(relBuilder.groupKey(fullGroupSet, groupSets.size() > 1, groupSets),
                distinctAggCalls);
        final RelNode distinct = relBuilder.peek();
        final int groupCount = fullGroupSet.cardinality();
        final int indicatorCount = groupSets.size() > 1 ? groupCount : 0;

        final RelOptCluster cluster = aggregate.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
        final RelDataType booleanType =
                typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType(SqlTypeName.BOOLEAN), false);
        final List<Pair<RexNode, String>> predicates = new ArrayList<>();
        final Map<ImmutableBitSet, Integer> filters = new HashMap<>();
        /** Function to register an filter for a group set. */
        class Registrar {
            RexNode group = null;
            int register(ImmutableBitSet groupSet) {
                if (group == null) {
                    group = makeGroup(groupCount - 1);
                }
                final RexNode node =
                        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, group,
                                rexBuilder.makeExactLiteral(
                                        toNumber(remap(fullGroupSet, groupSet))));
                predicates.add(Pair.of(node, toString(groupSet)));
                return groupCount + indicatorCount + distinctAggCalls.size()
                        + predicates.size() - 1;
            }

            private RexNode makeGroup(int i) {
                final RexInputRef ref =
                        rexBuilder.makeInputRef(booleanType, groupCount + i);
                final RexNode kase =
                        rexBuilder.makeCall(SqlStdOperatorTable.CASE, ref,
                                rexBuilder.makeExactLiteral(BigDecimal.ZERO),
                                rexBuilder.makeExactLiteral(TWO.pow(i)));
                if (i == 0) {
                    return kase;
                } else {
                    return rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
                            makeGroup(i - 1), kase);
                }
            }

            private BigDecimal toNumber(ImmutableBitSet bitSet) {
                BigDecimal n = BigDecimal.ZERO;
                for (int key : bitSet) {
                    n = n.add(TWO.pow(key));
                }
                return n;
            }

            private String toString(ImmutableBitSet bitSet) {
                final StringBuilder buf = new StringBuilder("$i");
                for (int key : bitSet) {
                    buf.append(key).append('_');
                }
                return buf.substring(0, buf.length() - 1);
            }
        }
        final Registrar registrar = new Registrar();
        for (ImmutableBitSet groupSet : groupSets) {
            filters.put(groupSet, registrar.register(groupSet));
        }

        if (!predicates.isEmpty()) {
            List<Pair<RexNode, String>> nodes = new ArrayList<>();
            for (RelDataTypeField f : relBuilder.peek().getRowType().getFieldList()) {
                final RexNode node = rexBuilder.makeInputRef(f.getType(), f.getIndex());
                nodes.add(Pair.of(node, f.getName()));
            }
            nodes.addAll(predicates);
            relBuilder.project(Pair.left(nodes), Pair.right(nodes));
        }

        int x = groupCount + indicatorCount;
        final List<AggregateCall> newCalls = new ArrayList<>();
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            final int newFilterArg;
            final List<Integer> newArgList;
            final SqlAggFunction aggregation;
            if (!aggCall.isDistinct()) {
                aggregation = SqlStdOperatorTable.MIN;
                newArgList = ImmutableIntList.of(x++);
                newFilterArg = filters.get(aggregate.getGroupSet());
            } else {
                aggregation = aggCall.getAggregation();
                newArgList = remap(fullGroupSet, aggCall.getArgList());
                newFilterArg =
                        filters.get(
                                ImmutableBitSet.of(aggCall.getArgList())
                                        .setIf(aggCall.filterArg, aggCall.filterArg >= 0)
                                        .union(aggregate.getGroupSet()));
            }
            final AggregateCall newCall =
                    AggregateCall.create(aggregation, false, newArgList, newFilterArg,
                            aggregate.getGroupCount(), distinct, null, aggCall.name);
            newCalls.add(newCall);
        }

        relBuilder.aggregate(
                relBuilder.groupKey(
                        remap(fullGroupSet, aggregate.getGroupSet()),
                        aggregate.indicator,
                        remap(fullGroupSet, aggregate.getGroupSets())),
                newCalls);
        relBuilder.convert(aggregate.getRowType(), true);
        call.transformTo(relBuilder.build());
    }

    private static ImmutableBitSet remap(ImmutableBitSet groupSet,
                                         ImmutableBitSet bitSet) {
        final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        for (Integer bit : bitSet) {
            builder.set(remap(groupSet, bit));
        }
        return builder.build();
    }

    private static ImmutableList<ImmutableBitSet> remap(ImmutableBitSet groupSet,
                                                        Iterable<ImmutableBitSet> bitSets) {
        final ImmutableList.Builder<ImmutableBitSet> builder =
                ImmutableList.builder();
        for (ImmutableBitSet bitSet : bitSets) {
            builder.add(remap(groupSet, bitSet));
        }
        return builder.build();
    }

    private static List<Integer> remap(ImmutableBitSet groupSet,
                                       List<Integer> argList) {
        ImmutableIntList list = ImmutableIntList.of();
        for (int arg : argList) {
            list = list.add(remap(groupSet, arg));
        }
        return list;
    }

    private static int remap(ImmutableBitSet groupSet, int arg) {
        return arg < 0 ? -1 : groupSet.indexOf(arg);
    }

    /**
     * Converts an aggregate relational expression that contains just one
     * distinct aggregate function (or perhaps several over the same arguments)
     * and no non-distinct aggregate functions.
     */
    private RelBuilder convertMonopole(RelBuilder relBuilder, Aggregate aggregate,
                                       List<Integer> argList, int filterArg) {
        // For example,
        //    SELECT deptno, COUNT(DISTINCT sal), SUM(DISTINCT sal)
        //    FROM emp
        //    GROUP BY deptno
        //
        // becomes
        //
        //    SELECT deptno, COUNT(distinct_sal), SUM(distinct_sal)
        //    FROM (
        //      SELECT DISTINCT deptno, sal AS distinct_sal
        //      FROM EMP GROUP BY deptno)
        //    GROUP BY deptno

        // Project the columns of the GROUP BY plus the arguments
        // to the agg function.
        final Map<Integer, Integer> sourceOf = new HashMap<>();
        createSelectDistinct(relBuilder, aggregate, argList, filterArg, sourceOf);

        // Create an aggregate on top, with the new aggregate list.
        final List<AggregateCall> newAggCalls =
                Lists.newArrayList(aggregate.getAggCallList());
        rewriteAggCalls(newAggCalls, argList, sourceOf);
        final int cardinality = aggregate.getGroupSet().cardinality();
        relBuilder.push(
                aggregate.copy(aggregate.getTraitSet(), relBuilder.build(),
                        aggregate.indicator, ImmutableBitSet.range(cardinality), null,
                        newAggCalls));
        return relBuilder;
    }

    /**
     * Converts all distinct aggregate calls to a given set of arguments.
     *
     * <p>This method is called several times, one for each set of arguments.
     * Each time it is called, it generates a JOIN to a new SELECT DISTINCT
     * relational expression, and modifies the set of top-level calls.
     *
     * @param aggregate Original aggregate
     * @param n         Ordinal of this in a join. {@code relBuilder} contains the
     *                  input relational expression (either the original
     *                  aggregate, the output from the previous call to this
     *                  method. {@code n} is 0 if we're converting the
     *                  first distinct aggregate in a query with no non-distinct
     *                  aggregates)
     * @param argList   Arguments to the distinct aggregate function
     * @param filterArg Argument that filters input to aggregate function, or -1
     * @param refs      Array of expressions which will be the projected by the
     *                  result of this rule. Those relating to this arg list will
     *                  be modified  @return Relational expression
     */
    private void doRewrite(RelBuilder relBuilder, Aggregate aggregate, int n,
                           List<Integer> argList, int filterArg, List<RexInputRef> refs) {
        final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
        final List<RelDataTypeField> leftFields;
        if (n == 0) {
            leftFields = null;
        } else {
            leftFields = relBuilder.peek().getRowType().getFieldList();
        }

        // LogicalAggregate(
        //     child,
        //     {COUNT(DISTINCT 1), SUM(DISTINCT 1), SUM(2)})
        //
        // becomes
        //
        // LogicalAggregate(
        //     LogicalJoin(
        //         child,
        //         LogicalAggregate(child, < all columns > {}),
        //         INNER,
        //         <f2 = f5>))
        //
        // E.g.
        //   SELECT deptno, SUM(DISTINCT sal), COUNT(DISTINCT gender), MAX(age)
        //   FROM Emps
        //   GROUP BY deptno
        //
        // becomes
        //
        //   SELECT e.deptno, adsal.sum_sal, adgender.count_gender, e.max_age
        //   FROM (
        //     SELECT deptno, MAX(age) as max_age
        //     FROM Emps GROUP BY deptno) AS e
        //   JOIN (
        //     SELECT deptno, COUNT(gender) AS count_gender FROM (
        //       SELECT DISTINCT deptno, gender FROM Emps) AS dgender
        //     GROUP BY deptno) AS adgender
        //     ON e.deptno = adgender.deptno
        //   JOIN (
        //     SELECT deptno, SUM(sal) AS sum_sal FROM (
        //       SELECT DISTINCT deptno, sal FROM Emps) AS dsal
        //     GROUP BY deptno) AS adsal
        //   ON e.deptno = adsal.deptno
        //   GROUP BY e.deptno
        //
        // Note that if a query contains no non-distinct aggregates, then the
        // very first join/group by is omitted.  In the example above, if
        // MAX(age) is removed, then the sub-select of "e" is not needed, and
        // instead the two other group by's are joined to one another.

        // Project the columns of the GROUP BY plus the arguments
        // to the agg function.
        final Map<Integer, Integer> sourceOf = new HashMap<>();
        createSelectDistinct(relBuilder, aggregate, argList, filterArg, sourceOf);

        // Now compute the aggregate functions on top of the distinct dataset.
        // Each distinct agg becomes a non-distinct call to the corresponding
        // field from the right; for example,
        //   "COUNT(DISTINCT e.sal)"
        // becomes
        //   "COUNT(distinct_e.sal)".
        final List<AggregateCall> aggCallList = new ArrayList<>();
        final List<AggregateCall> aggCalls = aggregate.getAggCallList();

        final int groupAndIndicatorCount =
                aggregate.getGroupCount() + aggregate.getIndicatorCount();
        int i = groupAndIndicatorCount - 1;
        for (AggregateCall aggCall : aggCalls) {
            ++i;

            // Ignore agg calls which are not distinct or have the wrong set
            // arguments. If we're rewriting aggs whose args are {sal}, we will
            // rewrite COUNT(DISTINCT sal) and SUM(DISTINCT sal) but ignore
            // COUNT(DISTINCT gender) or SUM(sal).
            if (!aggCall.isDistinct()) {
                continue;
            }
            if (!aggCall.getArgList().equals(argList)) {
                continue;
            }

            // Re-map arguments.
            final int argCount = aggCall.getArgList().size();
            //FIXME count function not support more than one argument, if argCount > 1, should use count(*) instead of count(c1,c2...)
            final List<Integer> newArgs = new ArrayList<>(argCount);
            if(argCount <=1)
                for (int j = 0; j < argCount; j++) {
                    final Integer arg = aggCall.getArgList().get(j);
                    newArgs.add(sourceOf.get(arg));
                }

            final int newFilterArg =
                    aggCall.filterArg >= 0 ? sourceOf.get(aggCall.filterArg) : -1;
            final AggregateCall newAggCall =
                    AggregateCall.create(aggCall.getAggregation(), false, newArgs,
                            newFilterArg, aggCall.getType(), aggCall.getName());
            assert refs.get(i) == null;
            if (n == 0) {
                refs.set(i,
                        new RexInputRef(groupAndIndicatorCount + aggCallList.size(),
                                newAggCall.getType()));
            } else {
                refs.set(i,
                        new RexInputRef(leftFields.size() + groupAndIndicatorCount
                                + aggCallList.size(), newAggCall.getType()));
            }
            aggCallList.add(newAggCall);
        }

        final Map<Integer, Integer> map = new HashMap<>();
        for (Integer key : aggregate.getGroupSet()) {
            map.put(key, map.size());
        }
        final ImmutableBitSet newGroupSet = aggregate.getGroupSet().permute(map);
        assert newGroupSet
                .equals(ImmutableBitSet.range(aggregate.getGroupSet().cardinality()));
        ImmutableList<ImmutableBitSet> newGroupingSets = null;
        if (aggregate.indicator) {
            newGroupingSets =
                    ImmutableBitSet.ORDERING.immutableSortedCopy(
                            ImmutableBitSet.permute(aggregate.getGroupSets(), map));
        }

        relBuilder.push(
                aggregate.copy(aggregate.getTraitSet(), relBuilder.build(),
                        aggregate.indicator, newGroupSet, newGroupingSets, aggCallList));

        // If there's no left child yet, no need to create the join
        if (n == 0) {
            return;
        }

        // Create the join condition. It is of the form
        //  'left.f0 = right.f0 and left.f1 = right.f1 and ...'
        // where {f0, f1, ...} are the GROUP BY fields.
        final List<RelDataTypeField> distinctFields =
                relBuilder.peek().getRowType().getFieldList();
        final List<RexNode> conditions = Lists.newArrayList();
        for (i = 0; i < groupAndIndicatorCount; ++i) {
            // null values form its own group
            // use "is not distinct from" so that the join condition
            // allows null values to match.
            conditions.add(
                    rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                            RexInputRef.of(i, leftFields),
                            new RexInputRef(leftFields.size() + i,
                                    distinctFields.get(i).getType())));
        }

        // Join in the new 'select distinct' relation.
        relBuilder.join(JoinRelType.INNER, conditions);
    }

    private static void rewriteAggCalls(
            List<AggregateCall> newAggCalls,
            List<Integer> argList,
            Map<Integer, Integer> sourceOf) {
        // Rewrite the agg calls. Each distinct agg becomes a non-distinct call
        // to the corresponding field from the right; for example,
        // "COUNT(DISTINCT e.sal)" becomes   "COUNT(distinct_e.sal)".
        for (int i = 0; i < newAggCalls.size(); i++) {
            final AggregateCall aggCall = newAggCalls.get(i);

            // Ignore agg calls which are not distinct or have the wrong set
            // arguments. If we're rewriting aggregates whose args are {sal}, we will
            // rewrite COUNT(DISTINCT sal) and SUM(DISTINCT sal) but ignore
            // COUNT(DISTINCT gender) or SUM(sal).
            if (!aggCall.isDistinct()) {
                continue;
            }
            if (!aggCall.getArgList().equals(argList)) {
                continue;
            }

            // Re-map arguments.
            final int argCount = aggCall.getArgList().size();
            final List<Integer> newArgs = new ArrayList<>(argCount);
            for (int j = 0; j < argCount; j++) {
                final Integer arg = aggCall.getArgList().get(j);
                newArgs.add(sourceOf.get(arg));
            }
            final AggregateCall newAggCall =
                    AggregateCall.create(aggCall.getAggregation(), false, newArgs, -1,
                            aggCall.getType(), aggCall.getName());
            newAggCalls.set(i, newAggCall);
        }
    }

    /**
     * Given an {@link org.apache.calcite.rel.logical.LogicalAggregate}
     * and the ordinals of the arguments to a
     * particular call to an aggregate function, creates a 'select distinct'
     * relational expression which projects the group columns and those
     * arguments but nothing else.
     *
     * <p>For example, given
     *
     * <blockquote>
     * <pre>select f0, count(distinct f1), count(distinct f2)
     * from t group by f0</pre>
     * </blockquote>
     *
     * and the argument list
     *
     * <blockquote>{2}</blockquote>
     *
     * returns
     *
     * <blockquote>
     * <pre>select distinct f0, f2 from t</pre>
     * </blockquote>
     *
     * '
     *
     * <p>The <code>sourceOf</code> map is populated with the source of each
     * column; in this case sourceOf.get(0) = 0, and sourceOf.get(1) = 2.</p>
     *
     * @param relBuilder Relational expression builder
     * @param aggregate Aggregate relational expression
     * @param argList   Ordinals of columns to make distinct
     * @param filterArg Ordinal of column to filter on, or -1
     * @param sourceOf  Out parameter, is populated with a map of where each
     *                  output field came from
     * @return Aggregate relational expression which projects the required
     * columns
     */
    private RelBuilder createSelectDistinct(RelBuilder relBuilder,
                                            Aggregate aggregate, List<Integer> argList, int filterArg,
                                            Map<Integer, Integer> sourceOf) {
        relBuilder.push(aggregate.getInput());
        final List<Pair<RexNode, String>> projects = new ArrayList<>();
        final List<RelDataTypeField> childFields =
                relBuilder.peek().getRowType().getFieldList();
        for (int i : aggregate.getGroupSet()) {
            sourceOf.put(i, projects.size());
            projects.add(RexInputRef.of2(i, childFields));
        }
        for (Integer arg : argList) {
            if (filterArg >= 0) {
                // Implement
                //   agg(DISTINCT arg) FILTER $f
                // by generating
                //   SELECT DISTINCT ... CASE WHEN $f THEN arg ELSE NULL END AS arg
                // and then applying
                //   agg(arg)
                // as usual.
                //
                // It works except for (rare) agg functions that need to see null
                // values.
                final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
                final RexInputRef filterRef = RexInputRef.of(filterArg, childFields);
                final Pair<RexNode, String> argRef = RexInputRef.of2(arg, childFields);
                RexNode condition =
                        rexBuilder.makeCall(SqlStdOperatorTable.CASE, filterRef,
                                argRef.left,
                                rexBuilder.ensureType(argRef.left.getType(),
                                        rexBuilder.constantNull(), true));
                sourceOf.put(arg, projects.size());
                projects.add(Pair.of(condition, "i$" + argRef.right));
                continue;
            }
            if (sourceOf.get(arg) != null) {
                continue;
            }
            sourceOf.put(arg, projects.size());
            projects.add(RexInputRef.of2(arg, childFields));
        }
        relBuilder.project(Pair.left(projects), Pair.right(projects));

        // Get the distinct values of the GROUP BY fields and the arguments
        // to the agg functions.
        relBuilder.push(
                aggregate.copy(aggregate.getTraitSet(), relBuilder.build(), false,
                        ImmutableBitSet.range(projects.size()),
                        null, ImmutableList.<AggregateCall>of()));
        return relBuilder;
    }
}

// End AggregateExpandDistinctAggregatesRule.java
