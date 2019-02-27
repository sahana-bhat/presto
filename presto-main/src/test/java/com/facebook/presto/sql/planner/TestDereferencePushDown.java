/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.Ordering;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.sort;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.unnest;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.tree.SortItem.NullOrdering.LAST;
import static com.facebook.presto.sql.tree.SortItem.Ordering.ASCENDING;

public class TestDereferencePushDown
        extends BasePlanTest
{
    private static final String VALUES = "(values ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))";

    @Test
    public void testJoin()
    {
        assertPlan(" with t1 as ( select * from " + VALUES + " as t (msg) ) select b.msg.x from t1 a, t1 b where a.msg.y = b.msg.y",
                output(ImmutableList.of("x"),
                        join(INNER, ImmutableList.of(equiJoinClause("left_y", "right_y")),
                                anyTree(
                                        project(ImmutableMap.of("left_y", expression("field.y")),
                                                values("field"))
                                ), anyTree(
                                        project(ImmutableMap.of("right_y", expression("field1.y"), "x", expression("field1.x")),
                                                values("field1"))))));

        assertPlan("WITH t(msg) AS (SELECT * FROM (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))) " +
                        "SELECT b.msg.x FROM t a, t b WHERE a.msg.y = b.msg.y",
                output(ImmutableList.of("b_x"),
                        join(INNER, ImmutableList.of(equiJoinClause("a_y", "b_y")),
                                anyTree(
                                        project(ImmutableMap.of("a_y", expression("msg.y")),
                                                values("msg"))
                                ), anyTree(
                                        project(ImmutableMap.of("b_y", expression("msg.y"), "b_x", expression("msg.x")),
                                                values("msg"))))));

        assertPlan("WITH t(msg) AS ( SELECT * FROM (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))) " +
                        "SELECT a.msg.y FROM t a JOIN t b ON a.msg.y = b.msg.y WHERE a.msg.x > bigint '5'",
                output(ImmutableList.of("a_y"),
                        join(INNER, ImmutableList.of(equiJoinClause("a_y", "b_y")),
                                anyTree(
                                        project(ImmutableMap.of("a_y", expression("msg.y")),
                                                filter("msg.x > bigint '5'",
                                                        values("msg")))
                                ), anyTree(
                                        project(ImmutableMap.of("b_y", expression("msg.y")),
                                                values("msg"))))));

        assertPlan("WITH t(msg) AS ( SELECT * FROM (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))) " +
                        "SELECT b.msg.x FROM t a JOIN t b ON a.msg.y = b.msg.y WHERE a.msg.x + b.msg.x < bigint '10'",
                output(ImmutableList.of("b_x"),
                        join(INNER, ImmutableList.of(equiJoinClause("a_y", "b_y")),
                                Optional.of("a_x + b_x < bigint '10'"),
                                anyTree(
                                        project(ImmutableMap.of("a_y", expression("msg.y"), "a_x", expression("msg.x")),
                                                values("msg"))
                                ), anyTree(
                                        project(ImmutableMap.of("b_y", expression("msg.y"), "b_x", expression("msg.x")),
                                                values("msg"))))));
    }

    @Test
    public void testInCase()
    {
        // Test dereferences in then clause will not be eagerly evaluated.
        String statement = "with t as (select * from (values cast(array[CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE)),ROW(1, 2.0)] as array<ROW(x BIGINT, y DOUBLE)>)) as t (arr) ) " +
                "select case when cardinality(arr) > cast(0 as bigint) then arr[cast(1 as bigint)].x end from t";
        assertPlan(statement,
                output(ImmutableList.of("x"),
                        project(ImmutableMap.of("x", expression("case when cardinality(field) > bigint '0' then field[bigint '1'].x end")), values("field"))));
    }

    @Test
    public void testFilter()
    {
        assertPlan(" with t1 as ( select * from " + VALUES + " as t (msg) ) select a.msg.y from t1 a join t1 b on a.msg.y = b.msg.y where a.msg.x > bigint '5'",
                output(ImmutableList.of("left_y"),
                        join(INNER, ImmutableList.of(equiJoinClause("left_y", "right_y")),
                                anyTree(
                                        project(ImmutableMap.of("left_y", expression("field.y")),
                                                filter("field.x > bigint '5'", values("field")))
                                ), anyTree(
                                        project(ImmutableMap.of("right_y", expression("field1.y")),
                                                values("field1"))))));
    }

    @Test
    public void testSemiJoin()
    {
        assertPlan("WITH t(msg) AS (SELECT * FROM (VALUES ROW(CAST(ROW(1, 2.0, 3) AS ROW(x BIGINT, y DOUBLE, z BIGINT))))) " +
                        "SELECT msg.y FROM t WHERE msg.x IN (SELECT msg.z FROM t)",
                anyTree(
                        semiJoin("a_x", "b_z", "SEMI_JOIN_RESULT",
                                anyTree(
                                        project(ImmutableMap.of("a_x", expression("msg.x"), "a_y", expression("msg.y")),
                                                values("msg"))),
                                anyTree(
                                        project(ImmutableMap.of("b_z", expression("msg.z")),
                                                values("msg"))))));
    }

    @Test
    public void testLimit()
    {
        assertPlan("WITH t(msg) AS (SELECT * FROM (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))) " +
                        "SELECT b.msg.x FROM t a, t b WHERE a.msg.y = b.msg.y limit 100",
                anyTree(join(INNER, ImmutableList.of(equiJoinClause("a_y", "b_y")),
                        anyTree(
                                project(ImmutableMap.of("a_y", expression("msg.y")),
                                        values("msg"))
                        ), anyTree(
                                project(ImmutableMap.of("b_y", expression("msg.y"), "b_x", expression("msg.x")),
                                        values("msg"))))));

        assertPlan("WITH t(msg) AS ( SELECT * FROM (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))) " +
                        "SELECT a.msg.y FROM t a JOIN t b ON a.msg.y = b.msg.y WHERE a.msg.x > bigint '5' limit 100",
                anyTree(join(INNER, ImmutableList.of(equiJoinClause("a_y", "b_y")),
                        anyTree(
                                project(ImmutableMap.of("a_y", expression("msg.y")),
                                        filter("msg.x > bigint '5'",
                                                values("msg")))
                        ), anyTree(
                                project(ImmutableMap.of("b_y", expression("msg.y")),
                                        values("msg"))))));

        assertPlan("WITH t(msg) AS ( SELECT * FROM (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))) " +
                        "SELECT b.msg.x FROM t a JOIN t b ON a.msg.y = b.msg.y WHERE a.msg.x + b.msg.x < bigint '10' limit 100",
                anyTree(join(INNER, ImmutableList.of(equiJoinClause("a_y", "b_y")),
                        Optional.of("a_x + b_x < bigint '10'"),
                        anyTree(
                                project(ImmutableMap.of("a_y", expression("msg.y"), "a_x", expression("msg.x")),
                                        values("msg"))
                        ), anyTree(
                                project(ImmutableMap.of("b_y", expression("msg.y"), "b_x", expression("msg.x")),
                                        values("msg"))))));
    }

    @Test
    public void testSort()
    {
        ImmutableList<Ordering> orderBy = ImmutableList.of(sort("b_x", ASCENDING, LAST));
        assertPlan("WITH t(msg) AS ( SELECT * FROM (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE))))) " +
                        "SELECT a.msg.x FROM t a JOIN t b ON a.msg.y = b.msg.y WHERE a.msg.x < bigint '10' ORDER BY b.msg.x",
                output(ImmutableList.of("expr"),
                        project(ImmutableMap.of("expr", expression("a_x")),
                                exchange(LOCAL, GATHER, orderBy,
                                        sort(orderBy,
                                                exchange(LOCAL, REPARTITION,
                                                        join(INNER, ImmutableList.of(equiJoinClause("a_y", "b_y")),
                                                                anyTree(
                                                                        project(ImmutableMap.of("a_y", expression("msg.y"), "a_x", expression("msg.x")),
                                                                                filter("msg.x < bigint '10'",
                                                                                        values("msg")))
                                                                ), anyTree(
                                                                        project(ImmutableMap.of("b_y", expression("msg.y"), "b_x", expression("msg.x")),
                                                                                values("msg"))))))))));
    }

    @Test
    public void testUnnest()
    {
        assertPlan("WITH t(msg, array) AS (SELECT * FROM (VALUES ROW(CAST(ROW(1, 2.0) AS ROW(x BIGINT, y DOUBLE)), ARRAY[1, 2, 3]))) " +
                        "SELECT a.msg.x FROM t a JOIN t b ON a.msg.y = b.msg.y CROSS JOIN UNNEST (a.array) WHERE a.msg.x + b.msg.x < bigint '10'",
                output(ImmutableList.of("expr"),
                        project(ImmutableMap.of("expr", expression("a_x")),
                                unnest(
                                        join(INNER, ImmutableList.of(equiJoinClause("a_y", "b_y")),
                                                Optional.of("a_x + b_x < bigint '10'"),
                                                anyTree(
                                                        project(ImmutableMap.of("a_y", expression("msg.y"), "a_x", expression("msg.x"), "a_z", expression("array")),
                                                                values("msg", "array"))
                                                ), anyTree(
                                                        project(ImmutableMap.of("b_y", expression("msg.y"), "b_x", expression("msg.x")),
                                                                values("msg"))))))));
    }
}
