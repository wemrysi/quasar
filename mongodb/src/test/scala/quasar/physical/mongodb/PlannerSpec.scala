/*
 * Copyright 2014–2017 SlamData Inc.
 *
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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar._
import quasar.contrib.specs2._
import quasar.fs._
import quasar.javascript._
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner._
import quasar.physical.mongodb.workflow._
import quasar.sql._

import scala.Either

import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._

class PlannerSpec extends
    PlannerWorkflowHelpers with
    PendingWithActualTracking {

  //to write the new actuals:
  // override val mode = WriteMode

  import Grouped.grouped
  import Reshape.reshape
  import jscore._
  import CollectionUtil._

  import fixExprOp._
  import PlannerHelpers._

  def plan(query: Fix[Sql]): Either[FileSystemError, Crystallized[WorkflowF]] =
    PlannerHelpers.plan(query)

  def trackPending(name: String, plan: => Either[FileSystemError, Crystallized[WorkflowF]], expectedOps: IList[MongoOp]) = {
    name >> {
      lazy val plan0 = plan

      "plan" in {
        plan0 must beRight.which(cwf => notBrokenWithOps(cwf.op, expectedOps))
      }.pendingUntilFixed

      "track" in {
        plan0 must beRight.which(cwf => trackActual(cwf, testFile(s"plan $name")))
      }
    }
  }

  "plan from query string" should {

    trackPending(
      "filter with both index and key projections",
      plan(sqlE"""select count(parents[0].sha) as count from slamengine_commits where parents[0].sha = "56d1caf5d082d1a6840090986e277d36d03f1859" """),
      IList(ReadOp, MatchOp, SimpleMapOp, GroupOp))

    "having with multiple projections" in {
      plan(sqlE"select city, sum(pop) from extraSmallZips group by city having sum(pop) > 40000") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, GroupOp, MatchOp, ProjectOp)))
        // Q3021
        // FIXME Fails with:
        // [error] x having with multiple projections (5 seconds, 908 ms)
        // [error]  'Left(QScriptPlanningFailed(InternalError(Invalid filter predicate, 'qsu14, must be a mappable function of 'qsu8.,None)))' is not Right (PlannerSpec.scala:65)
    }.pendingUntilFixed

    "select partially-applied substring" in {
      plan3_2(sqlE"""select substring("abcdefghijklmnop", 5, trunc(pop / 10000)) from extraSmallZips""") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, ProjectOp)))
    }

    "sort wildcard on expression" in {
      plan(sqlE"select * from zips order by pop/10 desc") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, SimpleMapOp, SortOp, ProjectOp)))
    }

    "sort with expression and alias" in {
      plan(sqlE"select pop/1000 as popInK from zips order by popInK") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, ProjectOp, SortOp)))
    }

    "sort with expression, alias, and filter" in {
      plan(sqlE"select pop/1000 as popInK from zips where pop >= 1000 order by popInK") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, MatchOp, ProjectOp, SortOp)))
    }

    "useful group by" in {
      plan(sqlE"""select city || ", " || state, sum(pop) from extraSmallZips group by city, state""") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, GroupOp, ProjectOp)))
    }

    // Q3171
    // FIXME errors with
    // [error]     java.lang.IndexOutOfBoundsException: 1
    // [error]     	at scala.collection.LinearSeqOptimized$class.apply(LinearSeqOptimized.scala:65)
    // [error]     	at scala.collection.immutable.List.apply(List.scala:84)
    // [error]     	at scala.collection.immutable.List.apply(List.scala:84)
    // [error]     	at scalaz.$bslash$div.fold(Either.scala:57)
    // [error]     	at quasar.qscript.analysis.OutlineInstances$$anon$3.quasar$qscript$analysis$OutlineInstances$$anon$3$$$anonfun$24(Outline.scala:263)
    // trackPending(
    //   "group by simple expression",
    //   plan(sqlE"select city, sum(pop) from extraSmallZips group by lower(city)"),
    //   IList(ReadOp, GroupOp, UnwindOp))

    "group by month" in {
      plan(sqlE"""select avg(epoch), date_part("month", `ts`) from days group by date_part("month", `ts`)""") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, GroupOp, ProjectOp)))
    }

    // FIXME: Needs an actual expectation and an IT
    "expr3 with grouping" in {
      plan(sqlE"select case when pop > 1000 then city else lower(city) end, count(*) from zips group by city") must
        beRight
        // Q3114
        // FIXME fails with: an implementation is missing (ReifyIdentities.scala:358)
    }.pendingUntilFixed

    "plan count and sum grouped by single field" in {
      plan(sqlE"select count(*) as cnt, sum(pop) as sm from zips group by state") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, GroupOp, ProjectOp)))
    }

    trackPending(
      "collect unaggregated fields into single doc when grouping",
      plan(sqlE"select city, state, sum(pop) from zips"),
      IList(ReadOp, ProjectOp, GroupOp, UnwindOp, ProjectOp))

    "plan unaggregated field when grouping, second case" in {
      plan(sqlE"select city, state, sum(pop) from zips") must
        beRight.which { cwf =>
          rootPushes(cwf.op) must_== Nil
          notBrokenWithOps(cwf.op, IList(ReadOp, GroupOp, UnwindOp, ProjectOp))
        }
    }.pendingUntilFixed

    trackPending(
      "double aggregation with another projection",
      plan(sqlE"select sum(avg(pop)), min(city) from zips group by state"),
      IList(ReadOp, GroupOp, GroupOp, UnwindOp))

    trackPending(
      "multiple expressions using same field",
      plan(sqlE"select pop, sum(pop), pop/1000 from zips"),
      IList(ReadOp, ProjectOp, GroupOp, UnwindOp, ProjectOp))

    "plan sum of expression in expression with another projection when grouped" in {
      plan(sqlE"select city, sum(pop-1)/1000 from zips group by city") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, GroupOp, ProjectOp)))
    }

    trackPending(
      "length of min (JS on top of reduce)",
      plan3_2(sqlE"select state, length(min(city)) as shortest from zips group by state"),
      IList(ReadOp, GroupOp, ProjectOp, SimpleMapOp, ProjectOp))

    "plan js expr grouped by js expr" in {
      plan3_2(sqlE"select length(city) as len, count(*) as cnt from zips group by length(city)") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, SimpleMapOp, GroupOp, ProjectOp)))
    }

    "plan expressions with ~"in {
      plan(sqlE"""select foo ~ "bar.*", "abc" ~ "a|b", "baz" ~ regex, target ~ regex from a""") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, SimpleMapOp, ProjectOp)))
    }

    trackPending(
      "object flatten",
      plan(sqlE"select geo{*} from usa_factbook"),
      IList(ReadOp, SimpleMapOp, ProjectOp))

    trackPending(
      "array concat with filter",
      plan(sqlE"""select loc || [ pop ] from zips where city = "BOULDER" """),
      IList(ReadOp, MatchOp, SimpleMapOp, ProjectOp))

    trackPending(
      "array flatten with unflattened field",
      plan(sqlE"SELECT `_id` as zip, loc as loc, loc[*] as coord FROM zips"),
      IList(ReadOp, ProjectOp, UnwindOp, ProjectOp))

    // Q3021
    // FIXME fails with:
    // 'Left(QScriptPlanningFailed(InternalError(Invalid filter predicate, 'qsu10, must be a mappable function of 'qsu4.,None)))' is not Right (PlannerSpec.scala:63)
    // trackPending(
    //   "unify flattened fields",
    //   plan(sqlE"select loc[*] from zips where loc[*] < 0"),
    //   IList(ReadOp, ProjectOp, UnwindOp, MatchOp, ProjectOp))

    // Q3021
    // FIXME fails with:
    // 'Left(QScriptPlanningFailed(InternalError(Invalid group key, 'qsu15, must be a mappable function of 'qsu4.,None)))' is not Right (PlannerSpec.scala:63)
    // trackPending(
    //   "group by flattened field",
    //   plan(sqlE"select substring(parents[*].sha, 0, 1), count(*) from slamengine_commits group by substring(parents[*].sha, 0, 1)"),
    //   IList(ReadOp, ProjectOp, UnwindOp, GroupOp, ProjectOp))

    trackPending(
      "unify flattened fields with unflattened field",
      plan(sqlE"select `_id` as zip, loc[*] from zips order by loc[*]"),
      IList(ReadOp, ProjectOp, UnwindOp, SortOp))

    // Q3021
    // FIXME fails with:
    // 'Left(QScriptPlanningFailed(InternalError(Invalid filter predicate, 'qsu22, must be a mappable function of 'qsu4.,None)))' is not Right (PlannerSpec.scala:63)
    // trackPending(
    //   "unify flattened with double-flattened",
    //   plan(sqlE"""select * from user_comments where (comments[*].id LIKE "%Dr%" OR comments[*].replyTo[*] LIKE "%Dr%")"""),
    //   IList(ReadOp, ProjectOp, UnwindOp, ProjectOp, UnwindOp, MatchOp, ProjectOp))

    "plan complex group by with sorting and limiting" in {
      plan(sqlE"SELECT city, SUM(pop) AS pop FROM zips GROUP BY city ORDER BY pop") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, GroupOp, ProjectOp, SortOp)))
    }

    "plan implicit group by with filter" in {
      plan(sqlE"""select avg(pop), min(city) from zips where state = "CO" """) must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, MatchOp, GroupOp, ProjectOp)))
    }

    "plan distinct as expression" in {
      plan(sqlE"select count(distinct(city)) from zips") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, GroupOp, GroupOp, ProjectOp)))
    }

    "plan distinct of expression as expression" in {
      plan(sqlE"select count(distinct substring(city, 0, 1)) from zips") must
        beRight.which(cwf => notBrokenWithOps(cwf.op, IList(ReadOp, GroupOp, GroupOp, ProjectOp)))
    }

    trackPending(
      "distinct with unrelated order by",
      plan(sqlE"select distinct city from zips order by pop desc"),
      IList(ReadOp, ProjectOp, SortOp, GroupOp, ProjectOp, SortOp, ProjectOp))

    trackPending(
      "distinct with sum and group",
      plan(sqlE"SELECT DISTINCT SUM(pop) AS totalPop, city, state FROM zips GROUP BY city"),
      IList(ReadOp, GroupOp, ProjectOp, UnwindOp, GroupOp, ProjectOp))

    trackPending(
      "distinct with sum, group, and orderBy",
      plan(sqlE"SELECT DISTINCT SUM(pop) AS totalPop, city, state FROM zips GROUP BY city ORDER BY totalPop DESC"),
      IList(ReadOp, GroupOp, ProjectOp, UnwindOp, SortOp, GroupOp, ProjectOp, SortOp))

    "plan time_of_day (JS)" in {
      plan(sqlE"select time_of_day(ts) from days") must
        beRight // NB: way too complicated to spell out here, and will change as JS generation improves
    }

    "plan non-equi join" in {
      plan(sqlE"select zips2.city from zips join zips2 on zips.`_id` < zips2.`_id`") must
      beWorkflow0(
        joinStructure(
          $read(collection("db", "zips")), "__tmp0", $$ROOT,
          $read(collection("db", "zips2")),
          $literal(Bson.Null),
          jscore.Literal(Js.Null).right,
          chain[Workflow](_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              JoinHandler.LeftName -> Selector.NotExpr(Selector.Size(0)),
              JoinHandler.RightName -> Selector.NotExpr(Selector.Size(0))))),
            $unwind(DocField(JoinHandler.LeftName)),
            $unwind(DocField(JoinHandler.RightName)),
            $project(
              reshape(
                "__tmp11"   ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Right.name),
                    $literal(Bson.Undefined)),
                "__tmp12" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr(List())))),
                    $cond(
                      $or(
                        $and(
                          $lt($literal(Bson.Null), $field(JoinDir.Right.name, "_id")),
                          $lt($field(JoinDir.Right.name, "_id"), $literal(Bson.Doc()))),
                        $and(
                          $lte($literal(Bson.Bool(false)), $field(JoinDir.Right.name, "_id")),
                          $lt($field(JoinDir.Right.name, "_id"), $literal(Bson.Regex("", ""))))),
                      $cond(
                        $and(
                          $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                          $lt($field(JoinDir.Left.name), $literal(Bson.Arr(List())))),
                        $cond(
                          $or(
                            $and(
                              $lt($literal(Bson.Null), $field(JoinDir.Left.name, "_id")),
                              $lt($field(JoinDir.Left.name, "_id"), $literal(Bson.Doc()))),
                            $and(
                              $lte($literal(Bson.Bool(false)), $field(JoinDir.Left.name, "_id")),
                              $lt($field(JoinDir.Left.name, "_id"), $literal(Bson.Regex("", ""))))),
                          $lt($field(JoinDir.Left.name, "_id"), $field(JoinDir.Right.name, "_id")),
                          $literal(Bson.Undefined)),
                        $literal(Bson.Undefined)),
                      $literal(Bson.Undefined)),
                    $literal(Bson.Undefined))),
              IgnoreId),
            $match(
              Selector.Doc(
                BsonField.Name("__tmp12") -> Selector.Eq(Bson.Bool(true)))),
            $project(
              reshape(sigil.Quasar -> $field("__tmp11", "city")),
              ExcludeId)),
          false).op)
    }.pendingWithActual(notOnPar, testFile("plan non-equi join"))

    "plan simple inner equi-join (map-reduce)" in {
      plan2_6(
        sqlE"select foo.name, bar.address from foo join bar on foo.id = bar.foo_id") must
      beWorkflow0(
        joinStructure(
          $read(collection("db", "foo")), "__tmp0", $$ROOT,
          $read(collection("db", "bar")),
          reshape("0" -> $field("id")),
          Obj(ListMap(Name("0") -> Select(ident(sigil.Quasar), "foo_id"))).right,
          chain[Workflow](_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              JoinHandler.LeftName -> Selector.NotExpr(Selector.Size(0)),
              JoinHandler.RightName -> Selector.NotExpr(Selector.Size(0))))),
            $unwind(DocField(JoinHandler.LeftName)),
            $unwind(DocField(JoinHandler.RightName)),
            $project(
              reshape(
                "name"    ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                      $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Left.name, "name"),
                    $literal(Bson.Undefined)),
                "address" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Right.name, "address"),
                    $literal(Bson.Undefined))),
              IgnoreId)),
          false).op)
    }.pendingWithActual(notOnPar, testFile("plan simple inner equi-join (map-reduce)"))

    "plan simple inner equi-join with expression ($lookup)" in {
      plan3_4(
        sqlE"select zips.city, smallZips.state from zips join smallZips on lower(zips.`_id`) = smallZips.`_id`",
        defaultStats,
        defaultIndexes,
        emptyDoc) must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $project(reshape(
          JoinDir.Left.name -> $$ROOT,
          "__tmp0" -> $toLower($field("_id"))),
          IgnoreId),
        $lookup(
          CollectionName("smallZips"),
          BsonField.Name("_id"),
          BsonField.Name("__tmp0"),
          JoinHandler.RightName),
        $project(reshape(
          JoinDir.Left.name -> $field(JoinDir.Left.name),
          JoinDir.Right.name -> $field(JoinDir.Right.name))),
        $unwind(DocField(JoinHandler.RightName)),
        $project(reshape(
          "city" ->
            $cond(
              $and(
                $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
              $field(JoinDir.Left.name, "city"),
              $literal(Bson.Undefined)),
          "state" ->
            $cond(
              $and(
                $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
              $field(JoinDir.Right.name, "state"),
              $literal(Bson.Undefined))),
          IgnoreId)))
    }.pendingWithActual(notOnPar, testFile("plan simple inner equi-join with expression ($lookup)"))

    "plan simple inner equi-join with pre-filtering ($lookup)" in {
      plan3_4(
        sqlE"select zips.city, smallZips.state from zips join smallZips on zips.`_id` = smallZips.`_id` where smallZips.pop >= 10000",
        defaultStats,
        defaultIndexes,
        emptyDoc) must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "smallZips")),
        $match(
          Selector.And(
            isNumeric(BsonField.Name("pop")),
            Selector.Doc(
              BsonField.Name("pop") -> Selector.Gte(Bson.Int32(10000))))),
        $project(reshape(
          JoinDir.Right.name -> $$ROOT,
          "__tmp2" -> $field("_id")),
          ExcludeId),
        $lookup(
          CollectionName("zips"),
          BsonField.Name("__tmp2"),
          BsonField.Name("_id"),
          JoinHandler.LeftName),
        $project(reshape(
          JoinDir.Right.name -> $field(JoinDir.Right.name),
          JoinDir.Left.name -> $field(JoinDir.Left.name))),
        $unwind(DocField(JoinHandler.LeftName)),
        $project(reshape(
          "city" ->
            $cond(
              $and(
                $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
              $field(JoinDir.Left.name, "city"),
              $literal(Bson.Undefined)),
          "state" ->
            $cond(
              $and(
                $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
              $field(JoinDir.Right.name, "state"),
              $literal(Bson.Undefined))),
          IgnoreId)))
    }.pendingWithActual(notOnPar, testFile("plan simple inner equi-join with pre-filtering ($lookup)"))

    "plan simple outer equi-join with wildcard" in {
      plan(sqlE"select * from foo full join bar on foo.id = bar.foo_id") must
      beWorkflow0(
        joinStructure(
          $read(collection("db", "foo")), "__tmp0", $$ROOT,
          $read(collection("db", "bar")),
          reshape("0" -> $field("id")),
          Obj(ListMap(Name("0") -> Select(ident("value"), "foo_id"))).right,
          chain[Workflow](_,
            $project(
              reshape(
                JoinDir.Left.name ->
                  $cond($eq($size($field(JoinDir.Left.name)), $literal(Bson.Int32(0))),
                    $literal(Bson.Arr(List(Bson.Doc()))),
                    $field(JoinDir.Left.name)),
                JoinDir.Right.name ->
                  $cond($eq($size($field(JoinDir.Right.name)), $literal(Bson.Int32(0))),
                    $literal(Bson.Arr(List(Bson.Doc()))),
                    $field(JoinDir.Right.name))),
              IgnoreId),
            $unwind(DocField(JoinHandler.LeftName)),
            $unwind(DocField(JoinHandler.RightName)),
            $simpleMap(
              NonEmptyList(
                MapExpr(JsFn(Name("x"),
                  Obj(ListMap(
                    Name("__tmp7") ->
                      If(
                        BinOp(jscore.And,
                          Call(ident("isObject"), List(Select(ident("x"), JoinDir.Right.name))),
                          UnOp(jscore.Not,
                            Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), JoinDir.Right.name))))),
                        If(
                          BinOp(jscore.And,
                            Call(ident("isObject"), List(Select(ident("x"), JoinDir.Left.name))),
                            UnOp(jscore.Not,
                              Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), JoinDir.Left.name))))),
                          SpliceObjects(List(
                            Select(ident("x"), JoinDir.Left.name),
                            Select(ident("x"), JoinDir.Right.name))),
                          ident("undefined")),
                        ident("undefined"))))))),
              ListMap()),
            $project(
              reshape(sigil.Quasar -> $field("__tmp7")),
              ExcludeId)),
          false).op)
    }.pendingWithActual(notOnPar, testFile("plan simple outer equi-join with wildcard"))

    "plan simple left equi-join (map-reduce)" in {
      plan(
        sqlE"select foo.name, bar.address from foo left join bar on foo.id = bar.foo_id") must
      beWorkflow0(
        joinStructure(
          $read(collection("db", "foo")), "__tmp0", $$ROOT,
          $read(collection("db", "bar")),
          reshape("0" -> $field("id")),
          Obj(ListMap(Name("0") -> Select(ident("value"), "foo_id"))).right,
          chain[Workflow](_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              JoinHandler.LeftName -> Selector.NotExpr(Selector.Size(0))))),
            $project(
              reshape(
                JoinDir.Left.name  -> $field(JoinDir.Left.name),
                JoinDir.Right.name ->
                  $cond($eq($size($field(JoinDir.Right.name)), $literal(Bson.Int32(0))),
                    $literal(Bson.Arr(List(Bson.Doc()))),
                    $field(JoinDir.Right.name))),
              IgnoreId),
            $unwind(DocField(JoinHandler.LeftName)),
            $unwind(DocField(JoinHandler.RightName)),
            $project(
              reshape(
                "name"    ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                      $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Left.name, "name"),
                    $literal(Bson.Undefined)),
                "address" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Right.name, "address"),
                    $literal(Bson.Undefined))),
              IgnoreId)),
          false).op)
    }.pendingWithActual(notOnPar, testFile("plan simple left equi-join (map-reduce)"))

    "plan simple left equi-join ($lookup)" in {
      plan3_4(
        sqlE"select foo.name, bar.address from foo left join bar on foo.id = bar.foo_id",
        defaultStats,
        indexes(collection("db", "bar") -> BsonField.Name("foo_id")),
        emptyDoc) must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "foo")),
        $project(reshape(JoinDir.Left.name -> $$ROOT)),
        $lookup(
          CollectionName("bar"),
          JoinHandler.LeftName \ BsonField.Name("id"),
          BsonField.Name("foo_id"),
          JoinHandler.RightName),
        $unwind(DocField(JoinHandler.RightName)),  // FIXME: need to preserve docs with no match
        $project(reshape(
          "name" ->
            $cond(
              $and(
                $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
              $field(JoinDir.Left.name, "name"),
              $literal(Bson.Undefined)),
          "address" ->
            $cond(
              $and(
                $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
              $field(JoinDir.Right.name, "address"),
              $literal(Bson.Undefined))),
          IgnoreId)))
    }.pendingWithActual("TODO: left/right joins in $lookup", testFile("plan simple left equi-join ($lookup)"))

    "plan simple right equi-join ($lookup)" in {
      plan3_4(
        sqlE"select foo.name, bar.address from foo right join bar on foo.id = bar.foo_id",
        defaultStats,
        indexes(collection("db", "bar") -> BsonField.Name("foo_id")),
        emptyDoc) must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "bar")),
        $project(reshape(JoinDir.Right.name -> $$ROOT)),
        $lookup(
          CollectionName("foo"),
          JoinHandler.RightName \ BsonField.Name("foo_id"),
          BsonField.Name("id"),
          JoinHandler.LeftName),
        $unwind(DocField(JoinHandler.LeftName)),  // FIXME: need to preserve docs with no match
        $project(reshape(
          "name" ->
            $cond(
              $and(
                $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
              $field(JoinDir.Left.name, "name"),
              $literal(Bson.Undefined)),
          "address" ->
            $cond(
              $and(
                $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
              $field(JoinDir.Right.name, "address"),
              $literal(Bson.Undefined))),
          IgnoreId)))
    }.pendingWithActual("TODO: left/right joins in $lookup", testFile("plan simple right equi-join ($lookup)"))

    "plan 3-way right equi-join (map-reduce)" in {
      plan2_6(
        sqlE"select foo.name, bar.address, baz.zip from foo join bar on foo.id = bar.foo_id right join baz on bar.id = baz.bar_id") must
      beWorkflow0(
        joinStructure(
          $read(collection("db", "baz")), "__tmp1", $$ROOT,
          joinStructure0(
            $read(collection("db", "foo")), "__tmp0", $$ROOT,
            $read(collection("db", "bar")),
            reshape("0" -> $field("id")),
            Obj(ListMap(Name("0") -> Select(ident("value"), "foo_id"))).right,
            chain[Workflow](_,
              $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                JoinHandler.LeftName -> Selector.NotExpr(Selector.Size(0)),
                JoinHandler.RightName -> Selector.NotExpr(Selector.Size(0))))),
              $unwind(DocField(JoinHandler.LeftName)),
              $unwind(DocField(JoinHandler.RightName))),
            false),
          reshape("0" -> $field("bar_id")),
          Obj(ListMap(Name("0") -> Select(Select(ident("value"), JoinDir.Right.name), "id"))).right,
          chain[Workflow](_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              JoinHandler.LeftName -> Selector.NotExpr(Selector.Size(0))))),
            $project(
              reshape(
                JoinDir.Right.name ->
                  $cond($eq($size($field(JoinDir.Right.name)), $literal(Bson.Int32(0))),
                    $literal(Bson.Arr(List(Bson.Doc()))),
                    $field(JoinDir.Right.name)),
                JoinDir.Left.name -> $field(JoinDir.Left.name)),
              IgnoreId),
            $unwind(DocField(JoinHandler.RightName)),
            $unwind(DocField(JoinHandler.LeftName)),
            $project(
              reshape(
                "name"    ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                      $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
                    $cond(
                      $and(
                        $lte($literal(Bson.Doc()), $field(JoinDir.Left.name, JoinDir.Left.name)),
                        $lt($field(JoinDir.Left.name, JoinDir.Left.name), $literal(Bson.Arr()))),
                      $field(JoinDir.Left.name, JoinDir.Left.name, "name"),
                      $literal(Bson.Undefined)),
                    $literal(Bson.Undefined)),
                "address" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                      $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
                    $cond(
                      $and(
                        $lte($literal(Bson.Doc()), $field(JoinDir.Left.name, JoinDir.Right.name)),
                        $lt($field(JoinDir.Left.name, JoinDir.Right.name), $literal(Bson.Arr()))),
                      $field(JoinDir.Left.name, JoinDir.Right.name, "address"),
                      $literal(Bson.Undefined)),
                    $literal(Bson.Undefined)),
                "zip"     ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Right.name, "zip"),
                    $literal(Bson.Undefined))),
              IgnoreId)),
          true).op)
    }.pendingWithActual(notOnPar, testFile("plan 3-way right equi-join (map-reduce)"))

    "plan 3-way equi-join ($lookup)" in {
      plan3_4(
        sqlE"select extraSmallZips.city, smallZips.state, zips.pop from extraSmallZips join smallZips on extraSmallZips.`_id` = smallZips.`_id` join zips on smallZips.`_id` = zips.`_id`",
        defaultStats,
        defaultIndexes,
        emptyDoc) must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "extraSmallZips")),
          $match(Selector.Doc(
            BsonField.Name("_id") -> Selector.Exists(true))),
          $project(reshape(JoinDir.Left.name -> $$ROOT)),
          $lookup(
            CollectionName("smallZips"),
            JoinHandler.LeftName \ BsonField.Name("_id"),
            BsonField.Name("_id"),
            JoinHandler.RightName),
          $unwind(DocField(JoinHandler.RightName)),
          $match(Selector.Doc(
            JoinHandler.RightName \ BsonField.Name("_id") -> Selector.Exists(true))),
          $project(reshape(JoinDir.Left.name -> $$ROOT)),
          $lookup(
            CollectionName("zips"),
            JoinHandler.LeftName \ JoinHandler.RightName \ BsonField.Name("_id"),
            BsonField.Name("_id"),
            JoinHandler.RightName),
          $unwind(DocField(JoinHandler.RightName)),
          $project(reshape(
            "city" ->
              $cond(
                $and(
                  $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                  $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
                $cond(
                  $and(
                    $lte($literal(Bson.Doc()), $field(JoinDir.Left.name, JoinDir.Left.name)),
                    $lt($field(JoinDir.Left.name, JoinDir.Left.name), $literal(Bson.Arr()))),
                  $field(JoinDir.Left.name, JoinDir.Left.name, "city"),
                  $literal(Bson.Undefined)),
                $literal(Bson.Undefined)),
            "state" ->
              $cond(
                $and(
                  $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                  $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
                $cond(
                  $and(
                    $lte($literal(Bson.Doc()), $field(JoinDir.Left.name, JoinDir.Right.name)),
                    $lt($field(JoinDir.Left.name, JoinDir.Right.name), $literal(Bson.Arr()))),
                  $field(JoinDir.Left.name, JoinDir.Right.name, "state"),
                  $literal(Bson.Undefined)),
                $literal(Bson.Undefined)),
            "pop" ->
              $cond(
                $and(
                  $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                  $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                $field(JoinDir.Right.name, "pop"),
                $literal(Bson.Undefined))),
            IgnoreId)))
    }.pendingWithActual(notOnPar, testFile("plan 3-way equi-join ($lookup)"))

    "plan count of $lookup" in {
      plan3_4(
        sqlE"select tp.`_id`, count(*) from `zips` as tp join `largeZips` as ti on tp.`_id` = ti.TestProgramId group by tp.`_id`",
        defaultStats,
        indexes(),
        emptyDoc) must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "largeZips")),
        $match(Selector.Doc(
          BsonField.Name("TestProgramId") -> Selector.Exists(true))),
        $project(reshape("right" -> $$ROOT), IgnoreId),
        $lookup(
          CollectionName("zips"),
          JoinHandler.RightName \ BsonField.Name("TestProgramId"),
          BsonField.Name("_id"),
          JoinHandler.LeftName),
        $unwind(DocField(JoinHandler.LeftName)),
        $group(
          grouped("1" -> $sum($literal(Bson.Int32(1)))),
          -\/(reshape("0" -> $cond(
            $and(
              $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
              $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
            $field(JoinDir.Left.name, "_id"),
            $literal(Bson.Undefined))))),
        $project(
          reshape(
            "_id" -> $field("_id", "0"),
            "1"   -> $include),
          IgnoreId)))
    }.pendingWithActual(notOnPar, testFile("plan count of $lookup"))

    "plan join with multiple conditions" in {
      plan(sqlE"select l.sha as child, l.author.login as c_auth, r.sha as parent, r.author.login as p_auth from slamengine_commits as l join slamengine_commits as r on r.sha = l.parents[0].sha and l.author.login = r.author.login") must
      beWorkflow0(
        joinStructure(
          chain[Workflow](
            $read(collection("db", "slamengine_commits")),
            $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
              obj(
                "__tmp4" -> Select(Access(Select(ident("x"), "parents"), jscore.Literal(Js.Num(0, false))), "sha"),
                "__tmp5" -> ident("x"),
                "__tmp6" -> Select(Select(ident("x"), "author"), "login"))))),
              ListMap())),
          "__tmp7", $field("__tmp5"),
          $read(collection("db", "slamengine_commits")),
          reshape(
            "0" -> $field("__tmp4"),
            "1" -> $field("__tmp6")),
          obj(
            "0" -> Select(ident("value"), "sha"),
            "1" -> Select(Select(ident("value"), "author"), "login")).right,
          chain[Workflow](_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              JoinHandler.LeftName -> Selector.NotExpr(Selector.Size(0)),
              JoinHandler.RightName -> Selector.NotExpr(Selector.Size(0))))),
            $unwind(DocField(JoinHandler.LeftName)),
            $unwind(DocField(JoinHandler.RightName)),
            $project(
              reshape(
                "child"  ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                      $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Left.name, "sha"),
                    $literal(Bson.Undefined)),
                "c_auth" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                      $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
                    $cond(
                      $and(
                        $lte($literal(Bson.Doc()), $field(JoinDir.Left.name, "author")),
                        $lt($field(JoinDir.Left.name, "author"), $literal(Bson.Arr()))),
                      $field(JoinDir.Left.name, "author", "login"),
                      $literal(Bson.Undefined)),
                    $literal(Bson.Undefined)),
                "parent" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Right.name, "sha"),
                    $literal(Bson.Undefined)),
                "p_auth" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                    $cond(
                      $and(
                        $lte($literal(Bson.Doc()), $field(JoinDir.Right.name, "author")),
                        $lt($field(JoinDir.Right.name, "author"), $literal(Bson.Arr()))),
                      $field(JoinDir.Right.name, "author", "login"),
                      $literal(Bson.Undefined)),
                    $literal(Bson.Undefined))),
              IgnoreId)),
        false).op)
    }.pendingWithActual("#1560", testFile("plan join with multiple conditions"))

    "plan join with non-JS-able condition" in {
      plan(sqlE"select z1.city as city1, z1.loc, z2.city as city2, z2.pop from zips as z1 join zips as z2 on z1.loc[*] = z2.loc[*]") must
      beWorkflow0(
        joinStructure(
          chain[Workflow](
            $read(collection("db", "zips")),
            $project(
              reshape(
                "__tmp0" -> $field("loc"),
                "__tmp1" -> $$ROOT),
              IgnoreId),
            $unwind(DocField(BsonField.Name("__tmp0")))),
          "__tmp2", $field("__tmp1"),
          chain[Workflow](
            $read(collection("db", "zips")),
            $project(
              reshape(
                "__tmp3" -> $field("loc"),
                "__tmp4" -> $$ROOT),
              IgnoreId),
            $unwind(DocField(BsonField.Name("__tmp3")))),
          reshape("0" -> $field("__tmp0")),
          ("__tmp5", $field("__tmp4"), reshape(
            "0" -> $field("__tmp3")).left).left,
          chain[Workflow](_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              JoinHandler.LeftName -> Selector.NotExpr(Selector.Size(0)),
              JoinHandler.RightName -> Selector.NotExpr(Selector.Size(0))))),
            $unwind(DocField(JoinHandler.LeftName)),
            $unwind(DocField(JoinHandler.RightName)),
            $project(
              reshape(
                "city1" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                      $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Left.name, "city"),
                    $literal(Bson.Undefined)),
                "loc" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                      $lt($field(JoinDir.Left.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Left.name, "loc"),
                    $literal(Bson.Undefined)),
                "city2" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Right.name, "city"),
                    $literal(Bson.Undefined)),
                "pop" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Right.name, "pop"),
                    $literal(Bson.Undefined))),
              IgnoreId)),
          false).op)
    // Q3021
    // FIXME fails with:
    // 'Left(QScriptPlanningFailed(InternalError(Invalid join condition, 'qsu21, must be a mappable function of 'qsu11 and 'qsu14.,None)))' is not Right (PendingWithActualTracking.scala:94)
    // }.pendingWithActual(notOnPar, testFile("plan join with non-JS-able condition"))
  }.pendingUntilFixed

    "plan simple cross" in {
      plan(sqlE"select zips2.city from zips, zips2 where zips.pop < zips2.pop") must
      beWorkflow0(
        joinStructure(
          $read(collection("db", "zips")), "__tmp0", $$ROOT,
          $read(collection("db", "zips2")),
          $literal(Bson.Null),
          jscore.Literal(Js.Null).right,
          chain[Workflow](_,
            $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              JoinHandler.LeftName -> Selector.NotExpr(Selector.Size(0)),
              JoinHandler.RightName -> Selector.NotExpr(Selector.Size(0))))),
            $unwind(DocField(JoinHandler.LeftName)),
            $unwind(DocField(JoinHandler.RightName)),
            $project(
              reshape(
                "__tmp11"   ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                    $field(JoinDir.Right.name),
                    $literal(Bson.Undefined)),
                "__tmp12" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                      $lt($field(JoinDir.Right.name), $literal(Bson.Arr(List())))),
                    $cond(
                      $or(
                        $and(
                          $lt($literal(Bson.Null), $field(JoinDir.Right.name, "pop")),
                          $lt($field(JoinDir.Right.name, "pop"), $literal(Bson.Doc()))),
                        $and(
                          $lte($literal(Bson.Bool(false)), $field(JoinDir.Right.name, "pop")),
                          $lt($field(JoinDir.Right.name, "pop"), $literal(Bson.Regex("", ""))))),
                      $cond(
                        $and(
                          $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                          $lt($field(JoinDir.Left.name), $literal(Bson.Arr(List())))),
                        $cond(
                          $or(
                            $and(
                              $lt($literal(Bson.Null), $field(JoinDir.Left.name, "pop")),
                              $lt($field(JoinDir.Left.name, "pop"), $literal(Bson.Doc()))),
                            $and(
                              $lte($literal(Bson.Bool(false)), $field(JoinDir.Left.name, "pop")),
                              $lt($field(JoinDir.Left.name, "pop"), $literal(Bson.Regex("", ""))))),
                          $lt($field(JoinDir.Left.name, "pop"), $field(JoinDir.Right.name, "pop")),
                          $literal(Bson.Undefined)),
                        $literal(Bson.Undefined)),
                      $literal(Bson.Undefined)),
                    $literal(Bson.Undefined))),
              IgnoreId),
            $match(
              Selector.Doc(
                BsonField.Name("__tmp12") -> Selector.Eq(Bson.Bool(true)))),
            $project(
              reshape(sigil.Quasar -> $field("__tmp11", "city")),
              ExcludeId)),
          false).op)
    }.pendingWithActual(notOnPar, testFile("plan simple cross"))

    "SD-1263 specific case of plan multiple reducing projections (all, distinct, orderBy)" in {
      val q = sqlE"select distinct loc || [pop - 1] as p1, pop - 1 as p2 from zips group by territory order by p2".project.asInstanceOf[Select[Fix[Sql]]]

      plan(q.embed) must beRight.which { fop =>
        val wf = fop.op
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        countAccumOps(wf) must_== 1
        countUnwindOps(wf) must_== 0
        countMatchOps(wf) must_== 0
        danglingReferences(wf) must_== Nil
        brokenProjectOps(wf) must_== 0
        appropriateColumns(wf, q)
        rootPushes(wf) must_== Nil
      }
      //Q3154
      //FIXME has dangling reference
    }.pendingUntilFixed
  }
}
