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
import quasar._, RenderTree.ops._
import quasar.common.{Map => _, _}
import quasar.contrib.specs2.PendingWithActualTracking
import quasar.fp._
import quasar.fp.ski._
import quasar.fs._
import quasar.javascript._
import quasar.frontend.{logicalplan => lp}, lp.{LogicalPlan => LP}
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner._
import quasar.physical.mongodb.workflow._
import quasar.qscript.DiscoverPath
import quasar.sql , sql.{fixpoint => sqlF, _}
import quasar.std._

import java.time.Instant
import scala.Either

import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.scalacheck._
import org.specs2.execute._
import org.specs2.matcher.{Matcher, Expectable}
import pathy.Path._
import scalaz._, Scalaz._

class PlannerSpec extends
    org.specs2.mutable.Specification with
    org.specs2.ScalaCheck with
    CompilerHelpers with
    TreeMatchers with
    PendingWithActualTracking {

  import StdLib.{set => s, _}
  import structural._
  import Grouped.grouped
  import Reshape.reshape
  import jscore._
  import CollectionUtil._

  type EitherWriter[A] =
    EitherT[Writer[Vector[PhaseResult], ?], FileSystemError, A]

  val notOnPar = "Not on par with old (LP-based) connector."

  def emit[A: RenderTree](label: String, v: A): EitherWriter[A] =
    EitherT[Writer[PhaseResults, ?], FileSystemError, A](Writer(Vector(PhaseResult.tree(label, v)), v.right))

  case class equalToWorkflow(expected: Workflow, addDetails: Boolean)
      extends Matcher[Crystallized[WorkflowF]] {
    def apply[S <: Crystallized[WorkflowF]](s: Expectable[S]) = {

      val st = RenderTree[Crystallized[WorkflowF]].render(s.value)

      val diff: String = (st diff expected.render).shows

      val details =
        if (addDetails) FailureDetails(st.shows, expected.render.shows)
        else NoDetails

      result(expected == s.value.op,
             "\ntrees are equal:\n" + diff,
             "\ntrees are not equal:\n" + diff,
             s,
             details)
    }
  }

  import fixExprOp._
  val expr3_0Fp: ExprOp3_0F.fixpoint[Fix[ExprOp], ExprOp] =
    new ExprOp3_0F.fixpoint[Fix[ExprOp], ExprOp](_.embed)
  import expr3_0Fp._
  val expr3_2Fp: ExprOp3_2F.fixpoint[Fix[ExprOp], ExprOp] =
    new ExprOp3_2F.fixpoint[Fix[ExprOp], ExprOp](_.embed)
  import expr3_2Fp._
  val expr3_4Fp: ExprOp3_4F.fixpoint[Fix[ExprOp], ExprOp] =
    new ExprOp3_4F.fixpoint[Fix[ExprOp], ExprOp](_.embed)
  import expr3_4Fp._

  val basePath = rootDir[Sandboxed] </> dir("db")

  val listContents: DiscoverPath.ListContents[EitherWriter] =
    dir => (
      if (dir ≟ rootDir)
        Set(
          DirName("db").left[FileName],
          DirName("db1").left,
          DirName("db2").left)
      else
        Set(
          FileName("foo").right[DirName],
          FileName("zips").right,
          FileName("zips2").right,
          FileName("largeZips").right,
          FileName("a").right,
          FileName("slamengine_commits").right,
          FileName("person").right,
          FileName("caloriesBurnedData").right,
          FileName("bar").right,
          FileName("baz").right,
          FileName("usa_factbook").right,
          FileName("user_comments").right,
          FileName("days").right,
          FileName("logs").right,
          FileName("usa_factbook").right,
          FileName("cities").right)).point[EitherWriter]

  implicit val monadTell: MonadTell[EitherT[PhaseResultT[Id, ?], FileSystemError, ?], PhaseResults] =
    EitherT.monadListen[WriterT[Id, Vector[PhaseResult], ?], PhaseResults, FileSystemError](
      WriterT.writerTMonadListen[Id, Vector[PhaseResult]])

  def compileSqlToLP[M[_]: Monad: MonadFsErr: PhaseResultTell](sql: Fix[Sql]): M[Fix[LP]] = {
    val (log, s) = queryPlan(sql, Variables.empty, basePath, 0L, None).run.run
    val lp = s.fold(
      e => scala.sys.error(e.shows),
      d => d.fold(e => scala.sys.error(e.shows), ι)
    )
    for {
      _ <- scala.Predef.implicitly[PhaseResultTell[M]].tell(log)
    } yield lp
  }

  def queryPlanner[M[_]: Monad: MonadFsErr: PhaseResultTell]
    (sql: Fix[Sql], model: MongoQueryModel, stats: Collection => Option[CollectionStatistics],
    indexes: Collection => Option[Set[Index]], lc: DiscoverPath.ListContents[M],
    execTime: Instant)
      : M[Crystallized[WorkflowF]] = {
    for {
      lp <- compileSqlToLP[M](sql)
      qs <- MongoDb.lpToQScript(lp, lc)
      repr <- MongoDb.doPlan(qs, fs.QueryContext(stats, indexes, lc), model, execTime)
    } yield repr
  }

  val defaultStats: Collection => Option[CollectionStatistics] =
    κ(CollectionStatistics(10, 100, false).some)
  val defaultIndexes: Collection => Option[Set[Index]] =
    κ(Set(Index("_id_", NonEmptyList(BsonField.Name("_id") -> IndexType.Ascending), false)).some)

  def indexes(ps: (Collection, BsonField)*): Collection => Option[Set[Index]] =  {
    val map: Map[Collection, Set[Index]] = ps.toList.foldMap { case (c, f) => Map(c -> Set(Index(f.asText + "_", NonEmptyList(f -> IndexType.Ascending), false))) }
    c => map.get(c).orElse(defaultIndexes(c))
  }
  def plan0(query: Fix[Sql], model: MongoQueryModel,
    stats: Collection => Option[CollectionStatistics],
    indexes: Collection => Option[Set[Index]]
  ): Either[FileSystemError, Crystallized[WorkflowF]] =
    queryPlanner(query, model, stats, indexes, listContents, Instant.now).run.value.toEither

  def plan2_6(query: Fix[Sql]): Either[FileSystemError, Crystallized[WorkflowF]] =
    plan0(query, MongoQueryModel.`2.6`, κ(None), κ(None))

  def plan3_0(query: Fix[Sql]): Either[FileSystemError, Crystallized[WorkflowF]] =
    plan0(query, MongoQueryModel.`3.0`, κ(None), κ(None))

  def plan3_2(query: Fix[Sql]): Either[FileSystemError, Crystallized[WorkflowF]] =
    plan0(query, MongoQueryModel.`3.2`, defaultStats, defaultIndexes)

  def plan3_4(query: Fix[Sql],
    stats: Collection => Option[CollectionStatistics],
    indexes: Collection => Option[Set[Index]]
  ): Either[FileSystemError, Crystallized[WorkflowF]] =
    plan0(query, MongoQueryModel.`3.4`, stats, indexes)

  def plan(query: Fix[Sql]): Either[FileSystemError, Crystallized[WorkflowF]] =
    plan0(query, MongoQueryModel.`3.4`, defaultStats, defaultIndexes)

  def planAt(time: Instant, query: Fix[Sql]): Either[FileSystemError, Crystallized[WorkflowF]] =
    queryPlanner(query, MongoQueryModel.`3.4`, defaultStats, defaultIndexes, listContents, time).run.value.toEither

  def planLP(logical: Fix[LP]): Either[FileSystemError, Crystallized[WorkflowF]] = {
    (for {
     _  <- emit("Input", logical)
     simplified <- emit("Simplified", optimizer.simplify(logical))
     qs <- MongoDb.lpToQScript(simplified, listContents)
     phys <- MongoDb.doPlan(qs, fs.QueryContext(defaultStats, defaultIndexes, listContents), MongoQueryModel.`3.2`, Instant.now)
    } yield phys).run.value.toEither
  }

  def planLog(query: Fix[Sql]): Vector[PhaseResult] =
    queryPlanner(query, MongoQueryModel.`3.2`, defaultStats, defaultIndexes, listContents, Instant.now).run.written

  def beWorkflow(wf: Workflow) = beRight(equalToWorkflow(wf, addDetails = false))
  def beWorkflow0(wf: Workflow) = beRight(equalToWorkflow(wf, addDetails = true))

  implicit def toBsonField(name: String) = BsonField.Name(name)
  implicit def toLeftShape(shape: Reshape[ExprOp]): Reshape.Shape[ExprOp] = -\/ (shape)
  implicit def toRightShape(exprOp: Fix[ExprOp]):   Reshape.Shape[ExprOp] =  \/-(exprOp)

  def isNumeric(field: BsonField): Selector =
    Selector.Or(
      Selector.Doc(field -> Selector.Type(BsonType.Int32)),
      Selector.Doc(field -> Selector.Type(BsonType.Int64)),
      Selector.Doc(field -> Selector.Type(BsonType.Dec)),
      Selector.Doc(field -> Selector.Type(BsonType.Text)),
      Selector.Or(
        Selector.Doc(field -> Selector.Type(BsonType.Date)),
        Selector.Doc(field -> Selector.Type(BsonType.Bool))))

  def divide(a1: Fix[ExprOp], a2: Fix[ExprOp]) =
    $cond($eq(a2, $literal(Bson.Int32(0))),
      $cond($eq(a1, $literal(Bson.Int32(0))),
        $literal(Bson.Dec(Double.NaN)),
        $cond($gt(a1, $literal(Bson.Int32(0))),
          $literal(Bson.Dec(Double.PositiveInfinity)),
          $literal(Bson.Dec(Double.NegativeInfinity)))),
      $divide(a1, a2))

  "plan from query string" should {
    "plan simple select *" in {
      plan(sqlE"select * from foo") must beWorkflow(
        $read[WorkflowF](collection("db", "foo")))
    }

    "plan count(*)" in {
      plan(sqlE"select count(*) from foo") must beWorkflow(
        chain[Workflow](
          $read(collection("db", "foo")),
          $group(
            grouped("f0" -> $sum($literal(Bson.Int32(1)))),
            \/-($literal(Bson.Null))),
          $project(
            reshape("value" -> $field("f0")),
            ExcludeId)))
    }

    "plan simple field projection on single set" in {
      plan(sqlE"select foo.bar from foo") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "foo")),
          $project(
            reshape("value" -> $field("bar")),
            ExcludeId)))
    }

    "plan simple field projection on single set when table name is inferred" in {
      plan(sqlE"select bar from foo") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $project(
           reshape("value" -> $field("bar")),
           ExcludeId)))
    }

    "plan multiple field projection on single set when table name is inferred" in {
      plan(sqlE"select bar, baz from foo") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $project(
           reshape(
             "bar" -> $field("bar"),
             "baz" -> $field("baz")),
           IgnoreId)))
    }

    "plan simple addition on two fields" in {
      plan(sqlE"select foo + bar from baz") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "baz")),
         $project(
           reshape("value" ->
             $cond(
               $and(
                 $lt($literal(Bson.Null), $field("bar")),
                 $lt($field("bar"), $literal(Bson.Text("")))),
               $cond(
                 $or(
                   $and(
                     $lt($literal(Bson.Null), $field("foo")),
                     $lt($field("foo"), $literal(Bson.Text("")))),
                   $and(
                     $lte($literal(Check.minDate), $field("foo")),
                     $lt($field("foo"), $literal(Bson.Regex("", ""))))),
                 $add($field("foo"), $field("bar")),
                 $literal(Bson.Undefined)),
               $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan concat (3.0-)" in {
      plan3_0(sqlE"select concat(bar, baz) from foo") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $project(
           reshape("value" ->
             $cond(
               $and(
                 $lte($literal(Bson.Text("")), $field("baz")),
                 $lt($field("baz"), $literal(Bson.Doc()))),
               $cond(
                 $and(
                   $lte($literal(Bson.Text("")), $field("bar")),
                   $lt($field("bar"), $literal(Bson.Doc()))),
                 $concat($field("bar"), $field("baz")),
                 $literal(Bson.Undefined)),
               $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan concat (3.2+)" in {
      plan3_2(sqlE"select concat(bar, baz) from foo") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $project(
           reshape("value" ->
             $cond(
               $and(
                 $lte($literal(Bson.Text("")), $field("baz")),
                 $lt($field("baz"), $literal(Bson.Doc()))),
               $cond(
                 $and(
                   $lte($literal(Bson.Text("")), $field("bar")),
                   $lt($field("bar"), $literal(Bson.Doc()))),
                 $let(ListMap(DocVar.Name("a1") -> $field("bar"), DocVar.Name("a2") -> $field("baz")),
                   $cond($and($isArray($field("$a1")), $isArray($field("$a2"))),
                     $concatArrays(List($field("$a1"), $field("$a2"))),
                     $concat($field("$a1"), $field("$a2")))),
                 $literal(Bson.Undefined)),
               $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan concat strings with ||" in {
      plan(sqlE"""select city || ", " || state from zips""") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "zips")),
         $project(
           reshape(
             "value" ->
               $cond(
                 $or(
                   $and(
                     $lte($literal(Bson.Arr()), $field("state")),
                     $lt($field("state"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                   $and(
                     $lte($literal(Bson.Text("")), $field("state")),
                     $lt($field("state"), $literal(Bson.Doc())))),
                 $cond(
                   $or(
                     $and(
                       $lte($literal(Bson.Arr()), $field("city")),
                       $lt($field("city"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                     $and(
                       $lte($literal(Bson.Text("")), $field("city")),
                       $lt($field("city"), $literal(Bson.Doc())))),
                   $let( // TODO: ideally, this would be a single $concat
                     ListMap(
                       DocVar.Name("a1") ->
                         $let(
                           ListMap(
                             DocVar.Name("a1") -> $field("city"),
                             DocVar.Name("a2") -> $literal(Bson.Text(", "))),
                           $cond($and($isArray($field("$a1")), $isArray($field("$a2"))),
                             $concatArrays(List($field("$a1"), $field("$a2"))),
                             $concat($field("$a1"), $field("$a2")))),
                       DocVar.Name("a2") -> $field("state")),
                     $cond($and($isArray($field("$a1")), $isArray($field("$a2"))),
                       $concatArrays(List($field("$a1"), $field("$a2"))),
                       $concat($field("$a1"), $field("$a2")))),
                   $literal(Bson.Undefined)),
                 $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan concat strings with ||, constant on the right" in {
      plan(sqlE"""select a || b || "..." from foo""") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $project(
           reshape(
             "0" ->
               $cond(
                 $or(
                   $and(
                     $lte($literal(Bson.Arr()), $field("b")),
                     $lt($field("b"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                   $and(
                     $lte($literal(Bson.Text("")), $field("b")),
                     $lt($field("b"), $literal(Bson.Doc())))),
                 $cond(
                   $or(
                     $and(
                       $lte($literal(Bson.Arr()), $field("a")),
                       $lt($field("a"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                     $and(
                       $lte($literal(Bson.Text("")), $field("a")),
                       $lt($field("a"), $literal(Bson.Doc())))),
                   $concat( // TODO: ideally, this would be a single $concat
                     $concat($field("a"), $field("b")),
                     $literal(Bson.Text("..."))),
                   $literal(Bson.Undefined)),
                 $literal(Bson.Undefined))),
           IgnoreId)))
    }.pendingUntilFixed("SD-639")

    "plan concat with unknown types" in {
      plan(sqlE"select a || b from foo") must
        beRight
    }.pendingUntilFixed("SD-639")

    "plan lower" in {
      plan(sqlE"select lower(bar) from foo") must
      beWorkflow(chain[Workflow](
        $read(collection("db", "foo")),
        $project(
          reshape("value" ->
            $cond(
              $and(
                $lte($literal(Bson.Text("")), $field("bar")),
                $lt($field("bar"), $literal(Bson.Doc()))),
              $toLower($field("bar")),
              $literal(Bson.Undefined))),
          ExcludeId)))
    }

    "plan coalesce" in {
      plan(sqlE"select coalesce(bar, baz) from foo") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $project(
           reshape("value" ->
             $cond(
               $eq($field("bar"), $literal(Bson.Null)),
               $field("baz"),
               $field("bar"))),
           ExcludeId)))
    }

    "plan now() with a literal timestamp" in {
      val time = Instant.parse("2016-08-25T00:00:00.000Z")
      val bsTime = Bson.Date.fromInstant(time).get

      planAt(time, sqlE"""select NOW(), bar from foo""") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $project(
           reshape("0" -> $literal(bsTime), "bar" -> $field("bar")))))
    }

    "plan date field extraction" in {
      plan(sqlE"""select date_part("day", baz) from foo""") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $project(
           reshape("value" ->
             $cond(
               $and(
                 $lte($literal(Check.minDate), $field("baz")),
                 $lt($field("baz"), $literal(Bson.Regex("", "")))),
               $dayOfMonth($field("baz")),
               $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan complex date field extraction" in {
      plan(sqlE"""select date_part("quarter", baz) from foo""") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $project(
           reshape(
             "value" ->
               $cond(
                 $and(
                   $lte($literal(Check.minDate), $field("baz")),
                   $lt($field("baz"), $literal(Bson.Regex("", "")))),
                 $trunc(
                   $add(
                     $divide(
                       $subtract($month($field("baz")), $literal(Bson.Int32(1))),
                       $literal(Bson.Int32(3))),
                     $literal(Bson.Int32(1)))),
                 $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan date field extraction: \"dow\"" in {
      plan(sqlE"""select date_part("dow", baz) from foo""") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $project(
           reshape(
             "value" ->
               $cond(
                 $and(
                   $lte($literal(Check.minDate), $field("baz")),
                   $lt($field("baz"), $literal(Bson.Regex("", "")))),
                 $subtract($dayOfWeek($field("baz")), $literal(Bson.Int32(1))),
                 $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan date field extraction: \"isodow\"" in {
      plan(sqlE"""select date_part("isodow", baz) from foo""") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $project(
           reshape(
             "value" ->
               $cond(
                 $and(
                   $lte($literal(Check.minDate), $field("baz")),
                   $lt($field("baz"), $literal(Bson.Regex("", "")))),
                 $cond($eq($dayOfWeek($field("baz")), $literal(Bson.Int32(1))),
                   $literal(Bson.Int32(7)),
                   $subtract($dayOfWeek($field("baz")), $literal(Bson.Int32(1)))),
                 $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan filter by date field (SD-1508)" in {
      plan(sqlE"""select * from foo where date_part("year", ts) = 2016""") must
       beWorkflow0(chain[Workflow](
         $read(collection("db", "foo")),
         $project(
           reshape(
             "__tmp2" ->
               $cond(
                 $and(
                   $lte($literal(Check.minDate), $field("ts")),
                   $lt($field("ts"), $literal(Bson.Regex("", "")))),
                 $eq($year($field("ts")), $literal(Bson.Int32(2016))),
                 $literal(Bson.Undefined)),
             "__tmp3" -> $$ROOT),
           IgnoreId),
         $match(Selector.Doc(
           BsonField.Name("__tmp2") -> Selector.Eq(Bson.Bool(true)))),
         $project(
           reshape(
             "value" -> $field("__tmp3")),
           ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; foo)
        |├─ $ProjectF
        |│  ├─ Name("0" -> "$ts")
        |│  ├─ Name("1" -> { "$year": "$ts" })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $MatchF
        |│  ╰─ And
        |│     ├─ Doc
        |│     │  ╰─ Expr($0 -> Type(Date))
        |│     ╰─ Doc
        |│        ╰─ Expr($1 -> Eq(Int32(2016)))
        |╰─ $ProjectF
        |   ├─ Name("value" -> "$src")
        |   ╰─ ExcludeId""".stripMargin)

    "plan filter array element" in {
      plan(sqlE"select loc from zips where loc[0] < -73") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $match(Selector.Where(
          If(
            BinOp(And,
              Call(Select(ident("Array"), "isArray"), List(Select(ident("this"), "loc"))),
              BinOp(Or,
                BinOp(Or,
                  BinOp(jscore.Or,
                    Call(ident("isNumber"), List(Access(Select(ident("this"), "loc"), Literal(Js.Num(0, false))))),
                    BinOp(jscore.Or,
                      BinOp(Instance, Access(Select(ident("this"), "loc"), Literal(Js.Num(0, false))), ident("NumberInt")),
                      BinOp(Instance, Access(Select(ident("this"), "loc"), Literal(Js.Num(0, false))), ident("NumberLong")))),
                  Call(ident("isString"), List(Access(Select(ident("this"), "loc"), Literal(Js.Num(0, false)))))),
                BinOp(Or,
                  BinOp(Instance, Access(Select(ident("this"), "loc"), Literal(Js.Num(0, false))), ident("Date")),
                  BinOp(Eq, UnOp(TypeOf, Access(Select(ident("this"), "loc"), Literal(Js.Num(0, false)))), jscore.Literal(Js.Str("boolean")))))),
            BinOp(Lt, Access(Select(ident("this"), "loc"), Literal(Js.Num(0, false))), Literal(Js.Num(-73, false))),
            ident("undefined")).toJs)),
        $project(
          reshape("value" -> $field("loc")),
          ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $ProjectF
        |│  ├─ Name("0" -> {
        |│  │       "$cond": [
        |│  │         {
        |│  │           "$and": [
        |│  │             { "$lte": [{ "$literal": [] }, "$loc"] },
        |│  │             { "$lt": ["$loc", { "$literal": BinData(0, "") }] }]
        |│  │         },
        |│  │         {
        |│  │           "$cond": [
        |│  │             {
        |│  │               "$or": [
        |│  │                 {
        |│  │                   "$and": [
        |│  │                     {
        |│  │                       "$lt": [
        |│  │                         { "$literal": null },
        |│  │                         {
        |│  │                           "$arrayElemAt": ["$loc", { "$literal": NumberInt("0") }]
        |│  │                         }]
        |│  │                     },
        |│  │                     {
        |│  │                       "$lt": [
        |│  │                         {
        |│  │                           "$arrayElemAt": ["$loc", { "$literal": NumberInt("0") }]
        |│  │                         },
        |│  │                         { "$literal": {  } }]
        |│  │                     }]
        |│  │                 },
        |│  │                 {
        |│  │                   "$and": [
        |│  │                     {
        |│  │                       "$lte": [
        |│  │                         { "$literal": false },
        |│  │                         {
        |│  │                           "$arrayElemAt": ["$loc", { "$literal": NumberInt("0") }]
        |│  │                         }]
        |│  │                     },
        |│  │                     {
        |│  │                       "$lt": [
        |│  │                         {
        |│  │                           "$arrayElemAt": ["$loc", { "$literal": NumberInt("0") }]
        |│  │                         },
        |│  │                         { "$literal": new RegExp("", "") }]
        |│  │                     }]
        |│  │                 }]
        |│  │             },
        |│  │             {
        |│  │               "$lt": [
        |│  │                 { "$arrayElemAt": ["$loc", { "$literal": NumberInt("0") }] },
        |│  │                 { "$literal": NumberInt("-73") }]
        |│  │             },
        |│  │             { "$literal": undefined }]
        |│  │         },
        |│  │         { "$literal": undefined }]
        |│  │     })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ Expr($0 -> Eq(Bool(true)))
        |╰─ $ProjectF
        |   ├─ Name("value" -> "$src.loc")
        |   ╰─ ExcludeId""".stripMargin
    )

    "plan select array element (3.0-)" in {
      plan3_0(sqlE"select loc[0] from zips") must
      beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
          jscore.If(Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), "loc"))),
            Access(Select(ident("x"), "loc"), jscore.Literal(Js.Num(0, false))),
            ident("undefined"))))),
          ListMap())))
    }

    "plan select array element (3.2+)" in {
      plan3_2(sqlE"select loc[0] from zips") must
      beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          reshape(
            "value" ->
              $cond(
                $and(
                  $lte($literal(Bson.Arr(List())), $field("loc")),
                  $lt($field("loc"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                $arrayElemAt($field("loc"), $literal(Bson.Int32(0))),
                $literal(Bson.Undefined))),
          ExcludeId)))
    }

    "plan array length" in {
      plan(sqlE"select array_length(bar, 1) from zips") must
      beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          reshape("value" ->
            $cond(
              $and($lte($literal(Bson.Arr()), $field("bar")),
                $lt($field("bar"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
              $size($field("bar")),
              $literal(Bson.Undefined))),
          ExcludeId)))
    }

    "plan array length 3.2" in {
      plan3_2(sqlE"select array_length(bar, 1) from zips") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "zips")),
         $simpleMap(
           NonEmptyList(MapExpr(JsFn(Name("x"),
             If(
               Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), "bar"))),
               Call(ident("NumberLong"), List(Select(Select(ident("x"), "bar"), "length"))),
               ident(Js.Undefined.ident))))),
             ListMap())))
    }

    "plan sum in expression" in {
      plan(sqlE"select sum(pop) * 100 from zips") must
      beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $group(
          grouped("f0" ->
            $sum(
              $cond(
                $and(
                  $lt($literal(Bson.Null), $field("pop")),
                  $lt($field("pop"), $literal(Bson.Text("")))),
                $field("pop"),
                $literal(Bson.Undefined)))),
          \/-($literal(Bson.Null))),
        $project(
          reshape("value" -> $multiply($field("f0"), $literal(Bson.Int32(100)))),
          ExcludeId)))
    }

    "plan conditional" in {
      plan(sqlE"select case when pop < 10000 then city else loc end from zips") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "zips")),
         $project(
           reshape(
             "value" ->
               $cond(
                 $or(
                   $and(
                     $lt($literal(Bson.Null), $field("pop")),
                     $lt($field("pop"), $literal(Bson.Doc()))),
                   $and(
                     $lte($literal(Bson.Bool(false)), $field("pop")),
                     $lt($field("pop"), $literal(Bson.Regex("", ""))))),
                 $cond($lt($field("pop"), $literal(Bson.Int32(10000))),
                   $field("city"),
                   $field("loc")),
                 $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan negate" in {
      plan(sqlE"select -bar from foo") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $project(
           reshape("value" ->
             $cond(
               $and(
                 $lt($literal(Bson.Null), $field("bar")),
                 $lt($field("bar"), $literal(Bson.Text("")))),
               $multiply($literal(Bson.Int32(-1)), $field("bar")),
               $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan simple filter" in {
      plan(sqlE"select * from foo where bar > 10") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $match(Selector.And(
           isNumeric(BsonField.Name("bar")),
           Selector.Doc(
             BsonField.Name("bar") -> Selector.Gt(Bson.Int32(10)))))))
    }

    "plan simple reversed filter" in {
      plan(sqlE"select * from foo where 10 < bar") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $match(Selector.And(
           isNumeric(BsonField.Name("bar")),
           Selector.Doc(
             BsonField.Name("bar") -> Selector.Gt(Bson.Int32(10)))))))
    }

    "plan simple filter with expression in projection" in {
      plan(sqlE"select a + b from foo where bar > 10") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $match(Selector.And(
           isNumeric(BsonField.Name("bar")),
           Selector.Doc(
             BsonField.Name("bar") -> Selector.Gt(Bson.Int32(10))))),
         $project(
           reshape("value" ->
             $cond(
               $and(
                 $lt($literal(Bson.Null), $field("b")),
                 $lt($field("b"), $literal(Bson.Text("")))),
               $cond(
                 $or(
                   $and(
                     $lt($literal(Bson.Null), $field("a")),
                     $lt($field("a"), $literal(Bson.Text("")))),
                   $and(
                     $lte($literal(Check.minDate), $field("a")),
                     $lt($field("a"), $literal(Bson.Regex("", ""))))),
                 $add($field("a"), $field("b")),
                 $literal(Bson.Undefined)),
               $literal(Bson.Undefined))),
           ExcludeId)))
    }

    "plan simple js filter 3.2" in {
      val mjs = javascript[JsCore](_.embed)
      import mjs._

      plan3_2(sqlE"select * from zips where length(city) < 4") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        // FIXME: Inline this $simpleMap with the $match (SD-456)
        $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
          "0" -> If(
            isString(Select(ident("x"), "city")),
            BinOp(Lt,
              Call(ident("NumberLong"),
                List(Select(Select(ident("x"), "city"), "length"))),
                Literal(Js.Num(4, false))),
            ident(Js.Undefined.ident)),
          "src" -> ident("x"))))),
          ListMap()),
        $match(Selector.Doc(BsonField.Name("0") -> Selector.Eq(Bson.Bool(true)))),
        $project(
          reshape("value" -> $field("src")),
          ExcludeId)))
    }.pendingWithActual("#2541",
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(0: _.city)
        |│  │     ├─ Key(1: NumberLong(_.city.length))
        |│  │     ╰─ Key(src: _)
        |│  ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ And
        |│     ├─ Doc
        |│     │  ╰─ Expr($0 -> Type(Text))
        |│     ╰─ Doc
        |│        ╰─ Expr($1 -> Lt(Int32(4)))
        |╰─ $ProjectF
        |   ├─ Name("value" -> "$src")
        |   ╰─ ExcludeId""".stripMargin)

    "plan filter with js and non-js 3.2" in {
      val mjs = javascript[JsCore](_.embed)
      import mjs._

      plan3_2(sqlE"select * from zips where length(city) < 4 and pop < 20000") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        // FIXME: Inline this $simpleMap with the $match (SD-456)
        $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
          "0" ->
            If(
              BinOp(And,
                binop(Or,
                  BinOp(Or,
                    isAnyNumber(Select(ident("x"), "pop")),
                    isString(Select(ident("x"), "pop"))),
                  isDate(Select(ident("x"), "pop")),
                  isBoolean(Select(ident("x"), "pop"))),
                Call(ident("isString"), List(Select(ident("x"), "city")))),
              BinOp(And,
                BinOp(Lt,
                  Call(ident("NumberLong"),
                    List(Select(Select(ident("x"), "city"), "length"))),
                  Literal(Js.Num(4, false))),
                BinOp(Lt,
                  Select(ident("x"), "pop"),
                  Literal(Js.Num(20000, false)))),
            ident(Js.Undefined.ident)),
          "src" -> ident("x"))))),
          ListMap()),
        $match(
          Selector.Doc(
            BsonField.Name("0") -> Selector.Eq(Bson.Bool(true)))),
        $project(
          reshape("value" -> $field("src")),
          ExcludeId)))
    }.pendingWithActual("#2541",
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(0: _.pop)
        |│  │     ├─ Key(1: _.city)
        |│  │     ├─ Key(2: NumberLong(_.city.length))
        |│  │     ├─ Key(3: _.pop)
        |│  │     ╰─ Key(src: _)
        |│  ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ And
        |│     ├─ Or
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($0 -> Type(Int32))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($0 -> Type(Int64))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($0 -> Type(Dec))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($0 -> Type(Text))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($0 -> Type(Date))
        |│     │  ╰─ Doc
        |│     │     ╰─ Expr($0 -> Type(Bool))
        |│     ├─ Doc
        |│     │  ╰─ Expr($1 -> Type(Text))
        |│     ├─ Doc
        |│     │  ╰─ Expr($2 -> Lt(Int32(4)))
        |│     ╰─ Doc
        |│        ╰─ Expr($3 -> Lt(Int32(20000)))
        |╰─ $ProjectF
        |   ├─ Name("value" -> "$src")
        |   ╰─ ExcludeId""".stripMargin)

    "plan filter with between" in {
      plan(sqlE"select * from foo where bar between 10 and 100") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $match(
           Selector.And(
             isNumeric(BsonField.Name("bar")),
             Selector.And(
               Selector.Doc(
                 BsonField.Name("bar") -> Selector.Gte(Bson.Int32(10))),
               Selector.Doc(
                 BsonField.Name("bar") -> Selector.Lte(Bson.Int32(100))))))))
    }

    "plan filter with like" in {
      plan(sqlE"""select * from foo where bar like "A.%" """) must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $match(Selector.And(
           Selector.Doc(BsonField.Name("bar") ->
             Selector.Type(BsonType.Text)),
           Selector.Doc(
             BsonField.Name("bar") ->
               Selector.Regex("^A\\..*$", false, true, false, false))))))
    }

    "plan filter with LIKE and OR" in {
      plan(sqlE"""select * from foo where bar like "A%" or bar like "Z%" """) must
       beWorkflow0(chain[Workflow](
         $read(collection("db", "foo")),
         $match(
           Selector.Or(
             Selector.And(
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Type(BsonType.Text)),
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Regex("^A.*$", false, true, false, false))),
             Selector.And(
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Type(BsonType.Text)),
               Selector.Doc(BsonField.Name("bar") ->
                 Selector.Regex("^Z.*$", false, true, false, false)))))))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; foo)
        |╰─ $MatchF
        |   ╰─ And
        |      ├─ Doc
        |      │  ╰─ Expr($bar -> Type(Text))
        |      ╰─ Or
        |         ├─ Doc
        |         │  ╰─ Expr($bar -> Regex(^A.*$,false,true,false,false))
        |         ╰─ Doc
        |            ╰─ Expr($bar -> Regex(^Z.*$,false,true,false,false))""".stripMargin)

    "plan filter with field in constant set" in {
      plan(sqlE"""select * from zips where state in ("AZ", "CO")""") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.Doc(BsonField.Name("state") ->
            Selector.In(Bson.Arr(List(Bson.Text("AZ"), Bson.Text("CO"))))))))
    }

    "plan filter with field containing constant value" in {
      plan(sqlE"select * from zips where 43.058514 in loc[_]") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.Where(
            If(Call(Select(ident("Array"), "isArray"), List(Select(ident("this"), "loc"))),
              BinOp(Neq, jscore.Literal(Js.Num(-1, false)), Call(Select(Select(ident("this"), "loc"), "indexOf"), List(jscore.Literal(Js.Num(43.058514, true))))),
            ident("undefined")).toJs))))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(0: Array.isArray(_.loc) ? -1 !== _.loc.indexOf(43.058514) : undefined)
        |│  │     ╰─ Key(src: _)
        |│  ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ Expr($0 -> Eq(Bool(true)))
        |╰─ $ProjectF
        |   ├─ Name("value" -> "$src")
        |   ╰─ ExcludeId""".stripMargin)

    "filter field in single-element set" in {
      plan(sqlE"""select * from zips where state in ("NV")""") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.Doc(BsonField.Name("state") ->
            Selector.Eq(Bson.Text("NV"))))))
    }

    "filter field “in” a bare value" in {
      plan(sqlE"""select * from zips where state in "PA"""") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.Doc(BsonField.Name("state") ->
            Selector.Eq(Bson.Text("PA"))))))
    }

    "plan filter with field containing other field" in {
      import jscore._
      plan(sqlE"select * from zips where pop in loc[_]") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.Where(
            If(
              Call(Select(ident("Array"), "isArray"), List(Select(ident("this"), "loc"))),
              BinOp(Neq,
                jscore.Literal(Js.Num(-1.0,false)),
                Call(Select(Select(ident("this"), "loc"), "indexOf"),
                  List(Select(ident("this"), "pop")))),
              ident("undefined")).toJs))))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(0: Array.isArray(_.loc) ? -1 !== _.loc.indexOf(_.pop) : undefined)
        |│  │     ╰─ Key(src: _)
        |│  ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ Expr($0 -> Eq(Bool(true)))
        |╰─ $ProjectF
        |   ├─ Name("value" -> "$src")
        |   ╰─ ExcludeId""".stripMargin)

    "plan filter with ~" in {
      plan(sqlE"""select * from zips where city ~ "^B[AEIOU]+LD.*" """) must beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $match(Selector.And(
          Selector.Doc(
            BsonField.Name("city") -> Selector.Type(BsonType.Text)),
          Selector.Doc(
            BsonField.Name("city") -> Selector.Regex("^B[AEIOU]+LD.*", false, true, false, false))))))
    }

    "plan filter with ~*" in {
      plan(sqlE"""select * from zips where city ~* "^B[AEIOU]+LD.*" """) must beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $match(Selector.And(
          Selector.Doc(
            BsonField.Name("city") -> Selector.Type(BsonType.Text)),
          Selector.Doc(
            BsonField.Name("city") -> Selector.Regex("^B[AEIOU]+LD.*", true, true, false, false))))))
    }

    "plan filter with !~" in {
      plan(sqlE"""select * from zips where city !~ "^B[AEIOU]+LD.*" """) must beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $match(Selector.And(
          Selector.Doc(
            BsonField.Name("city") -> Selector.Type(BsonType.Text)),
          Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
            BsonField.Name("city") -> Selector.NotExpr(Selector.Regex("^B[AEIOU]+LD.*", false, true, false, false))))))))
    }

    "plan filter with !~*" in {
      plan(sqlE"""select * from zips where city !~* "^B[AEIOU]+LD.*" """) must beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $match(Selector.And(
          Selector.Doc(
            BsonField.Name("city") -> Selector.Type(BsonType.Text)),
          Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
            BsonField.Name("city") -> Selector.NotExpr(Selector.Regex("^B[AEIOU]+LD.*", true, true, false, false))))))))
    }

    "plan filter with alternative ~" in {
      plan(sqlE"""select * from a where "foo" ~ pattern or target ~ pattern""") must beWorkflow0(chain[Workflow](
        $read(collection("db", "a")),
        $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
          "__tmp8" -> Call(
            Select(New(Name("RegExp"), List(Select(ident("x"), "pattern"), jscore.Literal(Js.Str("m")))), "test"),
            List(jscore.Literal(Js.Str("foo")))),
          "__tmp9" -> ident("x"),
          "__tmp10" -> Select(ident("x"), "pattern"),
          "__tmp11" -> Select(ident("x"), "target"),
          "__tmp12" -> Call(
            Select(New(Name("RegExp"), List(Select(ident("x"), "pattern"), jscore.Literal(Js.Str("m")))), "test"),
            List(Select(ident("x"), "target"))))))),
          ListMap()),
        $match(
          Selector.Or(
            Selector.And(
              Selector.Doc(
                BsonField.Name("__tmp9") \ BsonField.Name("pattern") ->
                  Selector.Type(BsonType.Text)),
              Selector.Doc(
                BsonField.Name("__tmp8") -> Selector.Eq(Bson.Bool(true)))),
            Selector.And(
              Selector.Doc(
                BsonField.Name("__tmp10") -> Selector.Type(BsonType.Text)),
              Selector.And(
                Selector.Doc(
                  BsonField.Name("__tmp11") -> Selector.Type(BsonType.Text)),
                Selector.Doc(
                  BsonField.Name("__tmp12") -> Selector.Eq(Bson.Bool(true))))))),
        $project(
          reshape("value" -> $field("__tmp9")),
          ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; a)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(0: _.pattern)
        |│  │     ├─ Key(1: _.target)
        |│  │     ├─ Key(2: (new RegExp(_.pattern, "m")).test("foo"))
        |│  │     ├─ Key(3: (new RegExp(_.pattern, "m")).test(_.target))
        |│  │     ╰─ Key(src: _)
        |│  ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ And
        |│     ├─ Doc
        |│     │  ╰─ Expr($0 -> Type(Text))
        |│     ├─ Doc
        |│     │  ╰─ Expr($1 -> Type(Text))
        |│     ╰─ Or
        |│        ├─ Doc
        |│        │  ╰─ Expr($2 -> Eq(Bool(true)))
        |│        ╰─ Doc
        |│           ╰─ Expr($3 -> Eq(Bool(true)))
        |╰─ $ProjectF
        |   ├─ Name("value" -> "$src")
        |   ╰─ ExcludeId""".stripMargin)

    "plan filter with negate(s)" in {
      plan(sqlE"select * from foo where bar != -10 and baz > -1.0") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $match(
           Selector.And(
             isNumeric(BsonField.Name("baz")),
             Selector.And(
               Selector.Doc(
                 BsonField.Name("bar") -> Selector.Neq(Bson.Int32(-10))),
               Selector.Doc(
                 BsonField.Name("baz") -> Selector.Gt(Bson.Dec(-1.0))))))))
    }

    "plan complex filter" in {
      plan(sqlE"""select * from foo where bar > 10 and (baz = "quux" or foop = "zebra")""") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "foo")),
         $match(
           Selector.And(
             isNumeric(BsonField.Name("bar")),
             Selector.And(
               Selector.Doc(BsonField.Name("bar") -> Selector.Gt(Bson.Int32(10))),
               Selector.Or(
                 Selector.Doc(
                   BsonField.Name("baz") -> Selector.Eq(Bson.Text("quux"))),
                 Selector.Doc(
                   BsonField.Name("foop") -> Selector.Eq(Bson.Text("zebra")))))))))
    }

    "plan filter with not" in {
      plan(sqlE"select * from zips where not (pop > 0 and pop < 1000)") must
       beWorkflow(chain[Workflow](
         $read(collection("db", "zips")),
         $match(
           Selector.And(
             // TODO: eliminate duplication
             isNumeric(BsonField.Name("pop")),
             Selector.And(
               isNumeric(BsonField.Name("pop")),
               Selector.Or(
                 Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                   BsonField.Name("pop") -> Selector.NotExpr(Selector.Gt(Bson.Int32(0))))),
                 Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                   BsonField.Name("pop") -> Selector.NotExpr(Selector.Lt(Bson.Int32(1000)))))))))))
    }

    "plan filter with not and equality" in {
      plan(sqlE"select * from zips where not (pop = 0)") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $match(
            Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              BsonField.Name("pop") -> Selector.NotExpr(Selector.Eq(Bson.Int32(0))))))))
    }

    "plan filter with \"is not null\"" in {
      plan(sqlE"select * from zips where city is not null") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $match(
            Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
              BsonField.Name("city") -> Selector.Expr(Selector.Neq(Bson.Null)))))))
    }

    "plan filter with both index and field projections" in {
      plan(sqlE"""select count(parents[0].sha) as count from slamengine_commits where parents[0].sha = "56d1caf5d082d1a6840090986e277d36d03f1859" """) must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "slamengine_commits")),
          $match(Selector.Where(
            If(
              BinOp(jscore.And,
                Call(Select(ident("Array"), "isArray"), List(Select(ident("this"), "parents"))),
                BinOp(jscore.And,
                  Call(ident("isObject"), List(
                    Access(
                      Select(ident("this"), "parents"),
                      jscore.Literal(Js.Num(0, false))))),
                  UnOp(jscore.Not,
                    Call(Select(ident("Array"), "isArray"), List(
                      Access(
                        Select(ident("this"), "parents"),
                        jscore.Literal(Js.Num(0, false)))))))),
              BinOp(jscore.Eq,
                Select(
                  Access(
                    Select(ident("this"), "parents"),
                    jscore.Literal(Js.Num(0, false))),
                  "sha"),
                jscore.Literal(Js.Str("56d1caf5d082d1a6840090986e277d36d03f1859"))),
              ident("undefined")).toJs)),
          // NB: This map _looks_ unnecessary, but is actually simpler than the
          //     default impl that would be triggered by the $where selector
          //     above.
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"), obj()))),
            ListMap()),
          $group(
            grouped("count" -> $sum($literal(Bson.Int32(1)))),
            \/-($literal(Bson.Null)))))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; slamengine_commits)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(0: (Array.isArray(_.parents) && (isObject(_.parents[0]) && (! Array.isArray(_.parents[0])))) ? _.parents[0].sha === "56d1caf5d082d1a6840090986e277d36d03f1859" : undefined)
        |│  │     ╰─ Key(src: _)
        |│  ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ Expr($0 -> Eq(Bool(true)))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  ╰─ Scope(Map())
        |╰─ $GroupF
        |   ├─ Grouped
        |   │  ╰─ Name("count" -> { "$sum": { "$literal": NumberInt("1") } })
        |   ╰─ By({ "$literal": null })""".stripMargin)

    "plan simple having filter" in {
      plan(sqlE"select city from zips group by city having count(*) > 10") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $group(
          grouped(
            "__tmp1" -> $sum($literal(Bson.Int32(1)))),
          -\/(reshape("0" -> $field("city")))),
        $match(Selector.Doc(
          BsonField.Name("__tmp1") -> Selector.Gt(Bson.Int32(10)))),
        $project(
          reshape("value" -> $field("_id", "0")),
          ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $FoldLeftF
        |│  │  │  ├─ Chain
        |│  │  │  │  ├─ $ReadF(db; zips)
        |│  │  │  │  ├─ $SimpleMapF
        |│  │  │  │  │  ├─ Map
        |│  │  │  │  │  │  ╰─ Let(__val)
        |│  │  │  │  │  │     ├─ JsCore([_._id, _])
        |│  │  │  │  │  │     ╰─ JsCore([__val[0], __val[1]])
        |│  │  │  │  │  ╰─ Scope(Map())
        |│  │  │  │  ├─ $GroupF
        |│  │  │  │  │  ├─ Grouped
        |│  │  │  │  │  │  ╰─ Name("0" -> { "$push": "$$ROOT" })
        |│  │  │  │  │  ╰─ By({ "$literal": null })
        |│  │  │  │  ╰─ $ProjectF
        |│  │  │  │     ├─ Name("_id" -> "$_id")
        |│  │  │  │     ├─ Name("value")
        |│  │  │  │     │  ├─ Name("left" -> "$0")
        |│  │  │  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │  │  │     │  ╰─ Name("_id" -> "$_id")
        |│  │  │  │     ╰─ IncludeId
        |│  │  │  ╰─ Chain
        |│  │  │     ├─ $ReadF(db; zips)
        |│  │  │     ├─ $SimpleMapF
        |│  │  │     │  ├─ Map
        |│  │  │     │  │  ╰─ Let(__val)
        |│  │  │     │  │     ├─ JsCore([_._id, _])
        |│  │  │     │  │     ╰─ JsCore([__val[0], __val[1]])
        |│  │  │     │  ╰─ Scope(Map())
        |│  │  │     ├─ $GroupF
        |│  │  │     │  ├─ Grouped
        |│  │  │     │  │  ╰─ Name("f0" -> { "$sum": { "$literal": NumberInt("1") } })
        |│  │  │     │  ╰─ By({ "$literal": null })
        |│  │  │     ├─ $MapF
        |│  │  │     │  ├─ JavaScript(function (key, value) { return [null, { "left": [], "right": [value.f0] }] })
        |│  │  │     │  ╰─ Scope(Map())
        |│  │  │     ╰─ $ReduceF
        |│  │  │        ├─ JavaScript(function (key, values) {
        |│  │  │        │               var result = { "left": [], "right": [] };
        |│  │  │        │               values.forEach(
        |│  │  │        │                 function (value) {
        |│  │  │        │                   result.left = result.left.concat(value.left);
        |│  │  │        │                   result.right = result.right.concat(value.right)
        |│  │  │        │                 });
        |│  │  │        │               return result
        |│  │  │        │             })
        |│  │  │        ╰─ Scope(Map())
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ Doc
        |│  │  │     ├─ NotExpr($left -> Size(0))
        |│  │  │     ╰─ NotExpr($right -> Size(0))
        |│  │  ├─ $UnwindF(DocField(BsonField.Name("right")))
        |│  │  ├─ $UnwindF(DocField(BsonField.Name("left")))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ JsCore([_.left, _.right])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $ProjectF
        |│  │  │  ├─ Name("0" -> { "$literal": true })
        |│  │  │  ├─ Name("1" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│  │  │  ├─ Name("src" -> "$$ROOT")
        |│  │  │  ╰─ IgnoreId
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ And
        |│  │  │     ├─ Doc
        |│  │  │     │  ╰─ Expr($0 -> Eq(Bool(true)))
        |│  │  │     ╰─ Doc
        |│  │  │        ╰─ Expr($1 -> Gt(Int32(10)))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ JsCore([
        |│  │  │  │            _.src[0][0],
        |│  │  │  │            (isObject(_.src[0][1]) && (! Array.isArray(_.src[0][1]))) ? _.src[0][1] : undefined,
        |│  │  │  │            _.src[1] > 10])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$$ROOT" })
        |│  │  │  ╰─ By({ "$literal": null })
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; zips)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Let(__val)
        |│     │  │     ├─ JsCore([_._id, _])
        |│     │  │     ╰─ JsCore([__val[0], __val[1]])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) { return [null, { "left": [], "right": [value] }] })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ JsCore([_.left, _.right])
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> { "$literal": true })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ Expr($0 -> Eq(Bool(true)))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(f0: _.src[0][1].city)
        |│  │     ╰─ Key(b0: [
        |│  │            (isObject(_.src[1][1]) && (! Array.isArray(_.src[1][1]))) ? _.src[1][1].city : undefined])
        |│  ╰─ Scope(Map())
        |├─ $GroupF
        |│  ├─ Grouped
        |│  │  ╰─ Name("f0" -> { "$first": "$f0" })
        |│  ╰─ By
        |│     ╰─ Name("0" -> "$b0")
        |╰─ $ProjectF
        |   ├─ Name("value" -> "$f0")
        |   ╰─ ExcludeId""".stripMargin)

    "plan having with multiple projections" in {
      plan(sqlE"select city, sum(pop) from zips group by city having sum(pop) > 50000") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $group(
          grouped(
            "1" ->
              $sum(
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $field("pop"),
                  $literal(Bson.Undefined)))),
          -\/(reshape("0" -> $field("city")))),
        $match(Selector.Doc(
          BsonField.Name("1") -> Selector.Gt(Bson.Int32(50000)))),
        $project(
          reshape(
            "city" -> $field("_id", "0"),
            "1"    -> $include()),
          IgnoreId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $FoldLeftF
        |│  │  │  ├─ Chain
        |│  │  │  │  ├─ $FoldLeftF
        |│  │  │  │  │  ├─ Chain
        |│  │  │  │  │  │  ├─ $ReadF(db; zips)
        |│  │  │  │  │  │  ├─ $SimpleMapF
        |│  │  │  │  │  │  │  ├─ Map
        |│  │  │  │  │  │  │  │  ╰─ Let(__val)
        |│  │  │  │  │  │  │  │     ├─ JsCore([_._id, _])
        |│  │  │  │  │  │  │  │     ╰─ JsCore([
        |│  │  │  │  │  │  │  │               __val[0],
        |│  │  │  │  │  │  │  │               __val[1],
        |│  │  │  │  │  │  │  │               [
        |│  │  │  │  │  │  │  │                 __val[0],
        |│  │  │  │  │  │  │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1] : undefined,
        |│  │  │  │  │  │  │  │                 [
        |│  │  │  │  │  │  │  │                   (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].city : undefined],
        |│  │  │  │  │  │  │  │                 __val[1]]])
        |│  │  │  │  │  │  │  ╰─ Scope(Map())
        |│  │  │  │  │  │  ├─ $GroupF
        |│  │  │  │  │  │  │  ├─ Grouped
        |│  │  │  │  │  │  │  │  ╰─ Name("0" -> { "$push": "$$ROOT" })
        |│  │  │  │  │  │  │  ╰─ By({ "$literal": null })
        |│  │  │  │  │  │  ╰─ $ProjectF
        |│  │  │  │  │  │     ├─ Name("_id" -> "$_id")
        |│  │  │  │  │  │     ├─ Name("value")
        |│  │  │  │  │  │     │  ├─ Name("left" -> "$0")
        |│  │  │  │  │  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │  │  │  │  │     │  ╰─ Name("_id" -> "$_id")
        |│  │  │  │  │  │     ╰─ IncludeId
        |│  │  │  │  │  ╰─ Chain
        |│  │  │  │  │     ├─ $ReadF(db; zips)
        |│  │  │  │  │     ├─ $SimpleMapF
        |│  │  │  │  │     │  ├─ Map
        |│  │  │  │  │     │  │  ╰─ Let(__val)
        |│  │  │  │  │     │  │     ├─ Let(__val)
        |│  │  │  │  │     │  │     │  ├─ JsCore([_._id, _])
        |│  │  │  │  │     │  │     │  ╰─ JsCore([
        |│  │  │  │  │     │  │     │            __val[0],
        |│  │  │  │  │     │  │     │            __val[1],
        |│  │  │  │  │     │  │     │            [
        |│  │  │  │  │     │  │     │              __val[0],
        |│  │  │  │  │     │  │     │              (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1] : undefined,
        |│  │  │  │  │     │  │     │              [
        |│  │  │  │  │     │  │     │                (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].city : undefined],
        |│  │  │  │  │     │  │     │              __val[1]]])
        |│  │  │  │  │     │  │     ╰─ Obj
        |│  │  │  │  │     │  │        ╰─ Key(f0: ((isNumber(
        |│  │  │  │  │     │  │               (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined) || ((((isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined) instanceof NumberInt) || (((isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined) instanceof NumberLong))) && (isObject(__val[1]) && (! Array.isArray(__val[1])))) ? __val[1].pop : undefined)
        |│  │  │  │  │     │  ╰─ Scope(Map())
        |│  │  │  │  │     ├─ $GroupF
        |│  │  │  │  │     │  ├─ Grouped
        |│  │  │  │  │     │  │  ╰─ Name("f0" -> { "$sum": "$f0" })
        |│  │  │  │  │     │  ╰─ By({ "$literal": null })
        |│  │  │  │  │     ├─ $MapF
        |│  │  │  │  │     │  ├─ JavaScript(function (key, value) { return [null, { "left": [], "right": [value.f0] }] })
        |│  │  │  │  │     │  ╰─ Scope(Map())
        |│  │  │  │  │     ╰─ $ReduceF
        |│  │  │  │  │        ├─ JavaScript(function (key, values) {
        |│  │  │  │  │        │               var result = { "left": [], "right": [] };
        |│  │  │  │  │        │               values.forEach(
        |│  │  │  │  │        │                 function (value) {
        |│  │  │  │  │        │                   result.left = result.left.concat(value.left);
        |│  │  │  │  │        │                   result.right = result.right.concat(value.right)
        |│  │  │  │  │        │                 });
        |│  │  │  │  │        │               return result
        |│  │  │  │  │        │             })
        |│  │  │  │  │        ╰─ Scope(Map())
        |│  │  │  │  ├─ $MatchF
        |│  │  │  │  │  ╰─ Doc
        |│  │  │  │  │     ├─ NotExpr($left -> Size(0))
        |│  │  │  │  │     ╰─ NotExpr($right -> Size(0))
        |│  │  │  │  ├─ $UnwindF(DocField(BsonField.Name("right")))
        |│  │  │  │  ├─ $UnwindF(DocField(BsonField.Name("left")))
        |│  │  │  │  ├─ $SimpleMapF
        |│  │  │  │  │  ├─ Map
        |│  │  │  │  │  │  ╰─ JsCore([_.left, _.right])
        |│  │  │  │  │  ╰─ Scope(Map())
        |│  │  │  │  ├─ $ProjectF
        |│  │  │  │  │  ├─ Name("0" -> { "$literal": true })
        |│  │  │  │  │  ├─ Name("1" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│  │  │  │  │  ├─ Name("src" -> "$$ROOT")
        |│  │  │  │  │  ╰─ IgnoreId
        |│  │  │  │  ├─ $MatchF
        |│  │  │  │  │  ╰─ And
        |│  │  │  │  │     ├─ Doc
        |│  │  │  │  │     │  ╰─ Expr($0 -> Eq(Bool(true)))
        |│  │  │  │  │     ╰─ Doc
        |│  │  │  │  │        ╰─ Expr($1 -> Gt(Int32(50000)))
        |│  │  │  │  ├─ $SimpleMapF
        |│  │  │  │  │  ├─ Map
        |│  │  │  │  │  │  ╰─ JsCore([
        |│  │  │  │  │  │            _.src[0][0],
        |│  │  │  │  │  │            (isObject(_.src[0][1]) && (! Array.isArray(_.src[0][1]))) ? _.src[0][1] : undefined,
        |│  │  │  │  │  │            _.src[1] > 50000])
        |│  │  │  │  │  ╰─ Scope(Map())
        |│  │  │  │  ├─ $GroupF
        |│  │  │  │  │  ├─ Grouped
        |│  │  │  │  │  │  ╰─ Name("0" -> { "$push": "$$ROOT" })
        |│  │  │  │  │  ╰─ By({ "$literal": null })
        |│  │  │  │  ╰─ $ProjectF
        |│  │  │  │     ├─ Name("_id" -> "$_id")
        |│  │  │  │     ├─ Name("value")
        |│  │  │  │     │  ├─ Name("left" -> "$0")
        |│  │  │  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │  │  │     │  ╰─ Name("_id" -> "$_id")
        |│  │  │  │     ╰─ IncludeId
        |│  │  │  ╰─ Chain
        |│  │  │     ├─ $ReadF(db; zips)
        |│  │  │     ├─ $SimpleMapF
        |│  │  │     │  ├─ Map
        |│  │  │     │  │  ╰─ Let(__val)
        |│  │  │     │  │     ├─ JsCore([_._id, _])
        |│  │  │     │  │     ╰─ JsCore([
        |│  │  │     │  │               __val[0],
        |│  │  │     │  │               __val[1],
        |│  │  │     │  │               [
        |│  │  │     │  │                 __val[0],
        |│  │  │     │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1] : undefined,
        |│  │  │     │  │                 [
        |│  │  │     │  │                   (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].city : undefined],
        |│  │  │     │  │                 __val[1]]])
        |│  │  │     │  ╰─ Scope(Map())
        |│  │  │     ├─ $MapF
        |│  │  │     │  ├─ JavaScript(function (key, value) { return [null, { "left": [], "right": [value] }] })
        |│  │  │     │  ╰─ Scope(Map())
        |│  │  │     ╰─ $ReduceF
        |│  │  │        ├─ JavaScript(function (key, values) {
        |│  │  │        │               var result = { "left": [], "right": [] };
        |│  │  │        │               values.forEach(
        |│  │  │        │                 function (value) {
        |│  │  │        │                   result.left = result.left.concat(value.left);
        |│  │  │        │                   result.right = result.right.concat(value.right)
        |│  │  │        │                 });
        |│  │  │        │               return result
        |│  │  │        │             })
        |│  │  │        ╰─ Scope(Map())
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ Doc
        |│  │  │     ├─ NotExpr($left -> Size(0))
        |│  │  │     ╰─ NotExpr($right -> Size(0))
        |│  │  ├─ $UnwindF(DocField(BsonField.Name("right")))
        |│  │  ├─ $UnwindF(DocField(BsonField.Name("left")))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Let(__val)
        |│  │  │  │     ├─ JsCore([_.left, _.right])
        |│  │  │  │     ╰─ Obj
        |│  │  │  │        ├─ Key(f0: __val[0][1].city)
        |│  │  │  │        ╰─ Key(b0: [
        |│  │  │  │               (isObject(__val[1][1]) && (! Array.isArray(__val[1][1]))) ? __val[1][1].city : undefined])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("f0" -> { "$first": "$f0" })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> "$b0")
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$f0" })
        |│  │  │  ╰─ By({ "$literal": null })
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $FoldLeftF
        |│     │  ├─ Chain
        |│     │  │  ├─ $ReadF(db; zips)
        |│     │  │  ├─ $SimpleMapF
        |│     │  │  │  ├─ Map
        |│     │  │  │  │  ╰─ Let(__val)
        |│     │  │  │  │     ├─ JsCore([_._id, _])
        |│     │  │  │  │     ╰─ JsCore([
        |│     │  │  │  │               __val[0],
        |│     │  │  │  │               __val[1],
        |│     │  │  │  │               [
        |│     │  │  │  │                 __val[0],
        |│     │  │  │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1] : undefined,
        |│     │  │  │  │                 [
        |│     │  │  │  │                   (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].city : undefined],
        |│     │  │  │  │                 __val[1]]])
        |│     │  │  │  ╰─ Scope(Map())
        |│     │  │  ├─ $GroupF
        |│     │  │  │  ├─ Grouped
        |│     │  │  │  │  ╰─ Name("0" -> { "$push": { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("2") }] } })
        |│     │  │  │  ╰─ By({ "$literal": null })
        |│     │  │  ╰─ $ProjectF
        |│     │  │     ├─ Name("_id" -> "$_id")
        |│     │  │     ├─ Name("value")
        |│     │  │     │  ├─ Name("left" -> "$0")
        |│     │  │     │  ├─ Name("right" -> { "$literal": [] })
        |│     │  │     │  ╰─ Name("_id" -> "$_id")
        |│     │  │     ╰─ IncludeId
        |│     │  ╰─ Chain
        |│     │     ├─ $ReadF(db; zips)
        |│     │     ├─ $SimpleMapF
        |│     │     │  ├─ Map
        |│     │     │  │  ╰─ Let(__val)
        |│     │     │  │     ├─ JsCore([_._id, _])
        |│     │     │  │     ╰─ JsCore([
        |│     │     │  │               __val[0],
        |│     │     │  │               __val[1],
        |│     │     │  │               [
        |│     │     │  │                 __val[0],
        |│     │     │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1] : undefined,
        |│     │     │  │                 [
        |│     │     │  │                   (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].city : undefined],
        |│     │     │  │                 __val[1]]])
        |│     │     │  ╰─ Scope(Map())
        |│     │     ├─ $ProjectF
        |│     │     │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("2") }] })
        |│     │     │  ╰─ IgnoreId
        |│     │     ├─ $SimpleMapF
        |│     │     │  ├─ Map
        |│     │     │  │  ╰─ Obj
        |│     │     │  │     ├─ Key(f0: ((isNumber(
        |│     │     │  │     │      (isObject(_["0"][3]) && (! Array.isArray(_["0"][3]))) ? _["0"][3].pop : undefined) || ((((isObject(_["0"][3]) && (! Array.isArray(_["0"][3]))) ? _["0"][3].pop : undefined) instanceof NumberInt) || (((isObject(_["0"][3]) && (! Array.isArray(_["0"][3]))) ? _["0"][3].pop : undefined) instanceof NumberLong))) && (isObject(_["0"][3]) && (! Array.isArray(_["0"][3])))) ? _["0"][3].pop : undefined)
        |│     │     │  │     ╰─ Key(b0: [
        |│     │     │  │            (isObject(_["0"][3]) && (! Array.isArray(_["0"][3]))) ? _["0"][3].city : undefined])
        |│     │     │  ╰─ Scope(Map())
        |│     │     ├─ $GroupF
        |│     │     │  ├─ Grouped
        |│     │     │  │  ╰─ Name("f0" -> { "$sum": "$f0" })
        |│     │     │  ╰─ By
        |│     │     │     ╰─ Name("0" -> "$b0")
        |│     │     ├─ $MapF
        |│     │     │  ├─ JavaScript(function (key, value) { return [null, { "left": [], "right": [value.f0] }] })
        |│     │     │  ╰─ Scope(Map())
        |│     │     ╰─ $ReduceF
        |│     │        ├─ JavaScript(function (key, values) {
        |│     │        │               var result = { "left": [], "right": [] };
        |│     │        │               values.forEach(
        |│     │        │                 function (value) {
        |│     │        │                   result.left = result.left.concat(value.left);
        |│     │        │                   result.right = result.right.concat(value.right)
        |│     │        │                 });
        |│     │        │               return result
        |│     │        │             })
        |│     │        ╰─ Scope(Map())
        |│     ├─ $MatchF
        |│     │  ╰─ Doc
        |│     │     ├─ NotExpr($left -> Size(0))
        |│     │     ╰─ NotExpr($right -> Size(0))
        |│     ├─ $UnwindF(DocField(BsonField.Name("right")))
        |│     ├─ $UnwindF(DocField(BsonField.Name("left")))
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ JsCore([_.left, _.right])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $ProjectF
        |│     │  ├─ Name("0" -> { "$literal": true })
        |│     │  ├─ Name("1" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│     │  ├─ Name("src" -> "$$ROOT")
        |│     │  ╰─ IgnoreId
        |│     ├─ $MatchF
        |│     │  ╰─ And
        |│     │     ├─ Doc
        |│     │     │  ╰─ Expr($0 -> Eq(Bool(true)))
        |│     │     ╰─ Doc
        |│     │        ╰─ Expr($1 -> Gt(Int32(50000)))
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Obj
        |│     │  │     ├─ Key(f0: (isNumber(_.src[0][1].pop) || ((_.src[0][1].pop instanceof NumberInt) || (_.src[0][1].pop instanceof NumberLong))) ? _.src[0][1].pop : undefined)
        |│     │  │     ╰─ Key(b0: _.src[0][2])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $GroupF
        |│     │  ├─ Grouped
        |│     │  │  ╰─ Name("f0" -> { "$sum": "$f0" })
        |│     │  ╰─ By
        |│     │     ╰─ Name("0" -> "$b0")
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) { return [null, { "left": [], "right": [value.f0] }] })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ JsCore([_.left, _.right])
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> { "$literal": true })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ Expr($0 -> Eq(Bool(true)))
        |╰─ $ProjectF
        |   ├─ Name("city" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ├─ Name("1" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("1") }] })
        |   ╰─ IgnoreId""".stripMargin)

    "prefer projection+filter over JS filter" in {
      plan(sqlE"select * from zips where city <> state") must
      beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          reshape(
            "0" -> $neq($field("city"), $field("state")),
            "src" -> $$ROOT),
          IgnoreId),
        $match(
          Selector.Doc(
            BsonField.Name("0") -> Selector.Eq(Bson.Bool(true)))),
        $project(
          reshape("value" -> $field("src")),
          ExcludeId)))
    }

    "prefer projection+filter over nested JS filter" in {
      plan(sqlE"select * from zips where city <> state and pop < 10000") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          reshape(
            "__tmp4" ->
              $cond(
                $or(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Doc()))),
                  $and(
                    $lte($literal(Bson.Bool(false)), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Regex("", ""))))),
                $and(
                  $neq($field("city"), $field("state")),
                  $lt($field("pop"), $literal(Bson.Int32(10000)))),
                $literal(Bson.Undefined)),
            "__tmp5" -> $$ROOT),
          IgnoreId),
        $match(
          Selector.Doc(
            BsonField.Name("__tmp4") -> Selector.Eq(Bson.Bool(true)))),
        $project(
          reshape("value" -> $field("__tmp5")),
          ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $ProjectF
        |│  ├─ Name("0" -> "$pop")
        |│  ├─ Name("1" -> { "$ne": ["$city", "$state"] })
        |│  ├─ Name("2" -> "$pop")
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $MatchF
        |│  ╰─ And
        |│     ├─ Or
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($0 -> Type(Int32))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($0 -> Type(Int64))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($0 -> Type(Dec))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($0 -> Type(Text))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($0 -> Type(Date))
        |│     │  ╰─ Doc
        |│     │     ╰─ Expr($0 -> Type(Bool))
        |│     ├─ Doc
        |│     │  ╰─ Expr($1 -> Eq(Bool(true)))
        |│     ╰─ Doc
        |│        ╰─ Expr($2 -> Lt(Int32(10000)))
        |╰─ $ProjectF
        |   ├─ Name("value" -> "$src")
        |   ╰─ ExcludeId""".stripMargin)

    "filter on constant true" in {
      plan(sqlE"select * from zips where true") must
        beWorkflow($read(collection("db", "zips")))
    }

    "select partially-applied substring" in {
      plan3_2(sqlE"""select substring("abcdefghijklmnop", 5, pop / 10000) from zips""") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $project(
            reshape(
              "value" ->
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $substr(
                    $literal(Bson.Text("fghijklmnop")),
                    $literal(Bson.Int32(0)),
                    divide($field("pop"), $literal(Bson.Int32(10000)))),
                  $literal(Bson.Undefined))),
            ExcludeId)))
    }

    "drop nothing" in {
      plan(sqlE"select * from zips limit 5 offset 0") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $limit(5)))
    }

    "concat with empty string" in {
      plan(sqlE"""select "" || city || "" from zips""") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $project(
            reshape("value" ->
              $cond(
                $or(
                  $and(
                    $lte($literal(Bson.Arr()), $field("city")),
                    $lt($field("city"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                  $and(
                    $lte($literal(Bson.Text("")), $field("city")),
                    $lt($field("city"), $literal(Bson.Doc())))),
                $field("city"),
                $literal(Bson.Undefined))),
            ExcludeId)))
    }

    "plan simple sort with field in projection" in {
      plan(sqlE"select bar from foo order by bar") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "foo")),
          $sort(NonEmptyList(BsonField.Name("bar") -> SortDir.Ascending)),
          $project(
            reshape("value" -> $field("bar")),
            ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; foo)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ JsCore([_.bar, _])
        |│  │     ╰─ Obj
        |│  │        ├─ Key(0: (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].bar : undefined)
        |│  │        ╰─ Key(src: __val)
        |│  ╰─ Scope(Map())
        |├─ $SortF
        |│  ╰─ SortKey(0 -> Ascending)
        |╰─ $ProjectF
        |   ├─ Name("value" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ╰─ ExcludeId""".stripMargin)

    "plan simple sort with wildcard" in {
      plan(sqlE"select * from zips order by pop") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $sort(NonEmptyList(BsonField.Name("pop") -> SortDir.Ascending))))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ JsCore([_, _])
        |│  │     ╰─ Obj
        |│  │        ├─ Key(0: (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined)
        |│  │        ╰─ Key(src: __val)
        |│  ╰─ Scope(Map())
        |├─ $SortF
        |│  ╰─ SortKey(0 -> Ascending)
        |╰─ $ProjectF
        |   ├─ Name("value" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ╰─ ExcludeId""".stripMargin)

    "plan sort with expression in key" in {
      plan(sqlE"select baz from foo order by bar/10") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "foo")),
          $project(
            reshape(
              "baz"    -> $field("baz"),
              "__tmp2" ->
                $cond(
                  $or(
                    $and(
                      $lt($literal(Bson.Null), $field("bar")),
                      $lt($field("bar"), $literal(Bson.Text("")))),
                    $and(
                      $lte($literal(Check.minDate), $field("bar")),
                      $lt($field("bar"), $literal(Bson.Regex("", ""))))),
                  divide($field("bar"), $literal(Bson.Int32(10))),
                  $literal(Bson.Undefined))),
            IgnoreId),
          $sort(NonEmptyList(BsonField.Name("__tmp2") -> SortDir.Ascending)),
          $project(
            reshape("baz" -> $field("baz")),
            ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; foo)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ Arr
        |│  │     │  ├─ Obj
        |│  │     │  │  ╰─ Key(baz: _.baz)
        |│  │     │  ╰─ Ident(_)
        |│  │     ╰─ Obj
        |│  │        ├─ Key(0: (((isNumber(
        |│  │        │      (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].bar : undefined) || ((((isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].bar : undefined) instanceof NumberInt) || (((isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].bar : undefined) instanceof NumberLong))) || (((isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].bar : undefined) instanceof Date)) && (isObject(__val[1]) && (! Array.isArray(__val[1])))) ? __val[1].bar / 10 : undefined)
        |│  │        ╰─ Key(src: __val)
        |│  ╰─ Scope(Map())
        |├─ $SortF
        |│  ╰─ SortKey(0 -> Ascending)
        |╰─ $ProjectF
        |   ├─ Name("value" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ╰─ ExcludeId""".stripMargin)

    "plan select with wildcard and field" in {
      plan(sqlE"select *, pop from zips") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"),
              SpliceObjects(List(
                ident("x"),
                obj(
                  "pop" -> Select(ident("x"), "pop"))))))),
            ListMap())))
    }

    "plan select with wildcard and two fields" in {
      plan(sqlE"select *, city as city2, pop as pop2 from zips") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"),
              SpliceObjects(List(
                ident("x"),
                obj(
                  "city2" -> Select(ident("x"), "city")),
                obj(
                  "pop2"  -> Select(ident("x"), "pop"))))))),
            ListMap())))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |╰─ $SimpleMapF
        |   ├─ Map
        |   │  ╰─ SpliceObjects
        |   │     ├─ SpliceObjects
        |   │     │  ├─ Ident(_)
        |   │     │  ╰─ Obj
        |   │     │     ╰─ Key(city2: _.city)
        |   │     ╰─ Obj
        |   │        ╰─ Key(pop2: _.pop)
        |   ╰─ Scope(Map())""".stripMargin)

    "plan select with wildcard and two constants" in {
      plan(sqlE"""select *, "1", "2" from zips""") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"),
              SpliceObjects(List(
                ident("x"),
                obj(
                  "1" -> jscore.Literal(Js.Str("1"))),
                obj(
                  "2" -> jscore.Literal(Js.Str("2")))))))),
            ListMap())))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |╰─ $SimpleMapF
        |   ├─ Map
        |   │  ╰─ SpliceObjects
        |   │     ├─ SpliceObjects
        |   │     │  ├─ Ident(_)
        |   │     │  ╰─ Obj
        |   │     │     ╰─ Key(1: "1")
        |   │     ╰─ Obj
        |   │        ╰─ Key(2: "2")
        |   ╰─ Scope(Map())""".stripMargin)

    "plan select with multiple wildcards and fields" in {
      plan(sqlE"select state as state2, *, city as city2, *, pop as pop2 from zips where pop < 1000") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.And(
            isNumeric(BsonField.Name("pop")),
            Selector.Doc(
              BsonField.Name("pop") -> Selector.Lt(Bson.Int32(1000))))),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"),
              obj(
                "__tmp2" -> SpliceObjects(List(
                  obj(
                    "state2" -> Select(ident("x"), "state")),
                  ident("x"),
                  obj(
                    "city2" -> Select(ident("x"), "city")),
                  ident("x"),
                  obj(
                    "pop2" -> Select(ident("x"), "pop")))))))),
            ListMap()),
          $project(
            reshape("value" -> $field("__tmp2")),
            ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $MatchF
        |│  ╰─ And
        |│     ├─ Or
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($pop -> Type(Int32))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($pop -> Type(Int64))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($pop -> Type(Dec))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($pop -> Type(Text))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($pop -> Type(Date))
        |│     │  ╰─ Doc
        |│     │     ╰─ Expr($pop -> Type(Bool))
        |│     ╰─ Doc
        |│        ╰─ Expr($pop -> Lt(Int32(1000)))
        |╰─ $SimpleMapF
        |   ├─ Map
        |   │  ╰─ SpliceObjects
        |   │     ├─ SpliceObjects
        |   │     │  ├─ SpliceObjects
        |   │     │  │  ├─ SpliceObjects
        |   │     │  │  │  ├─ Obj
        |   │     │  │  │  │  ╰─ Key(state2: _.state)
        |   │     │  │  │  ╰─ Ident(_)
        |   │     │  │  ╰─ Obj
        |   │     │  │     ╰─ Key(city2: _.city)
        |   │     │  ╰─ Ident(_)
        |   │     ╰─ Obj
        |   │        ╰─ Key(pop2: _.pop)
        |   ╰─ Scope(Map())""".stripMargin)

    "plan sort with wildcard and expression in key" in {
      plan(sqlE"select * from zips order by pop*10 desc") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("__val"), SpliceObjects(List(
              ident("__val"),
              obj(
                "__sd__0" ->
                  jscore.If(
                    BinOp(jscore.Or,
                      Call(ident("isNumber"), List(Select(ident("__val"), "pop"))),
                      BinOp(jscore.Or,
                        BinOp(Instance, Select(ident("__val"), "pop"), ident("NumberInt")),
                        BinOp(Instance, Select(ident("__val"), "pop"), ident("NumberLong")))),
                    BinOp(Mult, Select(ident("__val"), "pop"), jscore.Literal(Js.Num(10, false))),
                    ident("undefined")))))))),
            ListMap()),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("__val"), obj(
              "__tmp2" ->
                Call(ident("remove"),
                  List(ident("__val"), jscore.Literal(Js.Str("__sd__0")))),
              "__tmp3" -> Select(ident("__val"), "__sd__0"))))),
            ListMap()),
          $sort(NonEmptyList(BsonField.Name("__tmp3") -> SortDir.Descending)),
          $project(
            reshape("value" -> $field("__tmp2")),
            ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ JsCore([remove(_, "__sd__0"), _])
        |│  │     ╰─ Obj
        |│  │        ├─ Key(0: ((isNumber(
        |│  │        │      (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined) || ((((isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined) instanceof NumberInt) || (((isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined) instanceof NumberLong))) && (isObject(__val[1]) && (! Array.isArray(__val[1])))) ? __val[1].pop * 10 : undefined)
        |│  │        ╰─ Key(src: __val)
        |│  ╰─ Scope(Map())
        |├─ $SortF
        |│  ╰─ SortKey(0 -> Descending)
        |╰─ $ProjectF
        |   ├─ Name("value" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ╰─ ExcludeId""".stripMargin)

    "plan simple sort with field not in projections" in {
      plan(sqlE"select name from person order by height") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "person")),
          $project(
            reshape(
              "name"   -> $field("name"),
              "__tmp0" -> $field("height")),
            IgnoreId),
          $sort(NonEmptyList(BsonField.Name("__tmp0") -> SortDir.Ascending)),
          $project(
            reshape("name" -> $field("name")),
            ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; person)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ Arr
        |│  │     │  ├─ Obj
        |│  │     │  │  ╰─ Key(name: _.name)
        |│  │     │  ╰─ Ident(_)
        |│  │     ╰─ Obj
        |│  │        ├─ Key(0: (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].height : undefined)
        |│  │        ╰─ Key(src: __val)
        |│  ╰─ Scope(Map())
        |├─ $SortF
        |│  ╰─ SortKey(0 -> Ascending)
        |╰─ $ProjectF
        |   ├─ Name("value" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ╰─ ExcludeId""".stripMargin)

    "plan sort with expression and alias" in {
      plan(sqlE"select pop/1000 as popInK from zips order by popInK") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $project(
            reshape(
              "popInK" ->
                $cond(
                  $or(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $and(
                      $lte($literal(Check.minDate), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Regex("", ""))))),
                  divide($field("pop"), $literal(Bson.Int32(1000))),
                  $literal(Bson.Undefined))),
            IgnoreId),
          $sort(NonEmptyList(BsonField.Name("popInK") -> SortDir.Ascending))))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ Arr
        |│  │     │  ├─ Obj
        |│  │     │  │  ╰─ Key(popInK: ((isNumber(_.pop) || ((_.pop instanceof NumberInt) || (_.pop instanceof NumberLong))) || (_.pop instanceof Date)) ? _.pop / 1000 : undefined)
        |│  │     │  ╰─ Ident(_)
        |│  │     ╰─ Obj
        |│  │        ├─ Key(0: ((isObject(__val[1]) && (! Array.isArray(__val[1]))) && ((isNumber(__val[1].pop) || ((__val[1].pop instanceof NumberInt) || (__val[1].pop instanceof NumberLong))) || (__val[1].pop instanceof Date))) ? __val[1].pop / 1000 : undefined)
        |│  │        ╰─ Key(src: __val)
        |│  ╰─ Scope(Map())
        |├─ $SortF
        |│  ╰─ SortKey(0 -> Ascending)
        |╰─ $ProjectF
        |   ├─ Name("value" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ╰─ ExcludeId""".stripMargin)

    "plan sort with filter" in {
      plan(sqlE"select city, pop from zips where pop <= 1000 order by pop desc, city") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.And(
            isNumeric(BsonField.Name("pop")),
            Selector.Doc(
              BsonField.Name("pop") -> Selector.Lte(Bson.Int32(1000))))),
          $project(
            reshape(
              "city" -> $field("city"),
              "pop"  -> $field("pop")),
            IgnoreId),
          $sort(NonEmptyList(
            BsonField.Name("pop") -> SortDir.Descending,
            BsonField.Name("city") -> SortDir.Ascending))))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $MatchF
        |│  ╰─ And
        |│     ├─ Or
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($pop -> Type(Int32))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($pop -> Type(Int64))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($pop -> Type(Dec))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($pop -> Type(Text))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($pop -> Type(Date))
        |│     │  ╰─ Doc
        |│     │     ╰─ Expr($pop -> Type(Bool))
        |│     ╰─ Doc
        |│        ╰─ Expr($pop -> Lte(Int32(1000)))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ Arr
        |│  │     │  ├─ Obj
        |│  │     │  │  ├─ Key(city: _.city)
        |│  │     │  │  ╰─ Key(pop: _.pop)
        |│  │     │  ╰─ Ident(_)
        |│  │     ╰─ Obj
        |│  │        ├─ Key(0: (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined)
        |│  │        ├─ Key(1: (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].city : undefined)
        |│  │        ╰─ Key(src: __val)
        |│  ╰─ Scope(Map())
        |├─ $SortF
        |│  ├─ SortKey(0 -> Descending)
        |│  ╰─ SortKey(1 -> Ascending)
        |╰─ $ProjectF
        |   ├─ Name("value" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ╰─ ExcludeId""".stripMargin)

    "plan sort with expression, alias, and filter" in {
      plan(sqlE"select pop/1000 as popInK from zips where pop >= 1000 order by popInK") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.And(
            isNumeric(BsonField.Name("pop")),
            Selector.Doc(BsonField.Name("pop") -> Selector.Gte(Bson.Int32(1000))))),
          $project(
            reshape(
              "popInK" ->
                $cond(
                  $or(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $and(
                      $lte($literal(Check.minDate), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Regex("", ""))))),
                  divide($field("pop"), $literal(Bson.Int32(1000))),
                  $literal(Bson.Undefined))),
            ExcludeId),
          $sort(NonEmptyList(BsonField.Name("popInK") -> SortDir.Ascending))))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $MatchF
        |│  ╰─ And
        |│     ├─ Or
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($pop -> Type(Int32))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($pop -> Type(Int64))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($pop -> Type(Dec))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($pop -> Type(Text))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($pop -> Type(Date))
        |│     │  ╰─ Doc
        |│     │     ╰─ Expr($pop -> Type(Bool))
        |│     ╰─ Doc
        |│        ╰─ Expr($pop -> Gte(Int32(1000)))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ Arr
        |│  │     │  ├─ Obj
        |│  │     │  │  ╰─ Key(popInK: ((isNumber(_.pop) || ((_.pop instanceof NumberInt) || (_.pop instanceof NumberLong))) || (_.pop instanceof Date)) ? _.pop / 1000 : undefined)
        |│  │     │  ╰─ Ident(_)
        |│  │     ╰─ Obj
        |│  │        ├─ Key(0: (((isNumber(
        |│  │        │      (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined) || ((((isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined) instanceof NumberInt) || (((isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined) instanceof NumberLong))) || (((isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined) instanceof Date)) && (isObject(__val[1]) && (! Array.isArray(__val[1])))) ? __val[1].pop / 1000 : undefined)
        |│  │        ╰─ Key(src: __val)
        |│  ╰─ Scope(Map())
        |├─ $SortF
        |│  ╰─ SortKey(0 -> Ascending)
        |╰─ $ProjectF
        |   ├─ Name("value" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ╰─ ExcludeId""".stripMargin)

    "plan multiple column sort with wildcard" in {
      plan(sqlE"select * from zips order by pop, city desc") must
       beWorkflow0(chain[Workflow](
         $read(collection("db", "zips")),
         $sort(NonEmptyList(
           BsonField.Name("pop") -> SortDir.Ascending,
           BsonField.Name("city") -> SortDir.Descending))))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ Let(__val)
        |│  │     │  ├─ JsCore([_._id, _])
        |│  │     │  ╰─ JsCore([
        |│  │     │            (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1] : undefined,
        |│  │     │            __val])
        |│  │     ╰─ Obj
        |│  │        ├─ Key(0: (isObject(__val[1][1]) && (! Array.isArray(__val[1][1]))) ? __val[1][1].pop : undefined)
        |│  │        ├─ Key(1: (isObject(__val[1][1]) && (! Array.isArray(__val[1][1]))) ? __val[1][1].city : undefined)
        |│  │        ╰─ Key(src: __val)
        |│  ╰─ Scope(Map())
        |├─ $SortF
        |│  ├─ SortKey(0 -> Ascending)
        |│  ╰─ SortKey(1 -> Descending)
        |╰─ $ProjectF
        |   ├─ Name("value" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ╰─ ExcludeId""".stripMargin)

    "plan many sort columns" in {
      plan(sqlE"select * from zips order by pop, state, city, a4, a5, a6") must
       beWorkflow0(chain[Workflow](
         $read(collection("db", "zips")),
         $sort(NonEmptyList(
           BsonField.Name("pop") -> SortDir.Ascending,
           BsonField.Name("state") -> SortDir.Ascending,
           BsonField.Name("city") -> SortDir.Ascending,
           BsonField.Name("a4") -> SortDir.Ascending,
           BsonField.Name("a5") -> SortDir.Ascending,
           BsonField.Name("a6") -> SortDir.Ascending))))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ Let(__val)
        |│  │     │  ├─ JsCore([_._id, _])
        |│  │     │  ╰─ JsCore([
        |│  │     │            (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1] : undefined,
        |│  │     │            __val])
        |│  │     ╰─ Obj
        |│  │        ├─ Key(0: (isObject(__val[1][1]) && (! Array.isArray(__val[1][1]))) ? __val[1][1].pop : undefined)
        |│  │        ├─ Key(1: (isObject(__val[1][1]) && (! Array.isArray(__val[1][1]))) ? __val[1][1].state : undefined)
        |│  │        ├─ Key(2: (isObject(__val[1][1]) && (! Array.isArray(__val[1][1]))) ? __val[1][1].city : undefined)
        |│  │        ├─ Key(3: (isObject(__val[1][1]) && (! Array.isArray(__val[1][1]))) ? __val[1][1].a4 : undefined)
        |│  │        ├─ Key(4: (isObject(__val[1][1]) && (! Array.isArray(__val[1][1]))) ? __val[1][1].a5 : undefined)
        |│  │        ├─ Key(5: (isObject(__val[1][1]) && (! Array.isArray(__val[1][1]))) ? __val[1][1].a6 : undefined)
        |│  │        ╰─ Key(src: __val)
        |│  ╰─ Scope(Map())
        |├─ $SortF
        |│  ├─ SortKey(0 -> Ascending)
        |│  ├─ SortKey(1 -> Ascending)
        |│  ├─ SortKey(2 -> Ascending)
        |│  ├─ SortKey(3 -> Ascending)
        |│  ├─ SortKey(4 -> Ascending)
        |│  ╰─ SortKey(5 -> Ascending)
        |╰─ $ProjectF
        |   ├─ Name("value" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ╰─ ExcludeId""".stripMargin)

    "plan efficient count and field ref" in {
      plan(sqlE"SELECT city, COUNT(*) AS cnt FROM zips ORDER BY cnt DESC") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $group(
              grouped(
                "city" -> $push($field("city")),
                "cnt"  -> $sum($literal(Bson.Int32(1)))),
              \/-($literal(Bson.Null))),
            $unwind(DocField("city")),
            $sort(NonEmptyList(BsonField.Name("cnt") -> SortDir.Descending)))
        }
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; zips)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Let(__val)
        |│  │  │  │     ├─ JsCore([_._id, _])
        |│  │  │  │     ╰─ JsCore([__val[0], __val[1]])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$$ROOT" })
        |│  │  │  ╰─ By({ "$literal": null })
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; zips)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Let(__val)
        |│     │  │     ├─ JsCore([_._id, _])
        |│     │  │     ╰─ JsCore([__val[0], __val[1]])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $GroupF
        |│     │  ├─ Grouped
        |│     │  │  ╰─ Name("f0" -> { "$sum": { "$literal": NumberInt("1") } })
        |│     │  ╰─ By({ "$literal": null })
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) { return [null, { "left": [], "right": [value.f0] }] })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ JsCore([_.left, _.right])
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> { "$literal": true })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ Expr($0 -> Eq(Bool(true)))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Arr
        |│  │     ├─ Obj
        |│  │     │  ├─ Key(city: (isObject(_.src[0][1]) && (! Array.isArray(_.src[0][1]))) ? _.src[0][1].city : undefined)
        |│  │     │  ╰─ Key(cnt: _.src[1])
        |│  │     ╰─ JsCore(_.src)
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> {
        |│  │       "$arrayElemAt": [
        |│  │         { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] },
        |│  │         { "$literal": NumberInt("1") }]
        |│  │     })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $SortF
        |│  ╰─ SortKey(0 -> Descending)
        |╰─ $ProjectF
        |   ├─ Name("value" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ╰─ ExcludeId""".stripMargin)

    "plan count and js expr" in {
      plan(sqlE"SELECT COUNT(*) as cnt, LENGTH(city) FROM zips") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
              "1" ->
                If(Call(ident("isString"), List(Select(ident("x"), "city"))),
                  Call(ident("NumberLong"),
                    List(Select(Select(ident("x"), "city"), "length"))),
                  ident("undefined")))))),
              ListMap()),
            $group(
              grouped(
                "cnt" -> $sum($literal(Bson.Int32(1))),
                "1"   -> $push($field("1"))),
              \/-($literal(Bson.Null))),
            $unwind(DocField("1")))
        }
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; zips)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Let(__val)
        |│  │  │  │     ├─ JsCore([_._id, _])
        |│  │  │  │     ╰─ JsCore([
        |│  │  │  │               __val[0],
        |│  │  │  │               (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].city : undefined,
        |│  │  │  │               (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? NumberLong(__val[1].city.length) : undefined,
        |│  │  │  │               __val[1]])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("f0" -> { "$sum": { "$literal": NumberInt("1") } })
        |│  │  │  ╰─ By({ "$literal": null })
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$f0" })
        |│  │  │  ╰─ By({ "$literal": null })
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; zips)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Let(__val)
        |│     │  │     ├─ JsCore([_._id, _])
        |│     │  │     ╰─ JsCore([
        |│     │  │               __val[0],
        |│     │  │               (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].city : undefined,
        |│     │  │               (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? NumberLong(__val[1].city.length) : undefined,
        |│     │  │               __val[1]])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) { return [null, { "left": [], "right": [value] }] })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ JsCore([_.left, _.right])
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> { "$literal": true })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ Expr($0 -> Eq(Bool(true)))
        |╰─ $ProjectF
        |   ├─ Name("cnt" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ├─ Name("1" -> {
        |   │       "$cond": [
        |   │         {
        |   │           "$and": [
        |   │             {
        |   │               "$lte": [
        |   │                 { "$literal": "" },
        |   │                 {
        |   │                   "$arrayElemAt": [
        |   │                     { "$arrayElemAt": ["$src", { "$literal": NumberInt("1") }] },
        |   │                     { "$literal": NumberInt("1") }]
        |   │                 }]
        |   │             },
        |   │             {
        |   │               "$lt": [
        |   │                 {
        |   │                   "$arrayElemAt": [
        |   │                     { "$arrayElemAt": ["$src", { "$literal": NumberInt("1") }] },
        |   │                     { "$literal": NumberInt("1") }]
        |   │                 },
        |   │                 { "$literal": {  } }]
        |   │             }]
        |   │         },
        |   │         {
        |   │           "$arrayElemAt": [
        |   │             { "$arrayElemAt": ["$src", { "$literal": NumberInt("1") }] },
        |   │             { "$literal": NumberInt("2") }]
        |   │         },
        |   │         { "$literal": undefined }]
        |   │     })
        |   ╰─ IgnoreId""".stripMargin)

    "plan trivial group by" in {
      plan(sqlE"select city from zips group by city") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $group(
          grouped(),
          -\/(reshape("0" -> $field("city")))),
        $project(
          reshape("value" -> $field("_id", "0")),
          ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(0: [_.city])
        |│  │     ╰─ Key(content: _)
        |│  ╰─ Scope(Map())
        |├─ $GroupF
        |│  ├─ Grouped
        |│  │  ╰─ Name("f0" -> { "$first": "$content.city" })
        |│  ╰─ By
        |│     ╰─ Name("0" -> "$0")
        |╰─ $ProjectF
        |   ├─ Name("value" -> "$f0")
        |   ╰─ ExcludeId""".stripMargin)

    "plan useless group by expression" in {
      plan(sqlE"select city from zips group by lower(city)") must
      beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          reshape("value" -> $field("city")),
          ExcludeId)))
    }

    "plan useful group by" in {
      plan(sqlE"""select city || ", " || state, sum(pop) from zips group by city, state""") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $group(
          grouped(
            "__tmp10" ->
              $first(
                $cond(
                  $or(
                    $and(
                      $lte($literal(Bson.Arr()), $field("city")),
                      $lt($field("city"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                    $and(
                      $lte($literal(Bson.Text("")), $field("city")),
                      $lt($field("city"), $literal(Bson.Doc())))),
                  $field("city"),
                  $literal(Bson.Undefined))),
            "__tmp11" ->
              $first(
                $cond(
                  $or(
                    $and(
                      $lte($literal(Bson.Arr()), $field("state")),
                      $lt($field("state"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                    $and(
                      $lte($literal(Bson.Text("")), $field("state")),
                      $lt($field("state"), $literal(Bson.Doc())))),
                  $field("state"),
                  $literal(Bson.Undefined))),
            "1" ->
              $sum(
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $field("pop"),
                  $literal(Bson.Undefined)))),
          -\/(reshape(
            "0" -> $field("city"),
            "1" -> $field("state")))),
        $project(
          reshape(
            "0" ->
              $concat(
                $concat($field("__tmp10"), $literal(Bson.Text(", "))),
                $field("__tmp11")),
            "1" -> $field("1")),
          IgnoreId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(0: [_.city, _.state])
        |│  │     ╰─ Key(content: _)
        |│  ╰─ Scope(Map())
        |├─ $GroupF
        |│  ├─ Grouped
        |│  │  ├─ Name("f0" -> {
        |│  │  │       "$first": {
        |│  │  │         "$cond": [
        |│  │  │           {
        |│  │  │             "$or": [
        |│  │  │               {
        |│  │  │                 "$and": [
        |│  │  │                   { "$lte": [{ "$literal": [] }, "$content.city"] },
        |│  │  │                   { "$lt": ["$content.city", { "$literal": BinData(0, "") }] }]
        |│  │  │               },
        |│  │  │               {
        |│  │  │                 "$and": [
        |│  │  │                   { "$lte": [{ "$literal": "" }, "$content.city"] },
        |│  │  │                   { "$lt": ["$content.city", { "$literal": {  } }] }]
        |│  │  │               }]
        |│  │  │           },
        |│  │  │           "$content.city",
        |│  │  │           { "$literal": undefined }]
        |│  │  │       }
        |│  │  │     })
        |│  │  ├─ Name("f1" -> {
        |│  │  │       "$first": {
        |│  │  │         "$cond": [
        |│  │  │           {
        |│  │  │             "$or": [
        |│  │  │               {
        |│  │  │                 "$and": [
        |│  │  │                   { "$lte": [{ "$literal": [] }, "$content.state"] },
        |│  │  │                   { "$lt": ["$content.state", { "$literal": BinData(0, "") }] }]
        |│  │  │               },
        |│  │  │               {
        |│  │  │                 "$and": [
        |│  │  │                   { "$lte": [{ "$literal": "" }, "$content.state"] },
        |│  │  │                   { "$lt": ["$content.state", { "$literal": {  } }] }]
        |│  │  │               }]
        |│  │  │           },
        |│  │  │           "$content.state",
        |│  │  │           { "$literal": undefined }]
        |│  │  │       }
        |│  │  │     })
        |│  │  ╰─ Name("f2" -> {
        |│  │          "$sum": {
        |│  │            "$cond": [
        |│  │              {
        |│  │                "$and": [
        |│  │                  { "$lt": [{ "$literal": null }, "$content.pop"] },
        |│  │                  { "$lt": ["$content.pop", { "$literal": "" }] }]
        |│  │              },
        |│  │              "$content.pop",
        |│  │              { "$literal": undefined }]
        |│  │          }
        |│  │        })
        |│  ╰─ By
        |│     ╰─ Name("0" -> "$0")
        |╰─ $ProjectF
        |   ├─ Name("0" -> {
        |   │       "$let": {
        |   │         "vars": {
        |   │           "a1": {
        |   │             "$let": {
        |   │               "vars": { "a1": "$f0", "a2": { "$literal": ", " } },
        |   │               "in": {
        |   │                 "$cond": [
        |   │                   { "$and": [{ "$isArray": "$$a1" }, { "$isArray": "$$a2" }] },
        |   │                   { "$concatArrays": ["$$a1", "$$a2"] },
        |   │                   { "$concat": ["$$a1", "$$a2"] }]
        |   │               }
        |   │             }
        |   │           },
        |   │           "a2": "$f1"
        |   │         },
        |   │         "in": {
        |   │           "$cond": [
        |   │             { "$and": [{ "$isArray": "$$a1" }, { "$isArray": "$$a2" }] },
        |   │             { "$concatArrays": ["$$a1", "$$a2"] },
        |   │             { "$concat": ["$$a1", "$$a2"] }]
        |   │         }
        |   │       }
        |   │     })
        |   ├─ Name("1" -> "$f2")
        |   ╰─ IgnoreId""".stripMargin)

    "plan group by expression" in {
      plan(sqlE"select city, sum(pop) from zips group by lower(city)") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $group(
          grouped(
            "city" -> $push($field("city")),
            "1" ->
              $sum(
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $field("pop"),
                  $literal(Bson.Undefined)))),
          -\/(reshape(
            "0" ->
              $cond(
                $and(
                  $lte($literal(Bson.Text("")), $field("city")),
                  $lt($field("city"), $literal(Bson.Doc()))),
                $toLower($field("city")),
                $literal(Bson.Undefined))))),
        $unwind(DocField(BsonField.Name("city")))))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; zips)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Let(__val)
        |│  │  │  │     ├─ JsCore([_._id, _])
        |│  │  │  │     ╰─ JsCore([
        |│  │  │  │               __val[0],
        |│  │  │  │               [__val[0]],
        |│  │  │  │               (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1] : undefined,
        |│  │  │  │               [
        |│  │  │  │                 (isString(
        |│  │  │  │                   (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].city : undefined) && (isObject(__val[1]) && (! Array.isArray(__val[1])))) ? __val[1].city.toLowerCase() : undefined],
        |│  │  │  │               __val[1]])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$$ROOT" })
        |│  │  │  ╰─ By({ "$literal": null })
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; zips)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Let(__val)
        |│     │  │     ├─ Let(__val)
        |│     │  │     │  ├─ JsCore([_._id, _])
        |│     │  │     │  ╰─ JsCore([
        |│     │  │     │            __val[0],
        |│     │  │     │            [__val[0]],
        |│     │  │     │            (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1] : undefined,
        |│     │  │     │            [
        |│     │  │     │              (isString(
        |│     │  │     │                (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].city : undefined) && (isObject(__val[1]) && (! Array.isArray(__val[1])))) ? __val[1].city.toLowerCase() : undefined],
        |│     │  │     │            __val[1]])
        |│     │  │     ╰─ Obj
        |│     │  │        ├─ Key(f0: ((isNumber(
        |│     │  │        │      (isObject(__val[4]) && (! Array.isArray(__val[4]))) ? __val[4].pop : undefined) || ((((isObject(__val[4]) && (! Array.isArray(__val[4]))) ? __val[4].pop : undefined) instanceof NumberInt) || (((isObject(__val[4]) && (! Array.isArray(__val[4]))) ? __val[4].pop : undefined) instanceof NumberLong))) && (isObject(__val[4]) && (! Array.isArray(__val[4])))) ? __val[4].pop : undefined)
        |│     │  │        ╰─ Key(b0: [
        |│     │  │               (isString(
        |│     │  │                 (isObject(__val[4]) && (! Array.isArray(__val[4]))) ? __val[4].city : undefined) && (isObject(__val[4]) && (! Array.isArray(__val[4])))) ? __val[4].city.toLowerCase() : undefined])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $GroupF
        |│     │  ├─ Grouped
        |│     │  │  ╰─ Name("f0" -> { "$sum": "$f0" })
        |│     │  ╰─ By
        |│     │     ╰─ Name("0" -> "$b0")
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) { return [null, { "left": [], "right": [value.f0] }] })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ JsCore([_.left, _.right])
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> { "$literal": true })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ Expr($0 -> Eq(Bool(true)))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(city: _.src[0][2].city)
        |│  │     ╰─ Key(1: _.src[1])
        |│  ╰─ Scope(Map())
        |╰─ $ProjectF
        |   ├─ Name("city" -> true)
        |   ├─ Name("1" -> true)
        |   ╰─ IgnoreId""".stripMargin)

    "plan group by month" in {
      plan(sqlE"""select avg(score) as a, DATE_PART("month", `date`) as m from caloriesBurnedData group by DATE_PART("month", `date`)""") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "caloriesBurnedData")),
          $group(
            grouped(
              "a" ->
                $avg(
                  $cond(
                    $and(
                      $lt($literal(Bson.Null), $field("score")),
                      $lt($field("score"), $literal(Bson.Text("")))),
                    $field("score"),
                    $literal(Bson.Undefined)))),
            -\/(reshape(
              "0" ->
                $cond(
                  $and(
                    $lte($literal(Check.minDate), $field("date")),
                    $lt($field("date"), $literal(Bson.Regex("", "")))),
                  $month($field("date")),
                  $literal(Bson.Undefined))))),
          $project(
            reshape(
              "a" -> $include(),
              "m" -> $field("_id", "0")),
            IgnoreId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; caloriesBurnedData)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(0: [(_.date instanceof Date) ? _.date.getUTCMonth() + 1 : undefined])
        |│  │     ╰─ Key(content: _)
        |│  ╰─ Scope(Map())
        |╰─ $GroupF
        |   ├─ Grouped
        |   │  ├─ Name("a" -> {
        |   │  │       "$avg": {
        |   │  │         "$cond": [
        |   │  │           {
        |   │  │             "$and": [
        |   │  │               { "$lt": [{ "$literal": null }, "$content.score"] },
        |   │  │               { "$lt": ["$content.score", { "$literal": "" }] }]
        |   │  │           },
        |   │  │           "$content.score",
        |   │  │           { "$literal": undefined }]
        |   │  │       }
        |   │  │     })
        |   │  ╰─ Name("m" -> {
        |   │          "$first": {
        |   │            "$cond": [
        |   │              {
        |   │                "$and": [
        |   │                  {
        |   │                    "$lte": [
        |   │                      { "$literal": ISODate("-292275055-05-16T16:47:04.192Z") },
        |   │                      "$content.date"]
        |   │                  },
        |   │                  { "$lt": ["$content.date", { "$literal": new RegExp("", "") }] }]
        |   │              },
        |   │              { "$month": "$content.date" },
        |   │              { "$literal": undefined }]
        |   │          }
        |   │        })
        |   ╰─ By
        |      ╰─ Name("0" -> "$0")""".stripMargin)

    // FIXME: Needs an actual expectation
    "plan expr3 with grouping" in {
      plan(sqlE"select case when pop > 1000 then city else lower(city) end, count(*) from zips group by city") must
        beRight
    }

    "plan trivial group by with wildcard" in {
      plan(sqlE"select * from zips group by city") must
        beWorkflow($read(collection("db", "zips")))
    }

    "plan count grouped by single field" in {
      plan(sqlE"select count(*) from bar group by baz") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "bar")),
            $group(
              grouped("__tmp0" -> $sum($literal(Bson.Int32(1)))),
              -\/(reshape("0" -> $field("baz")))),
            $project(
              reshape(
                "value" -> $field("__tmp0")),
              ExcludeId))
        }
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; bar)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ╰─ Key(0: [_.baz])
        |│  ╰─ Scope(Map())
        |├─ $GroupF
        |│  ├─ Grouped
        |│  │  ╰─ Name("f0" -> { "$sum": { "$literal": NumberInt("1") } })
        |│  ╰─ By
        |│     ╰─ Name("0" -> "$0")
        |╰─ $ProjectF
        |   ├─ Name("value" -> "$f0")
        |   ╰─ ExcludeId""".stripMargin)

    "plan count and sum grouped by single field" in {
      plan(sqlE"select count(*) as cnt, sum(biz) as sm from bar group by baz") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "bar")),
            $group(
              grouped(
                "cnt" -> $sum($literal(Bson.Int32(1))),
                "sm" ->
                  $sum(
                    $cond(
                      $and(
                        $lt($literal(Bson.Null), $field("biz")),
                        $lt($field("biz"), $literal(Bson.Text("")))),
                      $field("biz"),
                      $literal(Bson.Undefined)))),
              -\/(reshape("0" -> $field("baz")))))
        }
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; bar)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(0: [_.baz])
        |│  │     ╰─ Key(content: _)
        |│  ╰─ Scope(Map())
        |╰─ $GroupF
        |   ├─ Grouped
        |   │  ├─ Name("cnt" -> { "$sum": { "$literal": NumberInt("1") } })
        |   │  ╰─ Name("sm" -> {
        |   │          "$sum": {
        |   │            "$cond": [
        |   │              {
        |   │                "$and": [
        |   │                  { "$lt": [{ "$literal": null }, "$content.biz"] },
        |   │                  { "$lt": ["$content.biz", { "$literal": "" }] }]
        |   │              },
        |   │              "$content.biz",
        |   │              { "$literal": undefined }]
        |   │          }
        |   │        })
        |   ╰─ By
        |      ╰─ Name("0" -> "$0")""".stripMargin)

    "plan sum grouped by single field with filter" in {
      plan(sqlE"""select sum(pop) as sm from zips where state="CO" group by city""") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $match(Selector.Doc(
              BsonField.Name("state") -> Selector.Eq(Bson.Text("CO")))),
            $group(
              grouped("sm" ->
                $sum(
                  $cond(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $field("pop"),
                    $literal(Bson.Undefined)))),
              -\/(reshape("0" -> $field("city")))))
        }
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ Expr($state -> Eq(Text(CO)))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(0: [_.city])
        |│  │     ╰─ Key(content: _)
        |│  ╰─ Scope(Map())
        |╰─ $GroupF
        |   ├─ Grouped
        |   │  ╰─ Name("sm" -> {
        |   │          "$sum": {
        |   │            "$cond": [
        |   │              {
        |   │                "$and": [
        |   │                  { "$lt": [{ "$literal": null }, "$content.pop"] },
        |   │                  { "$lt": ["$content.pop", { "$literal": "" }] }]
        |   │              },
        |   │              "$content.pop",
        |   │              { "$literal": undefined }]
        |   │          }
        |   │        })
        |   ╰─ By
        |      ╰─ Name("0" -> "$0")""".stripMargin)

    "plan count and field when grouped" in {
      plan(sqlE"select count(*) as cnt, city from zips group by city") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $group(
              grouped(
                "cnt"  -> $sum($literal(Bson.Int32(1)))),
              -\/(reshape("0" -> $field("city")))),
            $project(
              reshape(
                "cnt" -> $include(),
                "city" -> $field("_id", "0")),
              IgnoreId))
        }
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(0: [_.city])
        |│  │     ╰─ Key(content: _)
        |│  ╰─ Scope(Map())
        |╰─ $GroupF
        |   ├─ Grouped
        |   │  ├─ Name("cnt" -> { "$sum": { "$literal": NumberInt("1") } })
        |   │  ╰─ Name("city" -> { "$first": "$content.city" })
        |   ╰─ By
        |      ╰─ Name("0" -> "$0")""".stripMargin)

    "collect unaggregated fields into single doc when grouping" in {
      plan(sqlE"select city, state, sum(pop) from zips") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          reshape(
            "__tmp3" -> reshape(
              "city"  -> $field("city"),
              "state" -> $field("state")),
            "__tmp4" -> reshape(
              "__tmp2" ->
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $field("pop"),
                  $literal(Bson.Undefined)))),
          IgnoreId),
        $group(
          grouped(
            "2" -> $sum($field("__tmp4", "__tmp2")),
            "__tmp3" -> $push($field("__tmp3"))),
          \/-($literal(Bson.Null))),
        $unwind(DocField("__tmp3")),
        $project(
          reshape(
            "city"  -> $field("__tmp3", "city"),
            "state" -> $field("__tmp3", "state"),
            "2"     -> $field("2")),
          IgnoreId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; zips)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Let(__val)
        |│  │  │  │     ├─ JsCore([_._id, _])
        |│  │  │  │     ╰─ Arr
        |│  │  │  │        ├─ JsCore(__val[0])
        |│  │  │  │        ├─ Obj
        |│  │  │  │        │  ╰─ Key(city: (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].city : undefined)
        |│  │  │  │        ├─ Obj
        |│  │  │  │        │  ╰─ Key(state: (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].state : undefined)
        |│  │  │  │        ╰─ JsCore(__val[1])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$$ROOT" })
        |│  │  │  ╰─ By({ "$literal": null })
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; zips)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Let(__val)
        |│     │  │     ├─ Let(__val)
        |│     │  │     │  ├─ JsCore([_._id, _])
        |│     │  │     │  ╰─ Arr
        |│     │  │     │     ├─ JsCore(__val[0])
        |│     │  │     │     ├─ Obj
        |│     │  │     │     │  ╰─ Key(city: (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].city : undefined)
        |│     │  │     │     ├─ Obj
        |│     │  │     │     │  ╰─ Key(state: (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].state : undefined)
        |│     │  │     │     ╰─ JsCore(__val[1])
        |│     │  │     ╰─ Obj
        |│     │  │        ╰─ Key(f0: ((isNumber(
        |│     │  │               (isObject(__val[3]) && (! Array.isArray(__val[3]))) ? __val[3].pop : undefined) || ((((isObject(__val[3]) && (! Array.isArray(__val[3]))) ? __val[3].pop : undefined) instanceof NumberInt) || (((isObject(__val[3]) && (! Array.isArray(__val[3]))) ? __val[3].pop : undefined) instanceof NumberLong))) && (isObject(__val[3]) && (! Array.isArray(__val[3])))) ? __val[3].pop : undefined)
        |│     │  ╰─ Scope(Map())
        |│     ├─ $GroupF
        |│     │  ├─ Grouped
        |│     │  │  ╰─ Name("f0" -> { "$sum": "$f0" })
        |│     │  ╰─ By({ "$literal": null })
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) { return [null, { "left": [], "right": [value.f0] }] })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ JsCore([_.left, _.right])
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> { "$literal": true })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ Expr($0 -> Eq(Bool(true)))
        |╰─ $SimpleMapF
        |   ├─ Map
        |   │  ╰─ SpliceObjects
        |   │     ├─ SpliceObjects
        |   │     │  ├─ JsCore(_.src[0][1])
        |   │     │  ╰─ JsCore(_.src[0][2])
        |   │     ╰─ Obj
        |   │        ╰─ Key(2: _.src[1])
        |   ╰─ Scope(Map())""".stripMargin)

    "plan unaggregated field when grouping, second case" in {
      // NB: the point being that we don't want to push $$ROOT
      plan(sqlE"select max(pop)/1000, pop from zips") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $group(
              grouped(
                "__tmp3" ->
                  $max($cond(
                    $or(
                      $and(
                        $lt($literal(Bson.Null), $field("pop")),
                        $lt($field("pop"), $literal(Bson.Text("")))),
                      $and(
                        $lte($literal(Check.minDate), $field("pop")),
                        $lt($field("pop"), $literal(Bson.Regex("", ""))))),
                    $field("pop"),
                    $literal(Bson.Undefined))),
                "pop"    -> $push($field("pop"))),
              \/-($literal(Bson.Null))),
            $unwind(DocField("pop")),
            $project(
              reshape(
                "0"   -> divide($field("__tmp3"), $literal(Bson.Int32(1000))),
                "pop" -> $field("pop")),
              IgnoreId))
        }
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; zips)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Let(__val)
        |│  │  │  │     ├─ Let(__val)
        |│  │  │  │     │  ├─ JsCore([_._id, _])
        |│  │  │  │     │  ╰─ JsCore([__val[0], __val[1]])
        |│  │  │  │     ╰─ Obj
        |│  │  │  │        ╰─ Key(f0: (((isNumber(
        |│  │  │  │               (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined) || ((((isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined) instanceof NumberInt) || (((isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined) instanceof NumberLong))) || (((isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined) instanceof Date)) && (isObject(__val[1]) && (! Array.isArray(__val[1])))) ? __val[1].pop : undefined)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("f0" -> { "$max": "$f0" })
        |│  │  │  ╰─ By({ "$literal": null })
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$f0" })
        |│  │  │  ╰─ By({ "$literal": null })
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; zips)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Let(__val)
        |│     │  │     ├─ JsCore([_._id, _])
        |│     │  │     ╰─ JsCore([__val[0], __val[1]])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) { return [null, { "left": [], "right": [value] }] })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ JsCore([_.left, _.right])
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> { "$literal": true })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ Expr($0 -> Eq(Bool(true)))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(0: _.src[0] / 1000)
        |│  │     ╰─ Key(pop: (isObject(_.src[1][1]) && (! Array.isArray(_.src[1][1]))) ? _.src[1][1].pop : undefined)
        |│  ╰─ Scope(Map())
        |╰─ $ProjectF
        |   ├─ Name("0" -> true)
        |   ├─ Name("pop" -> true)
        |   ╰─ IgnoreId""".stripMargin)

    "plan double aggregation with another projection" in {
      plan(sqlE"select sum(avg(pop)), min(city) from zips group by foo") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $group(
              grouped(
                "1"    ->
                  $min($cond(
                    $or(
                      $and(
                        $lt($literal(Bson.Null), $field("city")),
                        $lt($field("city"), $literal(Bson.Doc()))),
                      $and(
                        $lte($literal(Bson.Bool(false)), $field("city")),
                        $lt($field("city"), $literal(Bson.Regex("", ""))))),
                    $field("city"),
                    $literal(Bson.Undefined))),
                "__tmp10" ->
                  $avg($cond(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $field("pop"),
                    $literal(Bson.Undefined)))),
              -\/(reshape("0" -> $field("foo")))),
            $group(
              grouped(
                "0" -> $sum($field("__tmp10")),
                "1" -> $push($field("1"))),
              \/-($literal(Bson.Null))),
            $unwind(DocField("1")))
        }
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; zips)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Obj
        |│  │  │  │     ├─ Key(0: [_.foo])
        |│  │  │  │     ╰─ Key(content: _)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ├─ Name("f0" -> {
        |│  │  │  │  │       "$avg": {
        |│  │  │  │  │         "$cond": [
        |│  │  │  │  │           {
        |│  │  │  │  │             "$and": [
        |│  │  │  │  │               { "$lt": [{ "$literal": null }, "$content.pop"] },
        |│  │  │  │  │               { "$lt": ["$content.pop", { "$literal": "" }] }]
        |│  │  │  │  │           },
        |│  │  │  │  │           "$content.pop",
        |│  │  │  │  │           { "$literal": undefined }]
        |│  │  │  │  │       }
        |│  │  │  │  │     })
        |│  │  │  │  ╰─ Name("f1" -> {
        |│  │  │  │          "$min": {
        |│  │  │  │            "$cond": [
        |│  │  │  │              {
        |│  │  │  │                "$or": [
        |│  │  │  │                  {
        |│  │  │  │                    "$and": [
        |│  │  │  │                      { "$lt": [{ "$literal": null }, "$content.city"] },
        |│  │  │  │                      { "$lt": ["$content.city", { "$literal": {  } }] }]
        |│  │  │  │                  },
        |│  │  │  │                  {
        |│  │  │  │                    "$and": [
        |│  │  │  │                      { "$lte": [{ "$literal": false }, "$content.city"] },
        |│  │  │  │                      { "$lt": ["$content.city", { "$literal": new RegExp("", "") }] }]
        |│  │  │  │                  }]
        |│  │  │  │              },
        |│  │  │  │              "$content.city",
        |│  │  │  │              { "$literal": undefined }]
        |│  │  │  │          }
        |│  │  │  │        })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> "$0")
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ JsCore([[_._id["0"]], _.f0, [[_._id["0"]], _.f1]])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("f0" -> { "$sum": { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] } })
        |│  │  │  ╰─ By({ "$literal": null })
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$f0" })
        |│  │  │  ╰─ By({ "$literal": null })
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; zips)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Obj
        |│     │  │     ├─ Key(0: [_.foo])
        |│     │  │     ╰─ Key(content: _)
        |│     │  ╰─ Scope(Map())
        |│     ├─ $GroupF
        |│     │  ├─ Grouped
        |│     │  │  ├─ Name("f0" -> {
        |│     │  │  │       "$avg": {
        |│     │  │  │         "$cond": [
        |│     │  │  │           {
        |│     │  │  │             "$and": [
        |│     │  │  │               { "$lt": [{ "$literal": null }, "$content.pop"] },
        |│     │  │  │               { "$lt": ["$content.pop", { "$literal": "" }] }]
        |│     │  │  │           },
        |│     │  │  │           "$content.pop",
        |│     │  │  │           { "$literal": undefined }]
        |│     │  │  │       }
        |│     │  │  │     })
        |│     │  │  ╰─ Name("f1" -> {
        |│     │  │          "$min": {
        |│     │  │            "$cond": [
        |│     │  │              {
        |│     │  │                "$or": [
        |│     │  │                  {
        |│     │  │                    "$and": [
        |│     │  │                      { "$lt": [{ "$literal": null }, "$content.city"] },
        |│     │  │                      { "$lt": ["$content.city", { "$literal": {  } }] }]
        |│     │  │                  },
        |│     │  │                  {
        |│     │  │                    "$and": [
        |│     │  │                      { "$lte": [{ "$literal": false }, "$content.city"] },
        |│     │  │                      { "$lt": ["$content.city", { "$literal": new RegExp("", "") }] }]
        |│     │  │                  }]
        |│     │  │              },
        |│     │  │              "$content.city",
        |│     │  │              { "$literal": undefined }]
        |│     │  │          }
        |│     │  │        })
        |│     │  ╰─ By
        |│     │     ╰─ Name("0" -> "$0")
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ JsCore([[_._id["0"]], _.f0, [[_._id["0"]], _.f1]])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $ProjectF
        |│     │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("2") }] })
        |│     │  ╰─ IgnoreId
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) { return [null, { "left": [], "right": [value["0"]] }] })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ JsCore([_.left, _.right])
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> { "$literal": true })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ Expr($0 -> Eq(Bool(true)))
        |╰─ $ProjectF
        |   ├─ Name("0" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ├─ Name("1" -> {
        |   │       "$arrayElemAt": [
        |   │         { "$arrayElemAt": ["$src", { "$literal": NumberInt("1") }] },
        |   │         { "$literal": NumberInt("1") }]
        |   │     })
        |   ╰─ IgnoreId""".stripMargin)

    "plan aggregation on grouped field" in {
      plan(sqlE"select city, count(city) from zips group by city") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $group(
              grouped(
                "1" -> $sum($literal(Bson.Int32(1)))),
              -\/(reshape("0" -> $field("city")))),
            $project(
              reshape(
                "city" -> $field("_id", "0"),
                "1"    -> $include()),
              IgnoreId))
        }
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(0: [_.city])
        |│  │     ╰─ Key(content: _)
        |│  ╰─ Scope(Map())
        |╰─ $GroupF
        |   ├─ Grouped
        |   │  ├─ Name("city" -> { "$first": "$content.city" })
        |   │  ╰─ Name("1" -> { "$sum": { "$literal": NumberInt("1") } })
        |   ╰─ By
        |      ╰─ Name("0" -> "$0")""".stripMargin)

    "plan multiple expressions using same field" in {
      plan(sqlE"select pop, sum(pop), pop/1000 from zips") must
      beWorkflow0(chain[Workflow](
        $read (collection("db", "zips")),
        $project(
          reshape(
            "__tmp5" -> reshape(
              "pop" -> $field("pop"),
              "2"   ->
                $cond(
                  $or(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $and(
                      $lte($literal(Check.minDate), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Regex("", ""))))),
                  divide($field("pop"), $literal(Bson.Int32(1000))),
                  $literal(Bson.Undefined))),
            "__tmp6" -> reshape(
              "__tmp2" ->
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $field("pop"),
                  $literal(Bson.Undefined)))),
          IgnoreId),
        $group(
          grouped(
            "1" -> $sum($field("__tmp6", "__tmp2")),
            "__tmp5" -> $push($field("__tmp5"))),
          \/-($literal(Bson.Null))),
        $unwind(DocField("__tmp5")),
        $project(
          reshape(
            "pop" -> $field("__tmp5", "pop"),
            "1"   -> $field("1"),
            "2"   -> $field("__tmp5", "2")),
          IgnoreId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $FoldLeftF
        |│  │  │  ├─ Chain
        |│  │  │  │  ├─ $ReadF(db; zips)
        |│  │  │  │  ├─ $SimpleMapF
        |│  │  │  │  │  ├─ Map
        |│  │  │  │  │  │  ╰─ Let(__val)
        |│  │  │  │  │  │     ├─ JsCore([_._id, _])
        |│  │  │  │  │  │     ╰─ JsCore([
        |│  │  │  │  │  │               __val[0],
        |│  │  │  │  │  │               __val[1],
        |│  │  │  │  │  │               [
        |│  │  │  │  │  │                 __val[0],
        |│  │  │  │  │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined,
        |│  │  │  │  │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop / 1000 : undefined]])
        |│  │  │  │  │  ╰─ Scope(Map())
        |│  │  │  │  ├─ $GroupF
        |│  │  │  │  │  ├─ Grouped
        |│  │  │  │  │  │  ╰─ Name("0" -> { "$push": "$$ROOT" })
        |│  │  │  │  │  ╰─ By({ "$literal": null })
        |│  │  │  │  ╰─ $ProjectF
        |│  │  │  │     ├─ Name("_id" -> "$_id")
        |│  │  │  │     ├─ Name("value")
        |│  │  │  │     │  ├─ Name("left" -> "$0")
        |│  │  │  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │  │  │     │  ╰─ Name("_id" -> "$_id")
        |│  │  │  │     ╰─ IncludeId
        |│  │  │  ╰─ Chain
        |│  │  │     ├─ $ReadF(db; zips)
        |│  │  │     ├─ $SimpleMapF
        |│  │  │     │  ├─ Map
        |│  │  │     │  │  ╰─ Let(__val)
        |│  │  │     │  │     ├─ Let(__val)
        |│  │  │     │  │     │  ├─ JsCore([_._id, _])
        |│  │  │     │  │     │  ╰─ JsCore([
        |│  │  │     │  │     │            __val[0],
        |│  │  │     │  │     │            __val[1],
        |│  │  │     │  │     │            [
        |│  │  │     │  │     │              __val[0],
        |│  │  │     │  │     │              (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined,
        |│  │  │     │  │     │              (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop / 1000 : undefined]])
        |│  │  │     │  │     ╰─ Obj
        |│  │  │     │  │        ╰─ Key(f0: ((isNumber(
        |│  │  │     │  │               (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined) || ((((isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined) instanceof NumberInt) || (((isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined) instanceof NumberLong))) && (isObject(__val[1]) && (! Array.isArray(__val[1])))) ? __val[1].pop : undefined)
        |│  │  │     │  ╰─ Scope(Map())
        |│  │  │     ├─ $GroupF
        |│  │  │     │  ├─ Grouped
        |│  │  │     │  │  ╰─ Name("f0" -> { "$sum": "$f0" })
        |│  │  │     │  ╰─ By({ "$literal": null })
        |│  │  │     ├─ $MapF
        |│  │  │     │  ├─ JavaScript(function (key, value) { return [null, { "left": [], "right": [value.f0] }] })
        |│  │  │     │  ╰─ Scope(Map())
        |│  │  │     ╰─ $ReduceF
        |│  │  │        ├─ JavaScript(function (key, values) {
        |│  │  │        │               var result = { "left": [], "right": [] };
        |│  │  │        │               values.forEach(
        |│  │  │        │                 function (value) {
        |│  │  │        │                   result.left = result.left.concat(value.left);
        |│  │  │        │                   result.right = result.right.concat(value.right)
        |│  │  │        │                 });
        |│  │  │        │               return result
        |│  │  │        │             })
        |│  │  │        ╰─ Scope(Map())
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ Doc
        |│  │  │     ├─ NotExpr($left -> Size(0))
        |│  │  │     ╰─ NotExpr($right -> Size(0))
        |│  │  ├─ $UnwindF(DocField(BsonField.Name("right")))
        |│  │  ├─ $UnwindF(DocField(BsonField.Name("left")))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ JsCore([_.left, _.right])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$$ROOT" })
        |│  │  │  ╰─ By({ "$literal": null })
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; zips)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Let(__val)
        |│     │  │     ├─ JsCore([_._id, _])
        |│     │  │     ╰─ JsCore([
        |│     │  │               __val[0],
        |│     │  │               __val[1],
        |│     │  │               [
        |│     │  │                 __val[0],
        |│     │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined,
        |│     │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop / 1000 : undefined]])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) { return [null, { "left": [], "right": [value] }] })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Arr
        |│  │     ├─ Arr
        |│  │     │  ├─ JsCore(_.left[0][0])
        |│  │     │  ├─ Obj
        |│  │     │  │  ╰─ Key(pop: (isObject(_.left[0][1]) && (! Array.isArray(_.left[0][1]))) ? _.left[0][1].pop : undefined)
        |│  │     │  ╰─ Obj
        |│  │     │     ╰─ Key(1: _.left[1])
        |│  │     ╰─ JsCore(_.right[2])
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> { "$literal": true })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ Expr($0 -> Eq(Bool(true)))
        |╰─ $SimpleMapF
        |   ├─ Map
        |   │  ╰─ SpliceObjects
        |   │     ├─ SpliceObjects
        |   │     │  ├─ JsCore(_.src[0][1])
        |   │     │  ╰─ JsCore(_.src[0][2])
        |   │     ╰─ Obj
        |   │        ╰─ Key(2: ((isNumber(_.src[1][1]) || ((_.src[1][1] instanceof NumberInt) || (_.src[1][1] instanceof NumberLong))) || (_.src[1][1] instanceof Date)) ? _.src[1][2] : undefined)
        |   ╰─ Scope(Map())""".stripMargin)

    "plan sum of expression in expression with another projection when grouped" in {
      plan(sqlE"select city, sum(pop-1)/1000 from zips group by city") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $group(
          grouped(
            "__tmp6" ->
              $sum(
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("pop")),
                    $lt($field("pop"), $literal(Bson.Text("")))),
                  $subtract($field("pop"), $literal(Bson.Int32(1))),
                  $literal(Bson.Undefined)))),
          -\/(reshape("0" -> $field("city")))),
        $project(
          reshape(
            "city" -> $field("_id", "0"),
            "1"    -> divide($field("__tmp6"), $literal(Bson.Int32(1000)))),
          IgnoreId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(0: [_.city])
        |│  │     ╰─ Key(content: _)
        |│  ╰─ Scope(Map())
        |├─ $GroupF
        |│  ├─ Grouped
        |│  │  ├─ Name("f0" -> { "$first": "$content.city" })
        |│  │  ╰─ Name("f1" -> {
        |│  │          "$sum": {
        |│  │            "$cond": [
        |│  │              {
        |│  │                "$and": [
        |│  │                  { "$lt": [{ "$literal": null }, "$content.pop"] },
        |│  │                  { "$lt": ["$content.pop", { "$literal": "" }] }]
        |│  │              },
        |│  │              { "$subtract": ["$content.pop", { "$literal": NumberInt("1") }] },
        |│  │              { "$literal": undefined }]
        |│  │          }
        |│  │        })
        |│  ╰─ By
        |│     ╰─ Name("0" -> "$0")
        |╰─ $ProjectF
        |   ├─ Name("city" -> "$f0")
        |   ├─ Name("1" -> {
        |   │       "$cond": [
        |   │         {
        |   │           "$eq": [{ "$literal": NumberInt("1000") }, { "$literal": NumberInt("0") }]
        |   │         },
        |   │         {
        |   │           "$cond": [
        |   │             { "$eq": ["$f1", { "$literal": NumberInt("0") }] },
        |   │             { "$literal": NaN },
        |   │             {
        |   │               "$cond": [
        |   │                 { "$gt": ["$f1", { "$literal": NumberInt("0") }] },
        |   │                 { "$literal": Infinity },
        |   │                 { "$literal": -Infinity }]
        |   │             }]
        |   │         },
        |   │         { "$divide": ["$f1", { "$literal": NumberInt("1000") }] }]
        |   │     })
        |   ╰─ IgnoreId""".stripMargin)

    "plan length of min (JS on top of reduce)" in {
      plan3_2(sqlE"select state, length(min(city)) as shortest from zips group by state") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $group(
            grouped(
              "__tmp6" ->
                $min(
                  $cond(
                    $and(
                      $lte($literal(Bson.Text("")), $field("city")),
                      $lt($field("city"), $literal(Bson.Doc()))),
                    $field("city"),
                    $literal(Bson.Undefined)))),
            -\/(reshape("0" -> $field("state")))),
          $project(
            reshape(
              "state"  -> $field("_id", "0"),
              "__tmp6" -> $include()),
            IgnoreId),
          $simpleMap(NonEmptyList(
            MapExpr(JsFn(Name("x"), obj(
              "state" -> Select(ident("x"), "state"),
              "shortest" ->
                Call(ident("NumberLong"),
                  List(Select(Select(ident("x"), "__tmp6"), "length"))))))),
            ListMap()),
          $project(
            reshape(
              "state"    -> $include(),
              "shortest" -> $include()),
            IgnoreId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(0: [_.state])
        |│  │     ╰─ Key(content: _)
        |│  ╰─ Scope(Map())
        |├─ $GroupF
        |│  ├─ Grouped
        |│  │  ├─ Name("f0" -> { "$first": "$content.state" })
        |│  │  ╰─ Name("f1" -> {
        |│  │          "$min": {
        |│  │            "$cond": [
        |│  │              {
        |│  │                "$and": [
        |│  │                  { "$lte": [{ "$literal": "" }, "$content.city"] },
        |│  │                  { "$lt": ["$content.city", { "$literal": {  } }] }]
        |│  │              },
        |│  │              "$content.city",
        |│  │              { "$literal": undefined }]
        |│  │          }
        |│  │        })
        |│  ╰─ By
        |│     ╰─ Name("0" -> "$0")
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(state: _.f0)
        |│  │     ╰─ Key(shortest: NumberLong(_.f1.length))
        |│  ╰─ Scope(Map())
        |╰─ $ProjectF
        |   ├─ Name("state" -> true)
        |   ├─ Name("shortest" -> true)
        |   ╰─ IgnoreId""".stripMargin)

    "plan js expr grouped by js expr" in {
      plan3_2(sqlE"select length(city) as len, count(*) as cnt from zips group by length(city)") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"),
              obj(
                "f0" ->
                  If(Call(ident("isString"), List(Select(ident("x"), "city"))),
                    Call(ident("NumberLong"),
                      List(Select(Select(ident("x"), "city"), "length"))),
                    ident("undefined")),
                "b0" ->
                  Arr(List(If(Call(ident("isString"), List(Select(ident("x"), "city"))),
                    Call(ident("NumberLong"),
                      List(Select(Select(ident("x"), "city"), "length"))),
                    ident("undefined")))))))),
            ListMap()),
          $group(
            grouped(
              "len" -> $first($field("f0")),
              "cnt" -> $sum($literal(Bson.Int32(1)))),
            -\/(reshape("0" -> $field("b0"))))))
    }

    "plan simple JS inside expression" in {
      plan3_2(sqlE"select length(city) + 1 from zips") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
            If(Call(ident("isString"), List(Select(ident("x"), "city"))),
                BinOp(jscore.Add,
                  Call(ident("NumberLong"),
                    List(Select(Select(ident("x"), "city"), "length"))),
                  jscore.Literal(Js.Num(1, false))),
                ident("undefined"))))),
            ListMap())))
    }

    "plan expressions with ~"in {
      plan(sqlE"""select foo ~ "bar.*", "abc" ~ "a|b", "baz" ~ regex, target ~ regex from a""") must beWorkflow(chain[Workflow](
        $read(collection("db", "a")),
        $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
          obj(
            "0" -> If(Call(ident("isString"), List(Select(ident("x"), "foo"))),
              Call(
                Select(New(Name("RegExp"), List(jscore.Literal(Js.Str("bar.*")), jscore.Literal(Js.Str("m")))), "test"),
                List(Select(ident("x"), "foo"))),
              ident("undefined")),
            "1" -> jscore.Literal(Js.Bool(true)),
            "2" -> If(Call(ident("isString"), List(Select(ident("x"), "regex"))),
              Call(
                Select(New(Name("RegExp"), List(Select(ident("x"), "regex"), jscore.Literal(Js.Str("m")))), "test"),
                List(jscore.Literal(Js.Str("baz")))),
              ident("undefined")),
            "3" ->
              If(
                BinOp(jscore.And,
                  Call(ident("isString"), List(Select(ident("x"), "regex"))),
                  Call(ident("isString"), List(Select(ident("x"), "target")))),
                Call(
                  Select(New(Name("RegExp"), List(Select(ident("x"), "regex"), jscore.Literal(Js.Str("m")))), "test"),
                  List(Select(ident("x"), "target"))),
                ident("undefined")))))),
          ListMap()),
        $project(
          reshape(
            "0" -> $include(),
            "1" -> $include(),
            "2" -> $include(),
            "3" -> $include()),
          IgnoreId)))
    }

    "plan object flatten" in {
      plan(sqlE"select geo{*} from usa_factbook") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "usa_factbook")),
            $simpleMap(
              NonEmptyList(
                MapExpr(JsFn(Name("x"), obj(
                  "__tmp2" ->
                    If(
                      BinOp(jscore.And,
                        Call(ident("isObject"), List(Select(ident("x"), "geo"))),
                        UnOp(jscore.Not,
                          Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), "geo"))))),
                      Select(ident("x"), "geo"),
                      Obj(ListMap(Name("") -> ident("undefined"))))))),
                FlatExpr(JsFn(Name("x"), Select(ident("x"), "__tmp2")))),
              ListMap()),
            $project(
              reshape("value" -> $field("__tmp2")),
              ExcludeId))
        }
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; usa_factbook)
        |├─ $ProjectF
        |│  ├─ Name("0" -> {
        |│  │       "$cond": [
        |│  │         {
        |│  │           "$and": [
        |│  │             { "$lte": [{ "$literal": {  } }, "$geo"] },
        |│  │             { "$lt": ["$geo", { "$literal": [] }] }]
        |│  │         },
        |│  │         "$geo",
        |│  │         { "$literal": undefined }]
        |│  │     })
        |│  ╰─ IgnoreId
        |├─ $SimpleMapF
        |│  ├─ Flatten
        |│  │  ╰─ JsCore(_["0"])
        |│  ╰─ Scope(Map())
        |╰─ $ProjectF
        |   ├─ Name("value" -> "$0")
        |   ╰─ ExcludeId""".stripMargin)

    "plan array project with concat (3.0-)" in {
      plan3_0(sqlE"select city, loc[0] from zips") must
        beWorkflow {
          chain[Workflow](
            $read(collection("db", "zips")),
            $simpleMap(
              NonEmptyList(
                MapExpr(JsFn(Name("x"), obj(
                  "city" -> Select(ident("x"), "city"),
                  "1" ->
                    If(Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), "loc"))),
                      Access(Select(ident("x"), "loc"), jscore.Literal(Js.Num(0, false))),
                      ident("undefined")))))),
              ListMap()),
            $project(
              reshape(
                "city" -> $include(),
                "1"    -> $include()),
              IgnoreId))
        }
    }

    "plan array project with concat (3.2+)" in {
      plan3_2(sqlE"select city, loc[0] from zips") must
        beWorkflow {
          chain[Workflow](
            $read(collection("db", "zips")),
            $project(
              reshape(
                "city" -> $field("city"),
                "1"    ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Arr(List())), $field("loc")),
                      $lt($field("loc"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                    $arrayElemAt($field("loc"), $literal(Bson.Int32(0))),
                    $literal(Bson.Undefined))),
              IgnoreId))
        }
    }

    "plan array concat with filter" in {
      plan(sqlE"""select loc || [ pop ] from zips where city = "BOULDER" """) must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $match(Selector.Doc(
              BsonField.Name("city") -> Selector.Eq(Bson.Text("BOULDER")))),
            $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
              "__tmp4" ->
                If(
                  BinOp(jscore.Or,
                    Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), "loc"))),
                    Call(ident("isString"), List(Select(ident("x"), "loc")))),
                  SpliceArrays(List(
                    jscore.Select(ident("x"), "loc"),
                    jscore.Arr(List(jscore.Select(ident("x"), "pop"))))),
                  ident("undefined")))))),
              ListMap()),
            $project(
              reshape("value" -> $field("__tmp4")),
              ExcludeId))
        }
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ Expr($city -> Eq(Text(BOULDER)))
        |╰─ $SimpleMapF
        |   ├─ Map
        |   │  ╰─ JsCore((Array.isArray(_.loc) || isString(_.loc)) ? (Array.isArray(_.loc) || Array.isArray([_.pop])) ? _.loc.concat([_.pop]) : _.loc + [_.pop] : undefined)
        |   ╰─ Scope(Map())""".stripMargin)

    "plan array flatten" in {
      plan(sqlE"select loc[*] from zips") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $project(
              reshape(
                "__tmp2" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Arr(List())), $field("loc")),
                      $lt($field("loc"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                    $field("loc"),
                    $literal(Bson.Arr(List(Bson.Undefined))))),
              IgnoreId),
            $unwind(DocField(BsonField.Name("__tmp2"))),
            $project(
              reshape("value" -> $field("__tmp2")),
              ExcludeId))
        }
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $ProjectF
        |│  ├─ Name("0" -> {
        |│  │       "$cond": [
        |│  │         {
        |│  │           "$and": [
        |│  │             { "$lte": [{ "$literal": [] }, "$loc"] },
        |│  │             { "$lt": ["$loc", { "$literal": BinData(0, "") }] }]
        |│  │         },
        |│  │         "$loc",
        |│  │         { "$literal": undefined }]
        |│  │     })
        |│  ╰─ IgnoreId
        |├─ $SimpleMapF
        |│  ├─ Flatten
        |│  │  ╰─ JsCore(_["0"])
        |│  ╰─ Scope(Map())
        |╰─ $ProjectF
        |   ├─ Name("value" -> "$0")
        |   ╰─ ExcludeId""".stripMargin)

    "plan array concat" in {
      plan(sqlE"select loc || [ 0, 1, 2 ] from zips") must beWorkflow0 {
        chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(NonEmptyList(
            MapExpr(JsFn(Name("x"),
              Obj(ListMap(
                Name("__tmp4") ->
                  If(
                    BinOp(jscore.Or,
                      Call(Select(ident("Array"), "isArray"), List(Select(ident("x"), "loc"))),
                      Call(ident("isString"), List(Select(ident("x"), "loc")))),
                    SpliceArrays(List(
                      Select(ident("x"), "loc"),
                      Arr(List(
                        jscore.Literal(Js.Num(0, false)),
                        jscore.Literal(Js.Num(1, false)),
                        jscore.Literal(Js.Num(2, false)))))),
                    ident("undefined"))))))),
            ListMap()),
          $project(
            reshape("value" -> $field("__tmp4")),
            ExcludeId))
      }
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |╰─ $ProjectF
        |   ├─ Name("value" -> {
        |   │       "$cond": [
        |   │         {
        |   │           "$or": [
        |   │             {
        |   │               "$and": [
        |   │                 { "$lte": [{ "$literal": [] }, "$loc"] },
        |   │                 { "$lt": ["$loc", { "$literal": BinData(0, "") }] }]
        |   │             },
        |   │             {
        |   │               "$and": [
        |   │                 { "$lte": [{ "$literal": "" }, "$loc"] },
        |   │                 { "$lt": ["$loc", { "$literal": {  } }] }]
        |   │             }]
        |   │         },
        |   │         {
        |   │           "$let": {
        |   │             "vars": {
        |   │               "a1": "$loc",
        |   │               "a2": { "$literal": [NumberInt("0"), NumberInt("1"), NumberInt("2")] }
        |   │             },
        |   │             "in": {
        |   │               "$cond": [
        |   │                 { "$and": [{ "$isArray": "$$a1" }, { "$isArray": "$$a2" }] },
        |   │                 { "$concatArrays": ["$$a1", "$$a2"] },
        |   │                 { "$concat": ["$$a1", "$$a2"] }]
        |   │             }
        |   │           }
        |   │         },
        |   │         { "$literal": undefined }]
        |   │     })
        |   ╰─ ExcludeId""".stripMargin)

    "plan array flatten with unflattened field" in {
      plan(sqlE"SELECT `_id` as zip, loc as loc, loc[*] as coord FROM zips") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $project(
              reshape(
                "__tmp2" ->
                  $cond(
                    $and(
                      $lte($literal(Bson.Arr(List())), $field("loc")),
                      $lt($field("loc"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                    $field("loc"),
                    $literal(Bson.Arr(List(Bson.Undefined)))),
                "__tmp3" -> $$ROOT),
              IgnoreId),
            $unwind(DocField(BsonField.Name("__tmp2"))),
            $project(
              reshape(
                "zip"   -> $field("__tmp3", "_id"),
                "loc"   -> $field("__tmp3", "loc"),
                "coord" -> $field("__tmp2")),
              IgnoreId))
        }
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |╰─ $SimpleMapF
        |   ├─ Map
        |   │  ╰─ Obj
        |   │     ├─ Key(s)
        |   │     │  ╰─ Let(__val)
        |   │     │     ├─ JsCore([_._id, _])
        |   │     │     ╰─ Arr
        |   │     │        ├─ Obj
        |   │     │        │  ╰─ Key(zip: (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1]._id : undefined)
        |   │     │        ├─ Obj
        |   │     │        │  ╰─ Key(loc: (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].loc : undefined)
        |   │     │        ╰─ JsCore([
        |   │     │                  __val[0],
        |   │     │                  (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].loc : undefined])
        |   │     ╰─ Key(f)
        |   │        ╰─ Let(__val)
        |   │           ├─ Let(__val)
        |   │           │  ├─ JsCore([_._id, _])
        |   │           │  ╰─ Arr
        |   │           │     ├─ Obj
        |   │           │     │  ╰─ Key(zip: (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1]._id : undefined)
        |   │           │     ├─ Obj
        |   │           │     │  ╰─ Key(loc: (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].loc : undefined)
        |   │           │     ╰─ JsCore([
        |   │           │               __val[0],
        |   │           │               (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].loc : undefined])
        |   │           ╰─ JsCore(Array.isArray(__val[2][1]) ? __val[2][1] : undefined)
        |   ├─ Flatten
        |   │  ╰─ JsCore(_.f)
        |   ├─ Map
        |   │  ╰─ SpliceObjects
        |   │     ├─ SpliceObjects
        |   │     │  ├─ JsCore(_.s[0])
        |   │     │  ╰─ JsCore(_.s[1])
        |   │     ╰─ Obj
        |   │        ╰─ Key(coord: _.f)
        |   ╰─ Scope(Map())""".stripMargin)

    "unify flattened fields" in {
      plan(sqlE"select loc[*] from zips where loc[*] < 0") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          reshape(
            "__tmp6" ->
              $cond(
                $and(
                  $lte($literal(Bson.Arr(List())), $field("loc")),
                  $lt($field("loc"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                $field("loc"),
                $literal(Bson.Arr(List(Bson.Undefined))))),
          IgnoreId),
        $unwind(DocField(BsonField.Name("__tmp6"))),
        $match(Selector.Doc(
          BsonField.Name("__tmp6") -> Selector.Lt(Bson.Int32(0)))),
        $project(
          reshape("value" -> $field("__tmp6")),
          ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(s: (function (__val) {
        |│  │     │      return [
        |│  │     │        __val[0],
        |│  │     │        __val[1],
        |│  │     │        [
        |│  │     │          __val[0],
        |│  │     │          (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].loc : undefined]]
        |│  │     │    })(
        |│  │     │      [_._id, _]))
        |│  │     ╰─ Key(f: (function (__val) { return Array.isArray(__val[2][1]) ? __val[2][1] : undefined })(
        |│  │            (function (__val) {
        |│  │              return [
        |│  │                __val[0],
        |│  │                __val[1],
        |│  │                [
        |│  │                  __val[0],
        |│  │                  (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].loc : undefined]]
        |│  │            })(
        |│  │              [_._id, _])))
        |│  ├─ Flatten
        |│  │  ╰─ JsCore(_.f)
        |│  ├─ Map
        |│  │  ╰─ JsCore([
        |│  │            (isObject(_.s[1]) && (! Array.isArray(_.s[1]))) ? _.s[1] : undefined,
        |│  │            _.f < 0])
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ Expr($0 -> Eq(Bool(true)))
        |╰─ $SimpleMapF
        |   ├─ Map
        |   │  ╰─ JsCore(Array.isArray(_.src[0].loc) ? _.src[0].loc : undefined)
        |   ├─ Flatten
        |   │  ╰─ Ident(_)
        |   ╰─ Scope(Map())""".stripMargin)

    "group by flattened field" in {
      plan(sqlE"select substring(parents[*].sha, 0, 1), count(*) from slamengine_commits group by substring(parents[*].sha, 0, 1)") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "slamengine_commits")),
        $project(
          reshape(
            "__tmp12" ->
              $cond(
                $and(
                  $lte($literal(Bson.Arr(List())), $field("parents")),
                  $lt($field("parents"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                $field("parents"),
                $literal(Bson.Arr(List(Bson.Undefined))))),
          IgnoreId),
        $unwind(DocField(BsonField.Name("__tmp12"))),
        $group(
          grouped(
            "1" -> $sum($literal(Bson.Int32(1)))),
          -\/(reshape(
            "0" ->
              $cond(
                $and(
                  $lte($literal(Bson.Text("")), $field("__tmp12", "sha")),
                  $lt($field("__tmp12", "sha"), $literal(Bson.Doc()))),
                $substr($field("__tmp12", "sha"), $literal(Bson.Int32(0)), $literal(Bson.Int32(1))),
                $literal(Bson.Undefined))))),
        $project(
          reshape(
            "0" -> $field("_id", "0"),
            "1" -> $include()),
          IgnoreId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; slamengine_commits)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Let(__val)
        |│  │  │  │     ├─ JsCore([_._id, _])
        |│  │  │  │     ╰─ JsCore([
        |│  │  │  │               __val[0],
        |│  │  │  │               __val[1],
        |│  │  │  │               [
        |│  │  │  │                 __val[0],
        |│  │  │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].parents : undefined],
        |│  │  │  │               (Array.isArray(
        |│  │  │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].parents : undefined) && (isObject(__val[1]) && (! Array.isArray(__val[1])))) ? __val[1].parents : undefined])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $ProjectF
        |│  │  │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("3") }] })
        |│  │  │  ╰─ IgnoreId
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Flatten
        |│  │  │  │  ╰─ JsCore(_["0"])
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Obj
        |│  │  │  │     ├─ Key(0: [
        |│  │  │  │     │      isString(_["0"].sha) ? (0 < 0) ? "" : (1 < 0) ? _["0"].sha.substr(0, _["0"].sha.length) : _["0"].sha.substr(0, 1) : undefined])
        |│  │  │  │     ╰─ Key(content: _["0"])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("f0" -> {
        |│  │  │  │          "$first": {
        |│  │  │  │            "$cond": [
        |│  │  │  │              {
        |│  │  │  │                "$and": [
        |│  │  │  │                  { "$lte": [{ "$literal": "" }, "$content.sha"] },
        |│  │  │  │                  { "$lt": ["$content.sha", { "$literal": {  } }] }]
        |│  │  │  │              },
        |│  │  │  │              {
        |│  │  │  │                "$cond": [
        |│  │  │  │                  {
        |│  │  │  │                    "$or": [
        |│  │  │  │                      {
        |│  │  │  │                        "$lt": [
        |│  │  │  │                          { "$literal": NumberInt("0") },
        |│  │  │  │                          { "$literal": NumberInt("0") }]
        |│  │  │  │                      },
        |│  │  │  │                      {
        |│  │  │  │                        "$gt": [
        |│  │  │  │                          { "$literal": NumberInt("0") },
        |│  │  │  │                          { "$strLenCP": "$content.sha" }]
        |│  │  │  │                      }]
        |│  │  │  │                  },
        |│  │  │  │                  { "$literal": "" },
        |│  │  │  │                  {
        |│  │  │  │                    "$cond": [
        |│  │  │  │                      {
        |│  │  │  │                        "$lt": [
        |│  │  │  │                          { "$literal": NumberInt("1") },
        |│  │  │  │                          { "$literal": NumberInt("0") }]
        |│  │  │  │                      },
        |│  │  │  │                      {
        |│  │  │  │                        "$substrCP": [
        |│  │  │  │                          "$content.sha",
        |│  │  │  │                          { "$literal": NumberInt("0") },
        |│  │  │  │                          { "$strLenCP": "$content.sha" }]
        |│  │  │  │                      },
        |│  │  │  │                      {
        |│  │  │  │                        "$substrCP": [
        |│  │  │  │                          "$content.sha",
        |│  │  │  │                          { "$literal": NumberInt("0") },
        |│  │  │  │                          { "$literal": NumberInt("1") }]
        |│  │  │  │                      }]
        |│  │  │  │                  }]
        |│  │  │  │              },
        |│  │  │  │              { "$literal": undefined }]
        |│  │  │  │          }
        |│  │  │  │        })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> "$0")
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ JsCore([[_._id["0"]], _.f0])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$$ROOT" })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> {
        |│  │  │             "$arrayElemAt": [
        |│  │  │               { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("0") }] },
        |│  │  │               { "$literal": NumberInt("0") }]
        |│  │  │           })
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; slamengine_commits)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Let(__val)
        |│     │  │     ├─ JsCore([_._id, _])
        |│     │  │     ╰─ JsCore([
        |│     │  │               __val[0],
        |│     │  │               __val[1],
        |│     │  │               [
        |│     │  │                 __val[0],
        |│     │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].parents : undefined],
        |│     │  │               (Array.isArray(
        |│     │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].parents : undefined) && (isObject(__val[1]) && (! Array.isArray(__val[1])))) ? __val[1].parents : undefined])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $ProjectF
        |│     │  ├─ Name("s" -> "$$ROOT")
        |│     │  ├─ Name("f" -> {
        |│     │  │       "$cond": [
        |│     │  │         {
        |│     │  │           "$and": [
        |│     │  │             {
        |│     │  │               "$lte": [
        |│     │  │                 { "$literal": [] },
        |│     │  │                 {
        |│     │  │                   "$arrayElemAt": [
        |│     │  │                     { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("2") }] },
        |│     │  │                     { "$literal": NumberInt("1") }]
        |│     │  │                 }]
        |│     │  │             },
        |│     │  │             {
        |│     │  │               "$lt": [
        |│     │  │                 {
        |│     │  │                   "$arrayElemAt": [
        |│     │  │                     { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("2") }] },
        |│     │  │                     { "$literal": NumberInt("1") }]
        |│     │  │                 },
        |│     │  │                 { "$literal": BinData(0, "") }]
        |│     │  │             }]
        |│     │  │         },
        |│     │  │         {
        |│     │  │           "$arrayElemAt": [
        |│     │  │             { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("2") }] },
        |│     │  │             { "$literal": NumberInt("1") }]
        |│     │  │         },
        |│     │  │         { "$literal": undefined }]
        |│     │  │     })
        |│     │  ╰─ IgnoreId
        |│     ├─ $SimpleMapF
        |│     │  ├─ Flatten
        |│     │  │  ╰─ JsCore(_.f)
        |│     │  ├─ Map
        |│     │  │  ╰─ JsCore([
        |│     │  │            (isObject(_.s[1]) && (! Array.isArray(_.s[1]))) ? _.s[1] : undefined,
        |│     │  │            [
        |│     │  │              isString(_.f.sha) ? (0 < 0) ? "" : (1 < 0) ? _.f.sha.substr(0, _.f.sha.length) : _.f.sha.substr(0, 1) : undefined]])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $GroupF
        |│     │  ├─ Grouped
        |│     │  │  ╰─ Name("f0" -> { "$sum": { "$literal": NumberInt("1") } })
        |│     │  ╰─ By
        |│     │     ╰─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ JsCore([[_._id["0"]], _.f0])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) { return [{ "0": value[0][0] }, { "left": [], "right": [value] }] })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |╰─ $ProjectF
        |   ├─ Name("0" -> { "$arrayElemAt": ["$left", { "$literal": NumberInt("1") }] })
        |   ├─ Name("1" -> { "$arrayElemAt": ["$right", { "$literal": NumberInt("1") }] })
        |   ╰─ IgnoreId""".stripMargin)

    "unify flattened fields with unflattened field" in {
      plan(sqlE"select `_id` as zip, loc[*] from zips order by loc[*]") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          reshape(
            "__tmp2" ->
              $cond(
                $and(
                  $lte($literal(Bson.Arr(List())), $field("loc")),
                  $lt($field("loc"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                $field("loc"),
                $literal(Bson.Arr(List(Bson.Undefined)))),
            "__tmp3" -> $$ROOT),
          IgnoreId),
        $unwind(DocField(BsonField.Name("__tmp2"))),
        $project(
          reshape(
            "zip" -> $field("__tmp3", "_id"),
            "loc" -> $field("__tmp2")),
          IgnoreId),
        $sort(NonEmptyList(BsonField.Name("loc") -> SortDir.Ascending))))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(s: (function (__val) {
        |│  │     │      return [
        |│  │     │        __val[0],
        |│  │     │        __val[1],
        |│  │     │        [
        |│  │     │          __val[0],
        |│  │     │          (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].loc : undefined]]
        |│  │     │    })(
        |│  │     │      [_._id, _]))
        |│  │     ╰─ Key(f: (function (__val) { return Array.isArray(__val[2][1]) ? __val[2][1] : undefined })(
        |│  │            (function (__val) {
        |│  │              return [
        |│  │                __val[0],
        |│  │                __val[1],
        |│  │                [
        |│  │                  __val[0],
        |│  │                  (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].loc : undefined]]
        |│  │            })(
        |│  │              [_._id, _])))
        |│  ├─ Flatten
        |│  │  ╰─ JsCore(_.f)
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ Arr
        |│  │     │  ├─ Obj
        |│  │     │  │  ├─ Key(zip: (isObject(_.s[1]) && (! Array.isArray(_.s[1]))) ? _.s[1]._id : undefined)
        |│  │     │  │  ╰─ Key(loc: _.f)
        |│  │     │  ╰─ JsCore(_.f)
        |│  │     ╰─ JsCore([__val[0], __val])
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> {
        |│  │       "$arrayElemAt": [
        |│  │         { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] },
        |│  │         { "$literal": NumberInt("1") }]
        |│  │     })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $SortF
        |│  ╰─ SortKey(0 -> Ascending)
        |╰─ $ProjectF
        |   ├─ Name("value" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ╰─ ExcludeId""".stripMargin)

    "unify flattened with double-flattened" in {
      plan(sqlE"""select * from user_comments where (comments[*].id LIKE "%Dr%" OR comments[*].replyTo[*] LIKE "%Dr%")""") must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "user_comments")),
        $project(
          reshape(
            "__tmp14" ->
              $cond(
                $and(
                  $lte($literal(Bson.Arr(List())), $field("comments")),
                  $lt($field("comments"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                $field("comments"),
                $literal(Bson.Arr(List(Bson.Undefined)))),
            "__tmp15" -> $$ROOT),
          IgnoreId),
        $unwind(DocField(BsonField.Name("__tmp14"))),
        $project(
          reshape(
            "__tmp18" ->
              $cond(
                $and(
                  $lte($literal(Bson.Arr(List())), $field("__tmp14", "replyTo")),
                  $lt($field("__tmp14", "replyTo"), $literal(Bson.Binary.fromArray(scala.Array[Byte]())))),
                $field("__tmp14", "replyTo"),
                $literal(Bson.Arr(List(Bson.Undefined)))),
            "__tmp19" -> $$ROOT),
          IgnoreId),
        $unwind(DocField(BsonField.Name("__tmp18"))),
        $match(Selector.Or(
          Selector.And(
            Selector.Doc(
              BsonField.Name("__tmp19") \ BsonField.Name("__tmp14") \ BsonField.Name("id") -> Selector.Type(BsonType.Text)),
            Selector.Doc(
              BsonField.Name("__tmp19") \ BsonField.Name("__tmp14") \ BsonField.Name("id") -> Selector.Regex("^.*Dr.*$", false, true, false, false))),
          Selector.Doc(
            BsonField.Name("__tmp18") -> Selector.Regex("^.*Dr.*$", false, true, false, false)))),
        $project(
          reshape("value" -> $field("__tmp19", "__tmp15")),
          ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; user_comments)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Let(__val)
        |│  │  │  │     ├─ JsCore([_._id, _])
        |│  │  │  │     ╰─ JsCore([
        |│  │  │  │               __val[0],
        |│  │  │  │               __val[1],
        |│  │  │  │               [
        |│  │  │  │                 __val[0],
        |│  │  │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].comments : undefined]])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$$ROOT" })
        |│  │  │  ╰─ By({ "$literal": null })
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; user_comments)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Let(__val)
        |│     │  │     ├─ JsCore([_._id, _])
        |│     │  │     ╰─ JsCore([
        |│     │  │               __val[0],
        |│     │  │               __val[1],
        |│     │  │               [
        |│     │  │                 __val[0],
        |│     │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].comments : undefined]])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $ProjectF
        |│     │  ├─ Name("s" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("2") }] })
        |│     │  ├─ Name("f" -> {
        |│     │  │       "$cond": [
        |│     │  │         {
        |│     │  │           "$and": [
        |│     │  │             {
        |│     │  │               "$lte": [
        |│     │  │                 { "$literal": [] },
        |│     │  │                 {
        |│     │  │                   "$arrayElemAt": [
        |│     │  │                     { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("2") }] },
        |│     │  │                     { "$literal": NumberInt("1") }]
        |│     │  │                 }]
        |│     │  │             },
        |│     │  │             {
        |│     │  │               "$lt": [
        |│     │  │                 {
        |│     │  │                   "$arrayElemAt": [
        |│     │  │                     { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("2") }] },
        |│     │  │                     { "$literal": NumberInt("1") }]
        |│     │  │                 },
        |│     │  │                 { "$literal": BinData(0, "") }]
        |│     │  │             }]
        |│     │  │         },
        |│     │  │         {
        |│     │  │           "$arrayElemAt": [
        |│     │  │             { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("2") }] },
        |│     │  │             { "$literal": NumberInt("1") }]
        |│     │  │         },
        |│     │  │         { "$literal": undefined }]
        |│     │  │     })
        |│     │  ╰─ IgnoreId
        |│     ├─ $SimpleMapF
        |│     │  ├─ SubMap
        |│     │  │  ├─ JsCore(_.f)
        |│     │  │  ╰─ Let(m)
        |│     │  │     ├─ JsCore(_.f)
        |│     │  │     ╰─ Call
        |│     │  │        ├─ JsCore(Object.keys(m).map)
        |│     │  │        ╰─ Fun(Name(k))
        |│     │  │           ╰─ JsCore([k, m[k]])
        |│     │  ├─ Flatten
        |│     │  │  ╰─ JsCore(_.f)
        |│     │  ├─ Map
        |│     │  │  ╰─ JsCore([
        |│     │  │            _.f[1].id,
        |│     │  │            (new RegExp("^.*Dr.*$", "m")).test(_.f[1].id),
        |│     │  │            [_.f[0], _.s[0], _.f[1].replyTo]])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $ProjectF
        |│     │  ├─ Name("s" -> "$$ROOT")
        |│     │  ├─ Name("f" -> {
        |│     │  │       "$cond": [
        |│     │  │         {
        |│     │  │           "$and": [
        |│     │  │             {
        |│     │  │               "$lte": [
        |│     │  │                 { "$literal": [] },
        |│     │  │                 {
        |│     │  │                   "$arrayElemAt": [
        |│     │  │                     { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("2") }] },
        |│     │  │                     { "$literal": NumberInt("2") }]
        |│     │  │                 }]
        |│     │  │             },
        |│     │  │             {
        |│     │  │               "$lt": [
        |│     │  │                 {
        |│     │  │                   "$arrayElemAt": [
        |│     │  │                     { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("2") }] },
        |│     │  │                     { "$literal": NumberInt("2") }]
        |│     │  │                 },
        |│     │  │                 { "$literal": BinData(0, "") }]
        |│     │  │             }]
        |│     │  │         },
        |│     │  │         {
        |│     │  │           "$arrayElemAt": [
        |│     │  │             { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("2") }] },
        |│     │  │             { "$literal": NumberInt("2") }]
        |│     │  │         },
        |│     │  │         { "$literal": undefined }]
        |│     │  │     })
        |│     │  ╰─ IgnoreId
        |│     ├─ $SimpleMapF
        |│     │  ├─ Flatten
        |│     │  │  ╰─ JsCore(_.f)
        |│     │  ├─ Map
        |│     │  │  ╰─ JsCore([
        |│     │  │            isString(_.s[0]) ? _.s[1] : undefined,
        |│     │  │            (new RegExp("^.*Dr.*$", "m")).test(_.f)])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) { return [null, { "left": [], "right": [value] }] })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ JsCore([_.left, _.right])
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> { "$literal": true })
        |│  ├─ Name("1" -> {
        |│  │       "$arrayElemAt": [
        |│  │         { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] },
        |│  │         { "$literal": NumberInt("0") }]
        |│  │     })
        |│  ├─ Name("2" -> {
        |│  │       "$arrayElemAt": [
        |│  │         { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] },
        |│  │         { "$literal": NumberInt("1") }]
        |│  │     })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $MatchF
        |│  ╰─ And
        |│     ├─ Doc
        |│     │  ╰─ Expr($0 -> Eq(Bool(true)))
        |│     ╰─ Or
        |│        ├─ Doc
        |│        │  ╰─ Expr($1 -> Eq(Bool(true)))
        |│        ╰─ Doc
        |│           ╰─ Expr($2 -> Eq(Bool(true)))
        |╰─ $ProjectF
        |   ├─ Name("value" -> {
        |   │       "$cond": [
        |   │         {
        |   │           "$and": [
        |   │             {
        |   │               "$lte": [
        |   │                 { "$literal": {  } },
        |   │                 {
        |   │                   "$arrayElemAt": [
        |   │                     { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] },
        |   │                     { "$literal": NumberInt("1") }]
        |   │                 }]
        |   │             },
        |   │             {
        |   │               "$lt": [
        |   │                 {
        |   │                   "$arrayElemAt": [
        |   │                     { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] },
        |   │                     { "$literal": NumberInt("1") }]
        |   │                 },
        |   │                 { "$literal": [] }]
        |   │             }]
        |   │         },
        |   │         {
        |   │           "$arrayElemAt": [
        |   │             { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] },
        |   │             { "$literal": NumberInt("1") }]
        |   │         },
        |   │         { "$literal": undefined }]
        |   │     })
        |   ╰─ ExcludeId""".stripMargin)

    "plan limit with offset" in {
      plan(sqlE"SELECT * FROM zips OFFSET 100 LIMIT 5") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $limit(105),
          $skip(100)))
    }

    "plan sort and limit" in {
      plan(sqlE"SELECT city, pop FROM zips ORDER BY pop DESC LIMIT 5") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $project(
              reshape(
                "city" -> $field("city"),
                "pop"  -> $field("pop")),
              IgnoreId),
            $sort(NonEmptyList(BsonField.Name("pop") -> SortDir.Descending)),
            $limit(5))
        }
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ Arr
        |│  │     │  ├─ Obj
        |│  │     │  │  ├─ Key(city: (isObject(_) && (! Array.isArray(_))) ? _.city : undefined)
        |│  │     │  │  ╰─ Key(pop: (isObject(_) && (! Array.isArray(_))) ? _.pop : undefined)
        |│  │     │  ╰─ Ident(_)
        |│  │     ╰─ Obj
        |│  │        ├─ Key(0: (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].pop : undefined)
        |│  │        ╰─ Key(src: __val)
        |│  ╰─ Scope(Map())
        |├─ $SortF
        |│  ╰─ SortKey(0 -> Descending)
        |├─ $LimitF(5)
        |╰─ $ProjectF
        |   ├─ Name("value" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ╰─ ExcludeId""".stripMargin)

    "plan simple single field selection and limit" in {
      plan(sqlE"SELECT city FROM zips LIMIT 5") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $limit(5),
            $project(
              reshape("value" -> $field("city")),
              ExcludeId))
        }
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $LimitF(5)
        |╰─ $ProjectF
        |   ├─ Name("value" -> {
        |   │       "$cond": [
        |   │         {
        |   │           "$and": [
        |   │             { "$lte": [{ "$literal": {  } }, "$$ROOT"] },
        |   │             { "$lt": ["$$ROOT", { "$literal": [] }] }]
        |   │         },
        |   │         "$city",
        |   │         { "$literal": undefined }]
        |   │     })
        |   ╰─ ExcludeId""".stripMargin)

    "plan complex group by with sorting and limiting" in {
      plan(sqlE"SELECT city, SUM(pop) AS pop FROM zips GROUP BY city ORDER BY pop") must
        beWorkflow0 {
          chain[Workflow](
            $read(collection("db", "zips")),
            $group(
              grouped(
                "pop"  ->
                  $sum(
                    $cond(
                      $and(
                        $lt($literal(Bson.Null), $field("pop")),
                        $lt($field("pop"), $literal(Bson.Text("")))),
                      $field("pop"),
                      $literal(Bson.Undefined)))),
              -\/(reshape("0" -> $field("city")))),
            $project(
              reshape(
                "city" -> $field("_id", "0"),
                "pop"  -> $include()),
              IgnoreId),
            $sort(NonEmptyList(BsonField.Name("pop") -> SortDir.Ascending)))
        }
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(0: [_.city])
        |│  │     ╰─ Key(content: _)
        |│  ╰─ Scope(Map())
        |├─ $GroupF
        |│  ├─ Grouped
        |│  │  ├─ Name("f0" -> { "$first": "$content.city" })
        |│  │  ╰─ Name("f1" -> {
        |│  │          "$sum": {
        |│  │            "$cond": [
        |│  │              {
        |│  │                "$and": [
        |│  │                  { "$lt": [{ "$literal": null }, "$content.pop"] },
        |│  │                  { "$lt": ["$content.pop", { "$literal": "" }] }]
        |│  │              },
        |│  │              "$content.pop",
        |│  │              { "$literal": undefined }]
        |│  │          }
        |│  │        })
        |│  ╰─ By
        |│     ╰─ Name("0" -> "$0")
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ Arr
        |│  │     │  ├─ JsCore(_._id["0"])
        |│  │     │  ├─ Obj
        |│  │     │  │  ├─ Key(city: _.f0)
        |│  │     │  │  ╰─ Key(pop: _.f1)
        |│  │     │  ╰─ JsCore(_.f1)
        |│  │     ╰─ JsCore([__val[1], __val])
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> {
        |│  │       "$arrayElemAt": [
        |│  │         { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] },
        |│  │         { "$literal": NumberInt("2") }]
        |│  │     })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $SortF
        |│  ╰─ SortKey(0 -> Ascending)
        |╰─ $ProjectF
        |   ├─ Name("value" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ╰─ ExcludeId""".stripMargin)

    "plan filter and expressions with IS NULL" in {
      plan(sqlE"select foo is null from zips where foo is null") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.Doc(
            BsonField.Name("foo") -> Selector.Eq(Bson.Null))),
          $project(
            reshape("value" -> $eq($field("foo"), $literal(Bson.Null))),
            ExcludeId)))
    }

    "plan implicit group by with filter" in {
      plan(sqlE"""select avg(pop), min(city) from zips where state = "CO" """) must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.Doc(
            BsonField.Name("state") -> Selector.Eq(Bson.Text("CO")))),
          $group(
            grouped(
              "0" ->
                $avg(
                  $cond(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $field("pop"),
                    $literal(Bson.Undefined))),
              "1" ->
                $min(
                  $cond($or(
                    $and(
                      $lt($literal(Bson.Null), $field("city")),
                      $lt($field("city"), $literal(Bson.Doc()))),
                    $and(
                      $lte($literal(Bson.Bool(false)), $field("city")),
                      $lt($field("city"), $literal(Bson.Regex("", ""))))),
                    $field("city"),
                    $literal(Bson.Undefined)))),
            \/-($literal(Bson.Null)))))
    }

    "plan simple distinct" in {
      plan(sqlE"select distinct city, state from zips") must
      beWorkflow0(
        chain[Workflow](
          $read(collection("db", "zips")),
          $group(
            grouped(),
            -\/(reshape(
              "0" -> $field("city"),
              "1" -> $field("state")))),
          $project(
            reshape(
              "city"  -> $field("_id", "0"),
              "state" -> $field("_id", "1")),
            IgnoreId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(f0)
        |│  │     │  ╰─ Obj
        |│  │     │     ├─ Key(city: _.city)
        |│  │     │     ╰─ Key(state: _.state)
        |│  │     ╰─ Key(b0)
        |│  │        ╰─ Obj
        |│  │           ├─ Key(city: _.city)
        |│  │           ╰─ Key(state: _.state)
        |│  ╰─ Scope(Map())
        |├─ $GroupF
        |│  ├─ Grouped
        |│  │  ╰─ Name("f0" -> { "$first": "$f0" })
        |│  ╰─ By
        |│     ╰─ Name("0" -> "$b0")
        |╰─ $ProjectF
        |   ├─ Name("value" -> "$f0")
        |   ╰─ ExcludeId""".stripMargin)

    "plan distinct as expression" in {
      plan(sqlE"select count(distinct(city)) from zips") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $group(
            grouped(),
            -\/(reshape("0" -> $field("city")))),
          $group(
            grouped("__tmp2" -> $sum($literal(Bson.Int32(1)))),
            \/-($literal(Bson.Null))),
          $project(
            reshape("value" -> $field("__tmp2")),
            ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $GroupF
        |│  ├─ Grouped
        |│  ╰─ By
        |│     ╰─ Name("0" -> "$city")
        |├─ $ProjectF
        |│  ├─ Name("f0" -> "$_id.0")
        |│  ╰─ IgnoreId
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ JsCore([[_._id["0"]], _.f0])
        |│  ╰─ Scope(Map())
        |├─ $GroupF
        |│  ├─ Grouped
        |│  │  ╰─ Name("f0" -> { "$sum": { "$literal": NumberInt("1") } })
        |│  ╰─ By({ "$literal": null })
        |╰─ $ProjectF
        |   ├─ Name("value" -> "$f0")
        |   ╰─ ExcludeId""".stripMargin)

    "plan distinct of expression as expression" in {
      plan(sqlE"select count(distinct substring(city, 0, 1)) from zips") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $group(
            grouped(),
            -\/(reshape(
              "0" ->
                $cond(
                  $and(
                    $lte($literal(Bson.Text("")), $field("city")),
                    $lt($field("city"), $literal(Bson.Doc()))),
                  $substr(
                    $field("city"),
                    $literal(Bson.Int32(0)),
                    $literal(Bson.Int32(1))),
                  $literal(Bson.Undefined))))),
          $group(
            grouped("__tmp8" -> $sum($literal(Bson.Int32(1)))),
            \/-($literal(Bson.Null))),
          $project(
            reshape("value" -> $field("__tmp8")),
            ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $GroupF
        |│  ├─ Grouped
        |│  ╰─ By
        |│     ╰─ Name("0" -> {
        |│             "$cond": [
        |│               {
        |│                 "$and": [
        |│                   { "$lte": [{ "$literal": "" }, "$city"] },
        |│                   { "$lt": ["$city", { "$literal": {  } }] }]
        |│               },
        |│               {
        |│                 "$cond": [
        |│                   {
        |│                     "$or": [
        |│                       {
        |│                         "$lt": [
        |│                           { "$literal": NumberInt("0") },
        |│                           { "$literal": NumberInt("0") }]
        |│                       },
        |│                       {
        |│                         "$gt": [{ "$literal": NumberInt("0") }, { "$strLenCP": "$city" }]
        |│                       }]
        |│                   },
        |│                   { "$literal": "" },
        |│                   {
        |│                     "$cond": [
        |│                       {
        |│                         "$lt": [
        |│                           { "$literal": NumberInt("1") },
        |│                           { "$literal": NumberInt("0") }]
        |│                       },
        |│                       {
        |│                         "$substrCP": [
        |│                           "$city",
        |│                           { "$literal": NumberInt("0") },
        |│                           { "$strLenCP": "$city" }]
        |│                       },
        |│                       {
        |│                         "$substrCP": [
        |│                           "$city",
        |│                           { "$literal": NumberInt("0") },
        |│                           { "$literal": NumberInt("1") }]
        |│                       }]
        |│                   }]
        |│               },
        |│               { "$literal": undefined }]
        |│           })
        |├─ $ProjectF
        |│  ├─ Name("f0" -> "$_id.0")
        |│  ╰─ IgnoreId
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ JsCore([[_._id["0"]], _.f0])
        |│  ╰─ Scope(Map())
        |├─ $GroupF
        |│  ├─ Grouped
        |│  │  ╰─ Name("f0" -> { "$sum": { "$literal": NumberInt("1") } })
        |│  ╰─ By({ "$literal": null })
        |╰─ $ProjectF
        |   ├─ Name("value" -> "$f0")
        |   ╰─ ExcludeId""".stripMargin)

    "plan distinct of wildcard" in {
      plan(sqlE"select distinct * from zips") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
            obj(
              "__tmp1" ->
                Call(ident("remove"),
                  List(ident("x"), jscore.Literal(Js.Str("_id")))))))),
            ListMap()),
          $group(
            grouped(),
            -\/(reshape("0" -> $field("__tmp1")))),
          $project(
            reshape("value" -> $field("_id", "0")),
            ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ JsCore(remove(_, "_id"))
        |│  ╰─ Scope(Map())
        |├─ $GroupF
        |│  ├─ Grouped
        |│  ╰─ By
        |│     ╰─ Name("0" -> "$$ROOT")
        |╰─ $ProjectF
        |   ├─ Name("value" -> "$_id.0")
        |   ╰─ ExcludeId""".stripMargin)

    "plan distinct of wildcard as expression" in {
      plan(sqlE"select count(distinct *) from zips") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
            obj(
              "__tmp4" ->
                Call(ident("remove"),
                  List(ident("x"), jscore.Literal(Js.Str("_id")))))))),
            ListMap()),
          $group(
            grouped(),
            -\/(reshape("0" -> $field("__tmp4")))),
          $group(
            grouped("__tmp6" -> $sum($literal(Bson.Int32(1)))),
            \/-($literal(Bson.Null))),
          $project(
            reshape("value" -> $field("__tmp6")),
            ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ JsCore(remove(_, "_id"))
        |│  ╰─ Scope(Map())
        |├─ $GroupF
        |│  ├─ Grouped
        |│  ╰─ By
        |│     ╰─ Name("0" -> "$$ROOT")
        |├─ $ProjectF
        |│  ├─ Name("f0" -> "$_id.0")
        |│  ╰─ IgnoreId
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ JsCore([[_._id["0"]], _.f0])
        |│  ╰─ Scope(Map())
        |├─ $GroupF
        |│  ├─ Grouped
        |│  │  ╰─ Name("f0" -> { "$sum": { "$literal": NumberInt("1") } })
        |│  ╰─ By({ "$literal": null })
        |╰─ $ProjectF
        |   ├─ Name("value" -> "$f0")
        |   ╰─ ExcludeId""".stripMargin)

    "plan distinct with simple order by" in {
      plan(sqlE"select distinct city from zips order by city") must
        beWorkflow0(
          chain[Workflow](
            $read(collection("db", "zips")),
            $project(
              reshape("city" -> $field("city")),
              IgnoreId),
            $sort(NonEmptyList(BsonField.Name("city") -> SortDir.Ascending)),
            $group(
              grouped(),
              -\/(reshape("0" -> $field("city")))),
            $project(
              reshape("city" -> $field("_id", "0")),
              IgnoreId),
            $sort(NonEmptyList(BsonField.Name("city") -> SortDir.Ascending))))
    }.pendingWithActual("#1803",
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $GroupF
        |│  ├─ Grouped
        |│  ╰─ By
        |│     ╰─ Name("0" -> "$city")
        |├─ $ProjectF
        |│  ├─ Name("f0" -> "$_id.0")
        |│  ╰─ IgnoreId
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ JsCore([_._id["0"], _.f0])
        |│  │     ╰─ JsCore([__val[1], __val])
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> {
        |│  │       "$arrayElemAt": [
        |│  │         { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] },
        |│  │         { "$literal": NumberInt("1") }]
        |│  │     })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $SortF
        |│  ╰─ SortKey(0 -> Ascending)
        |╰─ $ProjectF
        |   ├─ Name("value" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ╰─ ExcludeId""".stripMargin)

    "plan distinct with unrelated order by" in {
      plan(sqlE"select distinct city from zips order by pop desc") must
        beWorkflow0(
          chain[Workflow](
            $read(collection("db", "zips")),
            $project(
              reshape(
                "__sd__0" -> $field("pop"),
                "city"    -> $field("city")),
              IgnoreId),
            $sort(NonEmptyList(
              BsonField.Name("__sd__0") -> SortDir.Descending)),
            $group(
              grouped("__tmp2" -> $first($$ROOT)),
              -\/(reshape("city" -> $field("city")))),
            $project(
              reshape(
                "city" -> $field("__tmp2", "city"),
                "__tmp0" -> $field("__tmp2", "__sd__0")),
              IgnoreId),
            $sort(NonEmptyList(
              BsonField.Name("__tmp0") -> SortDir.Descending)),
            $project(
              reshape("city" -> $field("city")),
              ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Arr
        |│  │     ├─ Obj
        |│  │     │  ├─ Key(city: _.city)
        |│  │     │  ╰─ Key(__sd__0: _.pop)
        |│  │     ╰─ JsCore(_.pop)
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $SortF
        |│  ╰─ SortKey(0 -> Descending)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(0: remove(_.src[0], "__sd__0"))
        |│  │     ╰─ Key(content: _.src)
        |│  ╰─ Scope(Map())
        |├─ $GroupF
        |│  ├─ Grouped
        |│  │  ╰─ Name("f0" -> { "$first": { "$arrayElemAt": ["$content", { "$literal": NumberInt("0") }] } })
        |│  ╰─ By
        |│     ╰─ Name("0" -> "$0")
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ JsCore([_._id["0"], _.f0, _.f0.__sd__0])
        |│  │     ╰─ JsCore([remove(__val[1], "__sd__0"), __val])
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> {
        |│  │       "$arrayElemAt": [
        |│  │         { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] },
        |│  │         { "$literal": NumberInt("2") }]
        |│  │     })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $SortF
        |│  ╰─ SortKey(0 -> Descending)
        |╰─ $ProjectF
        |   ├─ Name("value" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ╰─ ExcludeId""".stripMargin)

    "plan distinct as function with group" in {
      plan(sqlE"select state, count(distinct(city)) from zips group by state") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $group(
            grouped("__tmp0" -> $first($$ROOT)),
            -\/(reshape("0" -> $field("city")))),
          $group(
            grouped(
              "state" -> $first($field("__tmp0", "state")),
              "1"     -> $sum($literal(Bson.Int32(1)))),
            \/-($literal(Bson.Null)))))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; zips)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Obj
        |│  │  │  │     ├─ Key(0: [(isObject(_) && (! Array.isArray(_))) ? _.state : undefined])
        |│  │  │  │     ╰─ Key(content: _)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("f0" -> {
        |│  │  │  │          "$first": {
        |│  │  │  │            "$cond": [
        |│  │  │  │              {
        |│  │  │  │                "$and": [
        |│  │  │  │                  { "$lte": [{ "$literal": {  } }, "$content"] },
        |│  │  │  │                  { "$lt": ["$content", { "$literal": [] }] }]
        |│  │  │  │              },
        |│  │  │  │              "$content.state",
        |│  │  │  │              { "$literal": undefined }]
        |│  │  │  │          }
        |│  │  │  │        })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> "$0")
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ JsCore([[_._id["0"]], _.f0])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$$ROOT" })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> {
        |│  │  │             "$arrayElemAt": [
        |│  │  │               { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("0") }] },
        |│  │  │               { "$literal": NumberInt("0") }]
        |│  │  │           })
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; zips)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Obj
        |│     │  │     ├─ Key(0: (isObject(_) && (! Array.isArray(_))) ? _.city : undefined)
        |│     │  │     ├─ Key(1: [(isObject(_) && (! Array.isArray(_))) ? _.state : undefined])
        |│     │  │     ╰─ Key(content: _)
        |│     │  ╰─ Scope(Map())
        |│     ├─ $GroupF
        |│     │  ├─ Grouped
        |│     │  │  ╰─ Name("f0" -> {
        |│     │  │          "$first": {
        |│     │  │            "$cond": [
        |│     │  │              {
        |│     │  │                "$and": [
        |│     │  │                  { "$lte": [{ "$literal": {  } }, "$content"] },
        |│     │  │                  { "$lt": ["$content", { "$literal": [] }] }]
        |│     │  │              },
        |│     │  │              "$content.city",
        |│     │  │              { "$literal": undefined }]
        |│     │  │          }
        |│     │  │        })
        |│     │  ╰─ By
        |│     │     ├─ Name("0" -> "$0")
        |│     │     ╰─ Name("1" -> "$1")
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ JsCore([[_._id["0"], _._id["1"]], _.f0])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $GroupF
        |│     │  ├─ Grouped
        |│     │  │  ╰─ Name("f0" -> { "$sum": { "$literal": NumberInt("1") } })
        |│     │  ╰─ By
        |│     │     ╰─ Name("0" -> {
        |│     │             "$arrayElemAt": [
        |│     │               { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("0") }] },
        |│     │               { "$literal": NumberInt("1") }]
        |│     │           })
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ JsCore([[_._id["0"]], _.f0])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) { return [{ "0": value[0][0] }, { "left": [], "right": [value] }] })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |╰─ $ProjectF
        |   ├─ Name("state" -> { "$arrayElemAt": ["$left", { "$literal": NumberInt("1") }] })
        |   ├─ Name("1" -> { "$arrayElemAt": ["$right", { "$literal": NumberInt("1") }] })
        |   ╰─ IgnoreId""".stripMargin)

    "plan distinct with sum and group" in {
      plan(sqlE"SELECT DISTINCT SUM(pop) AS totalPop, city, state FROM zips GROUP BY city") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $group(
            grouped(
              "totalPop" ->
                $sum(
                  $cond(
                    $and(
                      $lt($literal(Bson.Null), $field("pop")),
                      $lt($field("pop"), $literal(Bson.Text("")))),
                    $field("pop"),
                    $literal(Bson.Undefined))),
              "state"    -> $push($field("state"))),
            -\/(reshape("0" -> $field("city")))),
          $project(
            reshape(
              "totalPop" -> $include(),
              "city"     -> $field("_id", "0"),
              "state"    -> $include()),
            IgnoreId),
          $unwind(DocField("state")),
          $group(
            grouped(),
            -\/(reshape(
              "0" -> $field("totalPop"),
              "1" -> $field("city"),
              "2" -> $field("state")))),
          $project(
            reshape(
              "totalPop" -> $field("_id", "0"),
              "city"     -> $field("_id", "1"),
              "state"    -> $field("_id", "2")),
            IgnoreId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; zips)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Let(__val)
        |│  │  │  │     ├─ Let(__val)
        |│  │  │  │     │  ├─ JsCore([_._id, _])
        |│  │  │  │     │  ╰─ JsCore([
        |│  │  │  │     │            __val[0],
        |│  │  │  │     │            (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1] : undefined,
        |│  │  │  │     │            [
        |│  │  │  │     │              (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].city : undefined],
        |│  │  │  │     │            __val[1]])
        |│  │  │  │     ╰─ Obj
        |│  │  │  │        ├─ Key(f0: ((isNumber(
        |│  │  │  │        │      (isObject(__val[3]) && (! Array.isArray(__val[3]))) ? __val[3].pop : undefined) || ((((isObject(__val[3]) && (! Array.isArray(__val[3]))) ? __val[3].pop : undefined) instanceof NumberInt) || (((isObject(__val[3]) && (! Array.isArray(__val[3]))) ? __val[3].pop : undefined) instanceof NumberLong))) && (isObject(__val[3]) && (! Array.isArray(__val[3])))) ? __val[3].pop : undefined)
        |│  │  │  │        ├─ Key(f1: (isObject(__val[3]) && (! Array.isArray(__val[3]))) ? __val[3].city : undefined)
        |│  │  │  │        ╰─ Key(b0: [
        |│  │  │  │               (isObject(__val[3]) && (! Array.isArray(__val[3]))) ? __val[3].city : undefined])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ├─ Name("f0" -> { "$sum": "$f0" })
        |│  │  │  │  ╰─ Name("f1" -> { "$first": "$f1" })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> "$b0")
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Arr
        |│  │  │  │     ├─ JsCore(_._id["0"])
        |│  │  │  │     ├─ Obj
        |│  │  │  │     │  ╰─ Key(totalPop: _.f0)
        |│  │  │  │     ╰─ Obj
        |│  │  │  │        ╰─ Key(city: _.f1)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$$ROOT" })
        |│  │  │  ╰─ By({ "$literal": null })
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; zips)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Let(__val)
        |│     │  │     ├─ JsCore([_._id, _])
        |│     │  │     ╰─ JsCore([
        |│     │  │               __val[0],
        |│     │  │               (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1] : undefined,
        |│     │  │               [
        |│     │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].city : undefined],
        |│     │  │               __val[1]])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) { return [null, { "left": [], "right": [value] }] })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ JsCore([_.left, _.right])
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> { "$literal": true })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ Expr($0 -> Eq(Bool(true)))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(f0)
        |│  │     │  ╰─ SpliceObjects
        |│  │     │     ├─ SpliceObjects
        |│  │     │     │  ├─ JsCore(_.src[0][1])
        |│  │     │     │  ╰─ JsCore(_.src[0][2])
        |│  │     │     ╰─ Obj
        |│  │     │        ╰─ Key(state: _.src[1][1].state)
        |│  │     ╰─ Key(b0)
        |│  │        ╰─ SpliceObjects
        |│  │           ├─ SpliceObjects
        |│  │           │  ├─ JsCore(_.src[0][1])
        |│  │           │  ╰─ JsCore(_.src[0][2])
        |│  │           ╰─ Obj
        |│  │              ╰─ Key(state: _.src[1][1].state)
        |│  ╰─ Scope(Map())
        |├─ $GroupF
        |│  ├─ Grouped
        |│  │  ╰─ Name("f0" -> { "$first": "$f0" })
        |│  ╰─ By
        |│     ╰─ Name("0" -> "$b0")
        |╰─ $ProjectF
        |   ├─ Name("value" -> "$f0")
        |   ╰─ ExcludeId""".stripMargin)

    "plan distinct with sum, group, and orderBy" in {
      plan(sqlE"SELECT DISTINCT SUM(pop) AS totalPop, city, state FROM zips GROUP BY city ORDER BY totalPop DESC") must
        beWorkflow0(
          chain[Workflow](
            $read(collection("db", "zips")),
            $group(
              grouped(
                "totalPop" ->
                  $sum(
                    $cond(
                      $and(
                        $lt($literal(Bson.Null), $field("pop")),
                        $lt($field("pop"), $literal(Bson.Text("")))),
                      $field("pop"),
                      $literal(Bson.Undefined))),
                "state"    -> $push($field("state"))),
              -\/(reshape("0" -> $field("city")))),
            $project(
              reshape(
                "totalPop" -> $include(),
                "city"     -> $field("_id", "0"),
                "state"    -> $include()),
              IgnoreId),
            $unwind(DocField("state")),
            $sort(NonEmptyList(BsonField.Name("totalPop") -> SortDir.Descending)),
            $group(
              grouped(),
              -\/(reshape(
                "0" -> $field("totalPop"),
                "1" -> $field("city"),
                "2" -> $field("state")))),
            $project(
              reshape(
                "totalPop" -> $field("_id", "0"),
                "city"     -> $field("_id", "1"),
                "state"    -> $field("_id", "2")),
              IgnoreId),
            $sort(NonEmptyList(BsonField.Name("totalPop") -> SortDir.Descending))))

    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; zips)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Let(__val)
        |│  │  │  │     ├─ Let(__val)
        |│  │  │  │     │  ├─ JsCore([_._id, _])
        |│  │  │  │     │  ╰─ JsCore([
        |│  │  │  │     │            __val[0],
        |│  │  │  │     │            (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1] : undefined,
        |│  │  │  │     │            [
        |│  │  │  │     │              (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].city : undefined],
        |│  │  │  │     │            __val[1]])
        |│  │  │  │     ╰─ Obj
        |│  │  │  │        ├─ Key(f0: ((isNumber(
        |│  │  │  │        │      (isObject(__val[3]) && (! Array.isArray(__val[3]))) ? __val[3].pop : undefined) || ((((isObject(__val[3]) && (! Array.isArray(__val[3]))) ? __val[3].pop : undefined) instanceof NumberInt) || (((isObject(__val[3]) && (! Array.isArray(__val[3]))) ? __val[3].pop : undefined) instanceof NumberLong))) && (isObject(__val[3]) && (! Array.isArray(__val[3])))) ? __val[3].pop : undefined)
        |│  │  │  │        ├─ Key(f1: (isObject(__val[3]) && (! Array.isArray(__val[3]))) ? __val[3].city : undefined)
        |│  │  │  │        ╰─ Key(b0: [
        |│  │  │  │               (isObject(__val[3]) && (! Array.isArray(__val[3]))) ? __val[3].city : undefined])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ├─ Name("f0" -> { "$sum": "$f0" })
        |│  │  │  │  ╰─ Name("f1" -> { "$first": "$f1" })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> "$b0")
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Arr
        |│  │  │  │     ├─ JsCore(_._id["0"])
        |│  │  │  │     ├─ Obj
        |│  │  │  │     │  ╰─ Key(totalPop: _.f0)
        |│  │  │  │     ╰─ Obj
        |│  │  │  │        ╰─ Key(city: _.f1)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$$ROOT" })
        |│  │  │  ╰─ By({ "$literal": null })
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; zips)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Let(__val)
        |│     │  │     ├─ JsCore([_._id, _])
        |│     │  │     ╰─ JsCore([
        |│     │  │               __val[0],
        |│     │  │               (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1] : undefined,
        |│     │  │               [
        |│     │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].city : undefined],
        |│     │  │               __val[1]])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) { return [null, { "left": [], "right": [value] }] })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ JsCore([_.left, _.right])
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> { "$literal": true })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ Expr($0 -> Eq(Bool(true)))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(f0)
        |│  │     │  ╰─ SpliceObjects
        |│  │     │     ├─ SpliceObjects
        |│  │     │     │  ├─ JsCore(_.src[0][1])
        |│  │     │     │  ╰─ JsCore(_.src[0][2])
        |│  │     │     ╰─ Obj
        |│  │     │        ╰─ Key(state: _.src[1][1].state)
        |│  │     ╰─ Key(b0)
        |│  │        ╰─ SpliceObjects
        |│  │           ├─ SpliceObjects
        |│  │           │  ├─ JsCore(_.src[0][1])
        |│  │           │  ╰─ JsCore(_.src[0][2])
        |│  │           ╰─ Obj
        |│  │              ╰─ Key(state: _.src[1][1].state)
        |│  ╰─ Scope(Map())
        |├─ $GroupF
        |│  ├─ Grouped
        |│  │  ╰─ Name("f0" -> { "$first": "$f0" })
        |│  ╰─ By
        |│     ╰─ Name("0" -> "$b0")
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ JsCore([_._id["0"], _.f0, _.f0.totalPop])
        |│  │     ╰─ JsCore([__val[1], __val])
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> {
        |│  │       "$arrayElemAt": [
        |│  │         { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] },
        |│  │         { "$literal": NumberInt("2") }]
        |│  │     })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $SortF
        |│  ╰─ SortKey(0 -> Descending)
        |╰─ $ProjectF
        |   ├─ Name("value" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ╰─ ExcludeId""".stripMargin)

    //
    "plan order by JS expr with filter" in {
      plan3_2(sqlE"select city, pop from zips where pop > 1000 order by length(city)") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.And(
            isNumeric(BsonField.Name("pop")),
            Selector.Doc(
              BsonField.Name("pop") -> Selector.Gt(Bson.Int32(1000))))),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
            "city"   -> Select(ident("x"), "city"),
            "pop"    -> Select(ident("x"), "pop"),
            "__tmp4" ->
              If(Call(ident("isString"), List(Select(ident("x"), "city"))),
                Call(ident("NumberLong"),
                  List(Select(Select(ident("x"), "city"), "length"))),
                ident("undefined")))))),
            ListMap()),
          $sort(NonEmptyList(BsonField.Name("__tmp4") -> SortDir.Ascending)),
          $project(
            reshape(
              "city" -> $field("city"),
              "pop"  -> $field("pop")),
            ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $MatchF
        |│  ╰─ And
        |│     ├─ Or
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($pop -> Type(Int32))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($pop -> Type(Int64))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($pop -> Type(Dec))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($pop -> Type(Text))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($pop -> Type(Date))
        |│     │  ╰─ Doc
        |│     │     ╰─ Expr($pop -> Type(Bool))
        |│     ╰─ Doc
        |│        ╰─ Expr($pop -> Gt(Int32(1000)))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ Arr
        |│  │     │  ├─ Obj
        |│  │     │  │  ├─ Key(city: _.city)
        |│  │     │  │  ╰─ Key(pop: _.pop)
        |│  │     │  ╰─ Ident(_)
        |│  │     ╰─ Obj
        |│  │        ├─ Key(0: (isString(
        |│  │        │      (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].city : undefined) && (isObject(__val[1]) && (! Array.isArray(__val[1])))) ? NumberLong(__val[1].city.length) : undefined)
        |│  │        ╰─ Key(src: __val)
        |│  ╰─ Scope(Map())
        |├─ $SortF
        |│  ╰─ SortKey(0 -> Ascending)
        |╰─ $ProjectF
        |   ├─ Name("value" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ╰─ ExcludeId""".stripMargin)

    "plan select length()" in {
      plan3_2(sqlE"select length(city) from zips") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "zips")),
          $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"),
            If(Call(ident("isString"),
              List(Select(ident("x"), "city"))),
              Call(ident("NumberLong"), List(Select(Select(ident("x"), "city"), "length"))),
              ident("undefined"))))),
          ListMap())))
    }

    "plan select length() and simple field" in {
      plan(sqlE"select city, length(city) from zips") must
      beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          reshape(
            "city" -> $field("city"),
            "1" ->
              $cond($and(
                $lte($literal(Bson.Text("")), $field("city")),
                $lt($field("city"), $literal(Bson.Doc()))),
                $strLenCP($field("city")),
                $literal(Bson.Undefined))),
          IgnoreId)))
    }

    "plan select length() and simple field 3.2" in {
      plan3_2(sqlE"select city, length(city) from zips") must
      beWorkflow(chain[Workflow](
        $read(collection("db", "zips")),
        $simpleMap(NonEmptyList(MapExpr(JsFn(Name("x"), obj(
          "city" -> Select(ident("x"), "city"),
          "1" ->
            If(Call(ident("isString"), List(Select(ident("x"), "city"))),
              Call(ident("NumberLong"),
                List(Select(Select(ident("x"), "city"), "length"))),
              ident("undefined")))))),
          ListMap()),
        $project(
          reshape(
            "city" -> $include(),
            "1"    -> $include()),
          IgnoreId)))
    }

    "plan combination of two distinct sets" in {
      plan(sqlE"SELECT (DISTINCT foo.bar) + (DISTINCT foo.baz) FROM foo") must
        beWorkflow0(
          $read(collection("db", "zips")))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; foo)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ JsCore(remove(_, "_id"))
        |│  ╰─ Scope(Map())
        |├─ $GroupF
        |│  ├─ Grouped
        |│  ╰─ By
        |│     ╰─ Name("0" -> "$$ROOT")
        |╰─ $ProjectF
        |   ├─ Name("value" -> {
        |   │       "$cond": [
        |   │         {
        |   │           "$and": [
        |   │             { "$lt": [{ "$literal": null }, "$_id.0.baz"] },
        |   │             { "$lt": ["$_id.0.baz", { "$literal": "" }] }]
        |   │         },
        |   │         {
        |   │           "$cond": [
        |   │             {
        |   │               "$or": [
        |   │                 {
        |   │                   "$and": [
        |   │                     { "$lt": [{ "$literal": null }, "$_id.0.bar"] },
        |   │                     { "$lt": ["$_id.0.bar", { "$literal": "" }] }]
        |   │                 },
        |   │                 {
        |   │                   "$and": [
        |   │                     {
        |   │                       "$lte": [
        |   │                         { "$literal": ISODate("-292275055-05-16T16:47:04.192Z") },
        |   │                         "$_id.0.bar"]
        |   │                     },
        |   │                     { "$lt": ["$_id.0.bar", { "$literal": new RegExp("", "") }] }]
        |   │                 }]
        |   │             },
        |   │             { "$add": ["$_id.0.bar", "$_id.0.baz"] },
        |   │             { "$literal": undefined }]
        |   │         },
        |   │         { "$literal": undefined }]
        |   │     })
        |   ╰─ ExcludeId""".stripMargin)

    "plan filter with timestamp and interval" in {
      val date0 = Bson.Date.fromInstant(Instant.parse("2014-11-17T00:00:00Z")).get
      val date22 = Bson.Date.fromInstant(Instant.parse("2014-11-17T22:00:00Z")).get

      plan(sqlE"""select * from days where date < timestamp("2014-11-17T22:00:00Z") and date - interval("PT12H") > timestamp("2014-11-17T00:00:00Z")""") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "days")),
          $project(
            reshape(
              "__tmp6" ->
                $cond(
                  $or(
                    $and(
                      $lt($literal(Bson.Null), $field("date")),
                      $lt($field("date"), $literal(Bson.Text("")))),
                    $and(
                      $lte($literal(Check.minDate), $field("date")),
                      $lt($field("date"), $literal(Bson.Regex("", ""))))),
                  $cond(
                    $or(
                      $and(
                        $lt($literal(Bson.Null), $field("date")),
                        $lt($field("date"), $literal(Bson.Doc()))),
                      $and(
                        $lte($literal(Bson.Bool(false)), $field("date")),
                        $lt($field("date"), $literal(Bson.Regex("", ""))))),
                    $and(
                      $lt($field("date"), $literal(date22)),
                      $gt(
                        $subtract($field("date"), $literal(Bson.Dec(12*60*60*1000))),
                        $literal(date0))),
                    $literal(Bson.Undefined)),
                  $literal(Bson.Undefined)),
              "__tmp7" -> $$ROOT),
            IgnoreId),
          $match(
            Selector.Doc(
              BsonField.Name("__tmp6") -> Selector.Eq(Bson.Bool(true)))),
          $project(
            reshape("value" -> $field("__tmp7")),
            ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; days)
        |├─ $ProjectF
        |│  ├─ Name("0" -> "$date")
        |│  ├─ Name("1" -> "$date")
        |│  ├─ Name("2" -> "$date")
        |│  ├─ Name("3" -> { "$subtract": ["$date", { "$literal": 4.32E7 }] })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $MatchF
        |│  ╰─ And
        |│     ├─ Or
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($0 -> Type(Int32))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($0 -> Type(Int64))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($0 -> Type(Dec))
        |│     │  ╰─ Doc
        |│     │     ╰─ Expr($0 -> Type(Date))
        |│     ├─ Or
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($1 -> Type(Int32))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($1 -> Type(Int64))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($1 -> Type(Dec))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($1 -> Type(Text))
        |│     │  ├─ Doc
        |│     │  │  ╰─ Expr($1 -> Type(Date))
        |│     │  ╰─ Doc
        |│     │     ╰─ Expr($1 -> Type(Bool))
        |│     ├─ Doc
        |│     │  ╰─ Expr($2 -> Lt(Date(1416261600000)))
        |│     ╰─ Doc
        |│        ╰─ Expr($3 -> Gt(Date(1416182400000)))
        |╰─ $ProjectF
        |   ├─ Name("value" -> "$src")
        |   ╰─ ExcludeId""".stripMargin)

    "plan time_of_day (JS)" in {
      plan(sqlE"select time_of_day(ts) from days") must
        beRight // NB: way too complicated to spell out here, and will change as JS generation improves
    }

    "plan time_of_day (pipeline)" in {
      import FormatSpecifier._

      plan3_0(sqlE"select time_of_day(ts) from days") must
        beWorkflow(chain[Workflow](
          $read(collection("db", "days")),
          $project(
            reshape("value" ->
              $cond(
                $and(
                  $lte($literal(Check.minDate), $field("ts")),
                  $lt($field("ts"), $literal(Bson.Regex("", "")))),
                $dateToString(Hour :: ":" :: Minute :: ":" :: Second :: "." :: Millisecond :: FormatString.empty, $field("ts")),
                $literal(Bson.Undefined))),
            ExcludeId)))
    }

    "plan filter on date" in {
      val date23 = Bson.Date.fromInstant(Instant.parse("2015-01-23T00:00:00Z")).get
      val date25 = Bson.Date.fromInstant(Instant.parse("2015-01-25T00:00:00Z")).get
      val date26 = Bson.Date.fromInstant(Instant.parse("2015-01-26T00:00:00Z")).get
      val date28 = Bson.Date.fromInstant(Instant.parse("2015-01-28T00:00:00Z")).get
      val date29 = Bson.Date.fromInstant(Instant.parse("2015-01-29T00:00:00Z")).get
      val date30 = Bson.Date.fromInstant(Instant.parse("2015-01-30T00:00:00Z")).get

      // Note: both of these boundaries require comparing with the start of the *next* day.
      plan(sqlE"""select * from logs where ((ts > date("2015-01-22") and ts <= date("2015-01-27")) and ts != date("2015-01-25")) or ts = date("2015-01-29")""") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "logs")),
          $match(Selector.Or(
            // TODO: Eliminate duplicates
            Selector.And(
              isNumeric(BsonField.Name("ts")),
              Selector.And(
                isNumeric(BsonField.Name("ts")),
                Selector.And(
                  Selector.And(
                    Selector.Doc(
                      BsonField.Name("ts") -> Selector.Gte(date23)),
                    Selector.Doc(
                      BsonField.Name("ts") -> Selector.Lt(date28))),
                  Selector.Or(
                    Selector.Doc(
                      BsonField.Name("ts") -> Selector.Lt(date25)),
                    Selector.Doc(
                      BsonField.Name("ts") -> Selector.Gte(date26)))))),
            Selector.And(
              Selector.Doc(
                BsonField.Name("ts") -> Selector.Gte(date29)),
              Selector.Doc(
                BsonField.Name("ts") -> Selector.Lt(date30)))))))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; logs)
        |╰─ $MatchF
        |   ╰─ And
        |      ├─ Or
        |      │  ├─ Doc
        |      │  │  ╰─ Expr($ts -> Type(Int32))
        |      │  ├─ Doc
        |      │  │  ╰─ Expr($ts -> Type(Int64))
        |      │  ├─ Doc
        |      │  │  ╰─ Expr($ts -> Type(Dec))
        |      │  ├─ Doc
        |      │  │  ╰─ Expr($ts -> Type(Text))
        |      │  ├─ Doc
        |      │  │  ╰─ Expr($ts -> Type(Date))
        |      │  ╰─ Doc
        |      │     ╰─ Expr($ts -> Type(Bool))
        |      ├─ Or
        |      │  ├─ Doc
        |      │  │  ╰─ Expr($ts -> Type(Int32))
        |      │  ├─ Doc
        |      │  │  ╰─ Expr($ts -> Type(Int64))
        |      │  ├─ Doc
        |      │  │  ╰─ Expr($ts -> Type(Dec))
        |      │  ├─ Doc
        |      │  │  ╰─ Expr($ts -> Type(Text))
        |      │  ├─ Doc
        |      │  │  ╰─ Expr($ts -> Type(Date))
        |      │  ╰─ Doc
        |      │     ╰─ Expr($ts -> Type(Bool))
        |      ╰─ Or
        |         ├─ And
        |         │  ├─ Doc
        |         │  │  ╰─ Expr($ts -> Gt(Date(1421884800000)))
        |         │  ├─ Doc
        |         │  │  ╰─ Expr($ts -> Lte(Date(1422316800000)))
        |         │  ╰─ Doc
        |         │     ╰─ Expr($ts -> Neq(Date(1422144000000)))
        |         ╰─ Doc
        |            ╰─ Expr($ts -> Eq(Date(1422489600000)))""".stripMargin)

    "plan js and filter with id" in {
      Bson.ObjectId.fromString("0123456789abcdef01234567").fold[Result](
        failure("Couldn’t create ObjectId."))(
        oid => plan3_2(sqlE"""select length(city), foo = oid("0123456789abcdef01234567") from days where `_id` = oid("0123456789abcdef01234567")""") must
          beWorkflow(chain[Workflow](
            $read(collection("db", "days")),
            $match(Selector.Doc(
              BsonField.Name("_id") -> Selector.Eq(oid))),
            $simpleMap(
              NonEmptyList(MapExpr(JsFn(Name("x"),
                obj(
                  "0" ->
                    If(Call(ident("isString"), List(Select(ident("x"), "city"))),
                      Call(ident("NumberLong"),
                        List(Select(Select(ident("x"), "city"), "length"))),
                      ident("undefined")),
                  "1" ->
                    BinOp(jscore.Eq,
                      Select(ident("x"), "foo"),
                      Call(ident("ObjectId"), List(jscore.Literal(Js.Str("0123456789abcdef01234567"))))))))),
              ListMap()),
            $project(
              reshape(
                "0" -> $include(),
                "1" -> $include()),
              IgnoreId))))
    }

    "plan convert to timestamp" in {
      plan(sqlE"select to_timestamp(epoch) from foo") must beWorkflow {
        chain[Workflow](
          $read(collection("db", "foo")),
          $project(
            reshape(
              "value" ->
                $cond(
                  $and(
                    $lt($literal(Bson.Null), $field("epoch")),
                    $lt($field("epoch"), $literal(Bson.Text("")))),
                  $add($literal(Bson.Date(0)), $field("epoch")),
                  $literal(Bson.Undefined))),
            ExcludeId))
      }
    }

    "plan convert to timestamp in map-reduce" in {
      plan3_2(sqlE"select length(name), to_timestamp(epoch) from foo") must beWorkflow {
        chain[Workflow](
          $read(collection("db", "foo")),
          $simpleMap(
            NonEmptyList(MapExpr(JsFn(Name("x"), obj(
              "0" ->
                If(Call(ident("isString"), List(Select(ident("x"), "name"))),
                  Call(ident("NumberLong"),
                    List(Select(Select(ident("x"), "name"), "length"))),
                  ident("undefined")),
              "1" ->
                If(
                  BinOp(jscore.Or,
                    Call(ident("isNumber"), List(Select(ident("x"), "epoch"))),
                    BinOp(jscore.Or,
                      BinOp(Instance, Select(ident("x"), "epoch"), ident("NumberInt")),
                      BinOp(Instance, Select(ident("x"), "epoch"), ident("NumberLong")))),
                  New(Name("Date"), List(Select(ident("x"), "epoch"))),
                  ident("undefined")))))),
            ListMap()),
          $project(
            reshape(
              "0" -> $include(),
              "1" -> $include()),
            IgnoreId))
      }
    }

    def joinStructure0(
      left: Workflow, leftName: String, leftBase: Fix[ExprOp], right: Workflow,
      leftKey: Reshape.Shape[ExprOp], rightKey: (String, Fix[ExprOp], Reshape.Shape[ExprOp]) \/ JsCore,
      fin: FixOp[WorkflowF],
      swapped: Boolean) = {

      val (leftLabel, rightLabel) =
        if (swapped) (JoinDir.Right.name, JoinDir.Left.name) else (JoinDir.Left.name, JoinDir.Right.name)
      def initialPipeOps(
        src: Workflow, name: String, base: Fix[ExprOp], key: Reshape.Shape[ExprOp], mainLabel: String, otherLabel: String):
          Workflow =
        chain[Workflow](
          src,
          $group(grouped(name -> $push(base)), key),
          $project(
            reshape(
              mainLabel  -> $field(name),
              otherLabel -> $literal(Bson.Arr(List())),
              "_id"      -> $include()),
            IncludeId))
      fin(
        $foldLeft(
          initialPipeOps(left, leftName, leftBase, leftKey, leftLabel, rightLabel),
          chain[Workflow](
            right,
            rightKey.fold(
              rk => initialPipeOps(_, rk._1, rk._2, rk._3, rightLabel, leftLabel),
              rk => $map($MapF.mapKeyVal(("key", "value"),
                rk.toJs,
                Js.AnonObjDecl(List(
                  (leftLabel, Js.AnonElem(List())),
                  (rightLabel, Js.AnonElem(List(Js.Ident("value"))))))),
                ListMap())),
            $reduce(
              Js.AnonFunDecl(List("key", "values"),
                List(
                  Js.VarDef(List(
                    ("result", Js.AnonObjDecl(List(
                      (leftLabel, Js.AnonElem(List())),
                      (rightLabel, Js.AnonElem(List()))))))),
                  Js.Call(Js.Select(Js.Ident("values"), "forEach"),
                    List(Js.AnonFunDecl(List("value"),
                      List(
                        Js.BinOp("=",
                          Js.Select(Js.Ident("result"), leftLabel),
                          Js.Call(
                            Js.Select(Js.Select(Js.Ident("result"), leftLabel), "concat"),
                            List(Js.Select(Js.Ident("value"), leftLabel)))),
                        Js.BinOp("=",
                          Js.Select(Js.Ident("result"), rightLabel),
                          Js.Call(
                            Js.Select(Js.Select(Js.Ident("result"), rightLabel), "concat"),
                            List(Js.Select(Js.Ident("value"), rightLabel)))))))),
                  Js.Return(Js.Ident("result")))),
              ListMap()))))
    }

    def joinStructure(
        left: Workflow, leftName: String, leftBase: Fix[ExprOp], right: Workflow,
        leftKey: Reshape.Shape[ExprOp], rightKey: (String, Fix[ExprOp], Reshape.Shape[ExprOp]) \/ JsCore,
        fin: FixOp[WorkflowF],
        swapped: Boolean) =
      Crystallize[WorkflowF].crystallize(joinStructure0(left, leftName, leftBase, right, leftKey, rightKey, fin, swapped))

    "plan simple join (map-reduce)" in {
      plan2_6(sqlE"select zips2.city from zips join zips2 on zips.`_id` = zips2.`_id`") must
        beWorkflow0(
          joinStructure(
            $read(collection("db", "zips")), "__tmp0", $$ROOT,
            $read(collection("db", "zips2")),
            reshape("0" -> $field("_id")),
            Obj(ListMap(Name("0") -> Select(ident("value"), "_id"))).right,
            chain[Workflow](_,
              $match(Selector.Doc(ListMap[BsonField, Selector.SelectorExpr](
                JoinHandler.LeftName -> Selector.NotExpr(Selector.Size(0)),
                JoinHandler.RightName -> Selector.NotExpr(Selector.Size(0))))),
              $unwind(DocField(JoinHandler.LeftName)),
              $unwind(DocField(JoinHandler.RightName)),
              $project(
                reshape("value" -> $field(JoinDir.Right.name, "city")),
                ExcludeId)),
            false).op)
    }.pendingWithActual("#1560",
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; zips)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Let(__val)
        |│  │  │  │     ├─ JsCore([_._id, _])
        |│  │  │  │     ╰─ Obj
        |│  │  │  │        ├─ Key(0: __val[1])
        |│  │  │  │        ├─ Key(1: true)
        |│  │  │  │        ╰─ Key(src: __val)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ And
        |│  │  │     ├─ Doc
        |│  │  │     │  ╰─ Expr($0 -> Type(Doc))
        |│  │  │     ╰─ Doc
        |│  │  │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Obj
        |│  │  │  │     ├─ Key(0: _.src[1]._id)
        |│  │  │  │     ╰─ Key(content: _.src)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$content" })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> "$0")
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; zips2)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Let(__val)
        |│     │  │     ├─ JsCore([_._id, _])
        |│     │  │     ╰─ Obj
        |│     │  │        ├─ Key(0: __val[1])
        |│     │  │        ├─ Key(1: true)
        |│     │  │        ╰─ Key(src: __val)
        |│     │  ╰─ Scope(Map())
        |│     ├─ $MatchF
        |│     │  ╰─ And
        |│     │     ├─ Doc
        |│     │     │  ╰─ Expr($0 -> Type(Doc))
        |│     │     ╰─ Doc
        |│     │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) { return [{ "0": value.src[1]._id }, { "left": [], "right": [value.src] }] })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |╰─ $SimpleMapF
        |   ├─ Map
        |   │  ╰─ JsCore(_.right[1].city)
        |   ╰─ Scope(Map())""".stripMargin)

    "plan simple join ($lookup)" in {
      plan(sqlE"select zips2.city from zips join zips2 on zips.`_id` = zips2.`_id`") must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "zips")),
          $match(Selector.Doc(
            BsonField.Name("_id") -> Selector.Exists(true))),
          $project(reshape(JoinDir.Left.name -> $$ROOT)),
          $lookup(
            CollectionName("zips2"),
            JoinHandler.LeftName \ BsonField.Name("_id"),
            BsonField.Name("_id"),
            JoinHandler.RightName),
          $unwind(DocField(JoinHandler.RightName)),
          $project(
            reshape("value" -> $field(JoinDir.Right.name, "city")),
            ExcludeId)))
    }.pendingWithActual("#1560",
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; zips)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ JsCore([_._id, _])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $ProjectF
        |│  │  │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│  │  │  ├─ Name("1" -> { "$literal": true })
        |│  │  │  ├─ Name("src" -> "$$ROOT")
        |│  │  │  ╰─ IgnoreId
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ And
        |│  │  │     ├─ Doc
        |│  │  │     │  ╰─ Expr($0 -> Type(Doc))
        |│  │  │     ╰─ Doc
        |│  │  │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Obj
        |│  │  │  │     ├─ Key(0: _.src[1]._id)
        |│  │  │  │     ╰─ Key(content: _.src)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$content" })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> "$0")
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; zips2)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ JsCore([_._id, _])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $ProjectF
        |│     │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│     │  ├─ Name("1" -> { "$literal": true })
        |│     │  ├─ Name("src" -> "$$ROOT")
        |│     │  ╰─ IgnoreId
        |│     ├─ $MatchF
        |│     │  ╰─ And
        |│     │     ├─ Doc
        |│     │     │  ╰─ Expr($0 -> Type(Doc))
        |│     │     ╰─ Doc
        |│     │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) { return [{ "0": value.src[1]._id }, { "left": [], "right": [value.src] }] })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |╰─ $SimpleMapF
        |   ├─ Map
        |   │  ╰─ JsCore(_.right[1].city)
        |   ╰─ Scope(Map())""".stripMargin)

    "plan simple join with sharded inputs" in {
      // NB: cannot use $lookup, so fall back to the old approach
      val query = sqlE"select zips2.city from zips join zips2 on zips.`_id` = zips2.`_id`"
      plan3_4(query,
        c => Map(
          collection("db", "zips") -> CollectionStatistics(10, 100, true),
          collection("db", "zips2") -> CollectionStatistics(15, 150, true)).get(c),
        defaultIndexes) must_==
        plan2_6(query)
    }.pendingUntilFixed(notOnPar)

    "plan simple join with sources in different DBs" in {
      // NB: cannot use $lookup, so fall back to the old approach
      val query = sqlE"select zips2.city from `/db1/zips` join `/db2/zips2` on zips.`_id` = zips2.`_id`"
      plan(query) must_== plan2_6(query)
    }.pendingUntilFixed(notOnPar)

    "plan simple join with no index" in {
      // NB: cannot use $lookup, so fall back to the old approach
      val query = sqlE"select zips2.city from zips join zips2 on zips.pop = zips2.pop"
      plan(query) must_== plan2_6(query)
    }.pendingUntilFixed(notOnPar)

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
              reshape("value" -> $field("__tmp11", "city")),
              ExcludeId)),
          false).op)
    }.pendingWithActual("#1560",
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; zips)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Let(__val)
        |│  │  │  │     ├─ JsCore([_._id, _])
        |│  │  │  │     ╰─ Obj
        |│  │  │  │        ├─ Key(0: __val[1]._id)
        |│  │  │  │        ├─ Key(1: true)
        |│  │  │  │        ├─ Key(2: __val[1])
        |│  │  │  │        ├─ Key(3: true)
        |│  │  │  │        ╰─ Key(src: __val)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ And
        |│  │  │     ├─ Or
        |│  │  │     │  ├─ Doc
        |│  │  │     │  │  ╰─ Expr($0 -> Type(Int32))
        |│  │  │     │  ├─ Doc
        |│  │  │     │  │  ╰─ Expr($0 -> Type(Int64))
        |│  │  │     │  ├─ Doc
        |│  │  │     │  │  ╰─ Expr($0 -> Type(Dec))
        |│  │  │     │  ├─ Doc
        |│  │  │     │  │  ╰─ Expr($0 -> Type(Text))
        |│  │  │     │  ├─ Doc
        |│  │  │     │  │  ╰─ Expr($0 -> Type(Date))
        |│  │  │     │  ╰─ Doc
        |│  │  │     │     ╰─ Expr($0 -> Type(Bool))
        |│  │  │     ├─ Doc
        |│  │  │     │  ╰─ Expr($1 -> Eq(Bool(true)))
        |│  │  │     ├─ Doc
        |│  │  │     │  ╰─ Expr($2 -> Type(Doc))
        |│  │  │     ╰─ Doc
        |│  │  │        ╰─ Expr($3 -> Eq(Bool(true)))
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$src" })
        |│  │  │  ╰─ By({ "$literal": null })
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; zips2)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Let(__val)
        |│     │  │     ├─ JsCore([_._id, _])
        |│     │  │     ╰─ Obj
        |│     │  │        ├─ Key(0: __val[1]._id)
        |│     │  │        ├─ Key(1: true)
        |│     │  │        ├─ Key(2: __val[1])
        |│     │  │        ├─ Key(3: true)
        |│     │  │        ╰─ Key(src: __val)
        |│     │  ╰─ Scope(Map())
        |│     ├─ $MatchF
        |│     │  ╰─ And
        |│     │     ├─ Or
        |│     │     │  ├─ Doc
        |│     │     │  │  ╰─ Expr($0 -> Type(Int32))
        |│     │     │  ├─ Doc
        |│     │     │  │  ╰─ Expr($0 -> Type(Int64))
        |│     │     │  ├─ Doc
        |│     │     │  │  ╰─ Expr($0 -> Type(Dec))
        |│     │     │  ├─ Doc
        |│     │     │  │  ╰─ Expr($0 -> Type(Text))
        |│     │     │  ├─ Doc
        |│     │     │  │  ╰─ Expr($0 -> Type(Date))
        |│     │     │  ╰─ Doc
        |│     │     │     ╰─ Expr($0 -> Type(Bool))
        |│     │     ├─ Doc
        |│     │     │  ╰─ Expr($1 -> Eq(Bool(true)))
        |│     │     ├─ Doc
        |│     │     │  ╰─ Expr($2 -> Type(Doc))
        |│     │     ╰─ Doc
        |│     │        ╰─ Expr($3 -> Eq(Bool(true)))
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) { return [null, { "left": [], "right": [value.src] }] })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ JsCore([
        |│  │     │         [
        |│  │     │           _.left[0],
        |│  │     │           [_.left[0]],
        |│  │     │           _.left[1],
        |│  │     │           ((((isNumber(_.left[1]._id) || ((_.left[1]._id instanceof NumberInt) || (_.left[1]._id instanceof NumberLong))) || isString(_.left[1]._id)) || ((_.left[1]._id instanceof Date) || ((typeof _.left[1]._id) === "boolean"))) ? true : false) && ((isObject(_.left[1]) && (! Array.isArray(_.left[1]))) ? true : false)],
        |│  │     │         [
        |│  │     │           _.right[0],
        |│  │     │           [_.right[0]],
        |│  │     │           _.right[1],
        |│  │     │           ((((isNumber(_.right[1]._id) || ((_.right[1]._id instanceof NumberInt) || (_.right[1]._id instanceof NumberLong))) || isString(_.right[1]._id)) || ((_.right[1]._id instanceof Date) || ((typeof _.right[1]._id) === "boolean"))) ? true : false) && ((isObject(_.right[1]) && (! Array.isArray(_.right[1]))) ? true : false)]])
        |│  │     ╰─ Obj
        |│  │        ├─ Key(0: true)
        |│  │        ├─ Key(1: __val[0][2]._id < __val[1][2]._id)
        |│  │        ╰─ Key(src: __val)
        |│  ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ And
        |│     ├─ Doc
        |│     │  ╰─ Expr($0 -> Eq(Bool(true)))
        |│     ╰─ Doc
        |│        ╰─ Expr($1 -> Eq(Bool(true)))
        |╰─ $SimpleMapF
        |   ├─ Map
        |   │  ╰─ JsCore((isObject(_.src[1][2]) && (! Array.isArray(_.src[1][2]))) ? _.src[1][2].city : undefined)
        |   ╰─ Scope(Map())""".stripMargin)

    "plan simple inner equi-join (map-reduce)" in {
      plan2_6(
        sqlE"select foo.name, bar.address from foo join bar on foo.id = bar.foo_id") must
      beWorkflow0(
        joinStructure(
          $read(collection("db", "foo")), "__tmp0", $$ROOT,
          $read(collection("db", "bar")),
          reshape("0" -> $field("id")),
          Obj(ListMap(Name("0") -> Select(ident("value"), "foo_id"))).right,
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
    }.pendingWithActual("#1560",
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; foo)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Let(__val)
        |│  │  │  │     ├─ JsCore([_._id, _])
        |│  │  │  │     ╰─ Obj
        |│  │  │  │        ├─ Key(0: __val[1])
        |│  │  │  │        ├─ Key(1: true)
        |│  │  │  │        ╰─ Key(src: __val)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ And
        |│  │  │     ├─ Doc
        |│  │  │     │  ╰─ Expr($0 -> Type(Doc))
        |│  │  │     ╰─ Doc
        |│  │  │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Obj
        |│  │  │  │     ├─ Key(0: _.src[1].id)
        |│  │  │  │     ╰─ Key(content: _.src)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$content" })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> "$0")
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; bar)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Let(__val)
        |│     │  │     ├─ JsCore([_._id, _])
        |│     │  │     ╰─ Obj
        |│     │  │        ├─ Key(0: __val[1])
        |│     │  │        ├─ Key(1: true)
        |│     │  │        ╰─ Key(src: __val)
        |│     │  ╰─ Scope(Map())
        |│     ├─ $MatchF
        |│     │  ╰─ And
        |│     │     ├─ Doc
        |│     │     │  ╰─ Expr($0 -> Type(Doc))
        |│     │     ╰─ Doc
        |│     │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) {
        |│     │  │               return [{ "0": value.src[1].foo_id }, { "left": [], "right": [value.src] }]
        |│     │  │             })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(name: (isObject(_.left[1]) && (! Array.isArray(_.left[1]))) ? _.left[1].name : undefined)
        |│  │     ╰─ Key(address: (isObject(_.right[1]) && (! Array.isArray(_.right[1]))) ? _.right[1].address : undefined)
        |│  ╰─ Scope(Map())
        |╰─ $ProjectF
        |   ├─ Name("name" -> true)
        |   ├─ Name("address" -> true)
        |   ╰─ IgnoreId""".stripMargin)

    "plan simple inner equi-join ($lookup)" in {
      plan3_4(
        sqlE"select foo.name, bar.address from foo join bar on foo.id = bar.foo_id",
        defaultStats,
        indexes(collection("db", "bar") -> BsonField.Name("foo_id"))) must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "foo")),
        $match(Selector.Doc(
          BsonField.Name("id") -> Selector.Exists(true))),
        $project(reshape(JoinDir.Left.name -> $$ROOT)),
        $lookup(
          CollectionName("bar"),
          JoinHandler.LeftName \ BsonField.Name("id"),
          BsonField.Name("foo_id"),
          JoinHandler.RightName),
        $unwind(DocField(JoinHandler.RightName)),
        $project(reshape(
          "name" ->
            $cond(
              $and(
                $lte($literal(Bson.Doc()), $field(JoinDir.Left.name)),
                $lt($field(JoinDir.Left.name), $literal(Bson.Arr(Nil)))),
              $field(JoinDir.Left.name, "name"),
              $literal(Bson.Undefined)),
          "address" ->
            $cond(
              $and(
                $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                $lt($field(JoinDir.Right.name), $literal(Bson.Arr(Nil)))),
              $field(JoinDir.Right.name, "address"),
              $literal(Bson.Undefined))),
          IgnoreId)))
    }.pendingWithActual("#1560",
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; foo)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ JsCore([_._id, _])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $ProjectF
        |│  │  │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│  │  │  ├─ Name("1" -> { "$literal": true })
        |│  │  │  ├─ Name("src" -> "$$ROOT")
        |│  │  │  ╰─ IgnoreId
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ And
        |│  │  │     ├─ Doc
        |│  │  │     │  ╰─ Expr($0 -> Type(Doc))
        |│  │  │     ╰─ Doc
        |│  │  │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Obj
        |│  │  │  │     ├─ Key(0: _.src[1].id)
        |│  │  │  │     ╰─ Key(content: _.src)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$content" })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> "$0")
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; bar)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ JsCore([_._id, _])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $ProjectF
        |│     │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│     │  ├─ Name("1" -> { "$literal": true })
        |│     │  ├─ Name("src" -> "$$ROOT")
        |│     │  ╰─ IgnoreId
        |│     ├─ $MatchF
        |│     │  ╰─ And
        |│     │     ├─ Doc
        |│     │     │  ╰─ Expr($0 -> Type(Doc))
        |│     │     ╰─ Doc
        |│     │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) {
        |│     │  │               return [{ "0": value.src[1].foo_id }, { "left": [], "right": [value.src] }]
        |│     │  │             })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(name: (isObject(_.left[1]) && (! Array.isArray(_.left[1]))) ? _.left[1].name : undefined)
        |│  │     ╰─ Key(address: (isObject(_.right[1]) && (! Array.isArray(_.right[1]))) ? _.right[1].address : undefined)
        |│  ╰─ Scope(Map())
        |╰─ $ProjectF
        |   ├─ Name("name" -> true)
        |   ├─ Name("address" -> true)
        |   ╰─ IgnoreId""".stripMargin)

    "plan simple inner equi-join with expression ($lookup)" in {
      plan3_4(
        sqlE"select foo.name, bar.address from foo join bar on lower(foo.id) = bar.foo_id",
        defaultStats,
        indexes(collection("db", "bar") -> BsonField.Name("foo_id"))) must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "foo")),
        $project(reshape(
          JoinDir.Left.name -> $$ROOT,
          "__tmp0" -> $toLower($field("id"))),
          IgnoreId),
        $lookup(
          CollectionName("bar"),
          BsonField.Name("__tmp0"),
          BsonField.Name("foo_id"),
          JoinHandler.RightName),
        $project(reshape(
          JoinDir.Left.name -> $field(JoinDir.Left.name),
          JoinDir.Right.name -> $field(JoinDir.Right.name))),
        $unwind(DocField(JoinHandler.RightName)),
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
    }.pendingWithActual("#1560",
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; foo)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Let(__val)
        |│  │  │  │     ├─ JsCore([_._id, _])
        |│  │  │  │     ╰─ Obj
        |│  │  │  │        ├─ Key(0: (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].id : undefined)
        |│  │  │  │        ├─ Key(1: true)
        |│  │  │  │        ╰─ Key(src: __val)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ And
        |│  │  │     ├─ Doc
        |│  │  │     │  ╰─ Expr($0 -> Type(Text))
        |│  │  │     ╰─ Doc
        |│  │  │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Obj
        |│  │  │  │     ├─ Key(0: (isObject(_.src[1]) && (! Array.isArray(_.src[1]))) ? _.src[1].id.toLowerCase() : undefined)
        |│  │  │  │     ╰─ Key(content: _.src)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$content" })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> "$0")
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; bar)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ JsCore([_._id, _])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $ProjectF
        |│     │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│     │  ├─ Name("1" -> { "$literal": true })
        |│     │  ├─ Name("src" -> "$$ROOT")
        |│     │  ╰─ IgnoreId
        |│     ├─ $MatchF
        |│     │  ╰─ And
        |│     │     ├─ Doc
        |│     │     │  ╰─ Expr($0 -> Type(Doc))
        |│     │     ╰─ Doc
        |│     │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) {
        |│     │  │               return [{ "0": value.src[1].foo_id }, { "left": [], "right": [value.src] }]
        |│     │  │             })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(name: ((isObject(
        |│  │     │      (isObject(_.left[1]) && (! Array.isArray(_.left[1]))) ? _.left[1] : undefined) && (! Array.isArray(
        |│  │     │      (isObject(_.left[1]) && (! Array.isArray(_.left[1]))) ? _.left[1] : undefined))) && (isObject(_.left[1]) && (! Array.isArray(_.left[1])))) ? _.left[1].name : undefined)
        |│  │     ╰─ Key(address: (isObject(_.right[1]) && (! Array.isArray(_.right[1]))) ? _.right[1].address : undefined)
        |│  ╰─ Scope(Map())
        |╰─ $ProjectF
        |   ├─ Name("name" -> true)
        |   ├─ Name("address" -> true)
        |   ╰─ IgnoreId""".stripMargin)

    "plan simple inner equi-join with pre-filtering ($lookup)" in {
      plan3_4(
        sqlE"select foo.name, bar.address from foo join bar on foo.id = bar.foo_id where bar.rating >= 4",
        defaultStats,
        indexes(collection("db", "foo") -> BsonField.Name("id"))) must
      beWorkflow0(chain[Workflow](
        $read(collection("db", "bar")),
        $match(
          Selector.And(
            isNumeric(BsonField.Name("rating")),
            Selector.Doc(
              BsonField.Name("rating") -> Selector.Gte(Bson.Int32(4))))),
        $project(reshape(
          JoinDir.Right.name -> $$ROOT,
          "__tmp2" -> $field("foo_id")),
          ExcludeId),
        $lookup(
          CollectionName("foo"),
          BsonField.Name("__tmp2"),
          BsonField.Name("id"),
          JoinHandler.LeftName),
        $project(reshape(
          JoinDir.Right.name -> $field(JoinDir.Right.name),
          JoinDir.Left.name -> $field(JoinDir.Left.name))),
        $unwind(DocField(JoinHandler.LeftName)),
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
    }.pendingWithActual("#1560",
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; foo)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ JsCore([_._id, _])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $ProjectF
        |│  │  │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│  │  │  ├─ Name("1" -> { "$literal": true })
        |│  │  │  ├─ Name("src" -> "$$ROOT")
        |│  │  │  ╰─ IgnoreId
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ And
        |│  │  │     ├─ Doc
        |│  │  │     │  ╰─ Expr($0 -> Type(Doc))
        |│  │  │     ╰─ Doc
        |│  │  │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Obj
        |│  │  │  │     ├─ Key(0: _.src[1].id)
        |│  │  │  │     ╰─ Key(content: _.src)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$content" })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> "$0")
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; bar)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Let(__val)
        |│     │  │     ├─ JsCore([_._id, _])
        |│     │  │     ╰─ Obj
        |│     │  │        ├─ Key(0: (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].rating : undefined)
        |│     │  │        ├─ Key(1: __val[1])
        |│     │  │        ├─ Key(2: __val[1].rating)
        |│     │  │        ╰─ Key(src: __val)
        |│     │  ╰─ Scope(Map())
        |│     ├─ $MatchF
        |│     │  ╰─ And
        |│     │     ├─ Or
        |│     │     │  ├─ Doc
        |│     │     │  │  ╰─ Expr($0 -> Type(Int32))
        |│     │     │  ├─ Doc
        |│     │     │  │  ╰─ Expr($0 -> Type(Int64))
        |│     │     │  ├─ Doc
        |│     │     │  │  ╰─ Expr($0 -> Type(Dec))
        |│     │     │  ├─ Doc
        |│     │     │  │  ╰─ Expr($0 -> Type(Text))
        |│     │     │  ├─ Doc
        |│     │     │  │  ╰─ Expr($0 -> Type(Date))
        |│     │     │  ╰─ Doc
        |│     │     │     ╰─ Expr($0 -> Type(Bool))
        |│     │     ├─ Doc
        |│     │     │  ╰─ Expr($1 -> Type(Doc))
        |│     │     ╰─ Doc
        |│     │        ╰─ Expr($2 -> Gte(Int32(4)))
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) {
        |│     │  │               return [
        |│     │  │                 {
        |│     │  │                   "0": (isObject(value.src[1]) && (! Array.isArray(value.src[1]))) ? value.src[1].foo_id : undefined
        |│     │  │                 },
        |│     │  │                 { "left": [], "right": [value.src] }]
        |│     │  │             })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(name: (isObject(_.left[1]) && (! Array.isArray(_.left[1]))) ? _.left[1].name : undefined)
        |│  │     ╰─ Key(address: ((isObject(
        |│  │            (isObject(_.right[1]) && (! Array.isArray(_.right[1]))) ? _.right[1] : undefined) && (! Array.isArray(
        |│  │            (isObject(_.right[1]) && (! Array.isArray(_.right[1]))) ? _.right[1] : undefined))) && (isObject(_.right[1]) && (! Array.isArray(_.right[1])))) ? _.right[1].address : undefined)
        |│  ╰─ Scope(Map())
        |╰─ $ProjectF
        |   ├─ Name("name" -> true)
        |   ├─ Name("address" -> true)
        |   ╰─ IgnoreId""".stripMargin)

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
              reshape("value" -> $field("__tmp7")),
              ExcludeId)),
          false).op)
    }.pendingWithActual("#1560",
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; foo)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ JsCore([_._id, _])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $ProjectF
        |│  │  │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│  │  │  ├─ Name("1" -> { "$literal": true })
        |│  │  │  ├─ Name("src" -> "$$ROOT")
        |│  │  │  ╰─ IgnoreId
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ And
        |│  │  │     ├─ Doc
        |│  │  │     │  ╰─ Expr($0 -> Type(Doc))
        |│  │  │     ╰─ Doc
        |│  │  │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Obj
        |│  │  │  │     ├─ Key(0: _.src[1].id)
        |│  │  │  │     ╰─ Key(content: _.src)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$content" })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> "$0")
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; bar)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ JsCore([_._id, _])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $ProjectF
        |│     │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│     │  ├─ Name("1" -> { "$literal": true })
        |│     │  ├─ Name("src" -> "$$ROOT")
        |│     │  ╰─ IgnoreId
        |│     ├─ $MatchF
        |│     │  ╰─ And
        |│     │     ├─ Doc
        |│     │     │  ╰─ Expr($0 -> Type(Doc))
        |│     │     ╰─ Doc
        |│     │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) {
        |│     │  │               return [{ "0": value.src[1].foo_id }, { "left": [], "right": [value.src] }]
        |│     │  │             })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("left" -> {
        |│  │       "$cond": [
        |│  │         { "$eq": [{ "$size": "$left" }, { "$literal": NumberInt("0") }] },
        |│  │         { "$literal": [{  }] },
        |│  │         "$left"]
        |│  │     })
        |│  ├─ Name("right" -> {
        |│  │       "$cond": [
        |│  │         { "$eq": [{ "$size": "$right" }, { "$literal": NumberInt("0") }] },
        |│  │         { "$literal": [{  }] },
        |│  │         "$right"]
        |│  │     })
        |│  ╰─ IgnoreId
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |╰─ $SimpleMapF
        |   ├─ Map
        |   │  ╰─ If
        |   │     ├─ JsCore(isObject(_.right[1]) && (! Array.isArray(_.right[1])))
        |   │     ├─ If
        |   │     │  ├─ JsCore(isObject(_.left[1]) && (! Array.isArray(_.left[1])))
        |   │     │  ├─ SpliceObjects
        |   │     │  │  ├─ JsCore(_.left[1])
        |   │     │  │  ╰─ JsCore(_.right[1])
        |   │     │  ╰─ Ident(undefined)
        |   │     ╰─ Ident(undefined)
        |   ╰─ Scope(Map())""".stripMargin)

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
    }.pendingWithActual("#1560",
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; foo)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ JsCore([_._id, _])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $ProjectF
        |│  │  │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│  │  │  ├─ Name("1" -> { "$literal": true })
        |│  │  │  ├─ Name("src" -> "$$ROOT")
        |│  │  │  ╰─ IgnoreId
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ And
        |│  │  │     ├─ Doc
        |│  │  │     │  ╰─ Expr($0 -> Type(Doc))
        |│  │  │     ╰─ Doc
        |│  │  │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Obj
        |│  │  │  │     ├─ Key(0: _.src[1].id)
        |│  │  │  │     ╰─ Key(content: _.src)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$content" })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> "$0")
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; bar)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ JsCore([_._id, _])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $ProjectF
        |│     │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│     │  ├─ Name("1" -> { "$literal": true })
        |│     │  ├─ Name("src" -> "$$ROOT")
        |│     │  ╰─ IgnoreId
        |│     ├─ $MatchF
        |│     │  ╰─ And
        |│     │     ├─ Doc
        |│     │     │  ╰─ Expr($0 -> Type(Doc))
        |│     │     ╰─ Doc
        |│     │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) {
        |│     │  │               return [{ "0": value.src[1].foo_id }, { "left": [], "right": [value.src] }]
        |│     │  │             })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ NotExpr($left -> Size(0))
        |├─ $ProjectF
        |│  ├─ Name("left" -> "$left")
        |│  ├─ Name("right" -> {
        |│  │       "$cond": [
        |│  │         { "$eq": [{ "$size": "$right" }, { "$literal": NumberInt("0") }] },
        |│  │         { "$literal": [{  }] },
        |│  │         "$right"]
        |│  │     })
        |│  ╰─ IgnoreId
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(name: (isObject(_.left[1]) && (! Array.isArray(_.left[1]))) ? _.left[1].name : undefined)
        |│  │     ╰─ Key(address: (isObject(_.right[1]) && (! Array.isArray(_.right[1]))) ? _.right[1].address : undefined)
        |│  ╰─ Scope(Map())
        |╰─ $ProjectF
        |   ├─ Name("name" -> true)
        |   ├─ Name("address" -> true)
        |   ╰─ IgnoreId""".stripMargin)

    "plan simple left equi-join ($lookup)" in {
      plan3_4(
        sqlE"select foo.name, bar.address from foo left join bar on foo.id = bar.foo_id",
        defaultStats,
        indexes(collection("db", "bar") -> BsonField.Name("foo_id"))) must
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
    }.pendingWithActual("TODO: left/right joins in $lookup",
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; foo)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ JsCore([_._id, _])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $ProjectF
        |│  │  │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│  │  │  ├─ Name("1" -> { "$literal": true })
        |│  │  │  ├─ Name("src" -> "$$ROOT")
        |│  │  │  ╰─ IgnoreId
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ And
        |│  │  │     ├─ Doc
        |│  │  │     │  ╰─ Expr($0 -> Type(Doc))
        |│  │  │     ╰─ Doc
        |│  │  │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Obj
        |│  │  │  │     ├─ Key(0: _.src[1].id)
        |│  │  │  │     ╰─ Key(content: _.src)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$content" })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> "$0")
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; bar)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ JsCore([_._id, _])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $ProjectF
        |│     │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│     │  ├─ Name("1" -> { "$literal": true })
        |│     │  ├─ Name("src" -> "$$ROOT")
        |│     │  ╰─ IgnoreId
        |│     ├─ $MatchF
        |│     │  ╰─ And
        |│     │     ├─ Doc
        |│     │     │  ╰─ Expr($0 -> Type(Doc))
        |│     │     ╰─ Doc
        |│     │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) {
        |│     │  │               return [{ "0": value.src[1].foo_id }, { "left": [], "right": [value.src] }]
        |│     │  │             })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ NotExpr($left -> Size(0))
        |├─ $ProjectF
        |│  ├─ Name("left" -> "$left")
        |│  ├─ Name("right" -> {
        |│  │       "$cond": [
        |│  │         { "$eq": [{ "$size": "$right" }, { "$literal": NumberInt("0") }] },
        |│  │         { "$literal": [{  }] },
        |│  │         "$right"]
        |│  │     })
        |│  ╰─ IgnoreId
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(name: (isObject(_.left[1]) && (! Array.isArray(_.left[1]))) ? _.left[1].name : undefined)
        |│  │     ╰─ Key(address: (isObject(_.right[1]) && (! Array.isArray(_.right[1]))) ? _.right[1].address : undefined)
        |│  ╰─ Scope(Map())
        |╰─ $ProjectF
        |   ├─ Name("name" -> true)
        |   ├─ Name("address" -> true)
        |   ╰─ IgnoreId""".stripMargin)

    "plan simple right equi-join ($lookup)" in {
      plan3_4(
        sqlE"select foo.name, bar.address from foo right join bar on foo.id = bar.foo_id",
        defaultStats,
        indexes(collection("db", "bar") -> BsonField.Name("foo_id"))) must
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
    }.pendingWithActual("TODO: left/right joins in $lookup",
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; foo)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ JsCore([_._id, _])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $ProjectF
        |│  │  │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│  │  │  ├─ Name("1" -> { "$literal": true })
        |│  │  │  ├─ Name("src" -> "$$ROOT")
        |│  │  │  ╰─ IgnoreId
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ And
        |│  │  │     ├─ Doc
        |│  │  │     │  ╰─ Expr($0 -> Type(Doc))
        |│  │  │     ╰─ Doc
        |│  │  │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Obj
        |│  │  │  │     ├─ Key(0: _.src[1].id)
        |│  │  │  │     ╰─ Key(content: _.src)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$content" })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> "$0")
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; bar)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ JsCore([_._id, _])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $ProjectF
        |│     │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│     │  ├─ Name("1" -> { "$literal": true })
        |│     │  ├─ Name("src" -> "$$ROOT")
        |│     │  ╰─ IgnoreId
        |│     ├─ $MatchF
        |│     │  ╰─ And
        |│     │     ├─ Doc
        |│     │     │  ╰─ Expr($0 -> Type(Doc))
        |│     │     ╰─ Doc
        |│     │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) {
        |│     │  │               return [{ "0": value.src[1].foo_id }, { "left": [], "right": [value.src] }]
        |│     │  │             })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $ProjectF
        |│  ├─ Name("left" -> {
        |│  │       "$cond": [
        |│  │         { "$eq": [{ "$size": "$left" }, { "$literal": NumberInt("0") }] },
        |│  │         { "$literal": [{  }] },
        |│  │         "$left"]
        |│  │     })
        |│  ├─ Name("right" -> "$right")
        |│  ╰─ IgnoreId
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(name: (isObject(_.left[1]) && (! Array.isArray(_.left[1]))) ? _.left[1].name : undefined)
        |│  │     ╰─ Key(address: (isObject(_.right[1]) && (! Array.isArray(_.right[1]))) ? _.right[1].address : undefined)
        |│  ╰─ Scope(Map())
        |╰─ $ProjectF
        |   ├─ Name("name" -> true)
        |   ├─ Name("address" -> true)
        |   ╰─ IgnoreId""".stripMargin)

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
    }.pendingWithActual("#1560",
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $FoldLeftF
        |│  │  │  ├─ Chain
        |│  │  │  │  ├─ $ReadF(db; foo)
        |│  │  │  │  ├─ $SimpleMapF
        |│  │  │  │  │  ├─ Map
        |│  │  │  │  │  │  ╰─ Let(__val)
        |│  │  │  │  │  │     ├─ JsCore([_._id, _])
        |│  │  │  │  │  │     ╰─ Obj
        |│  │  │  │  │  │        ├─ Key(0: __val[1])
        |│  │  │  │  │  │        ├─ Key(1: true)
        |│  │  │  │  │  │        ╰─ Key(src: __val)
        |│  │  │  │  │  ╰─ Scope(Map())
        |│  │  │  │  ├─ $MatchF
        |│  │  │  │  │  ╰─ And
        |│  │  │  │  │     ├─ Doc
        |│  │  │  │  │     │  ╰─ Expr($0 -> Type(Doc))
        |│  │  │  │  │     ╰─ Doc
        |│  │  │  │  │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│  │  │  │  ├─ $SimpleMapF
        |│  │  │  │  │  ├─ Map
        |│  │  │  │  │  │  ╰─ Obj
        |│  │  │  │  │  │     ├─ Key(0: _.src[1].id)
        |│  │  │  │  │  │     ╰─ Key(content: _.src)
        |│  │  │  │  │  ╰─ Scope(Map())
        |│  │  │  │  ├─ $GroupF
        |│  │  │  │  │  ├─ Grouped
        |│  │  │  │  │  │  ╰─ Name("0" -> { "$push": "$content" })
        |│  │  │  │  │  ╰─ By
        |│  │  │  │  │     ╰─ Name("0" -> "$0")
        |│  │  │  │  ╰─ $ProjectF
        |│  │  │  │     ├─ Name("_id" -> "$_id")
        |│  │  │  │     ├─ Name("value")
        |│  │  │  │     │  ├─ Name("left" -> "$0")
        |│  │  │  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │  │  │     │  ╰─ Name("_id" -> "$_id")
        |│  │  │  │     ╰─ IncludeId
        |│  │  │  ╰─ Chain
        |│  │  │     ├─ $ReadF(db; bar)
        |│  │  │     ├─ $SimpleMapF
        |│  │  │     │  ├─ Map
        |│  │  │     │  │  ╰─ Let(__val)
        |│  │  │     │  │     ├─ JsCore([_._id, _])
        |│  │  │     │  │     ╰─ Obj
        |│  │  │     │  │        ├─ Key(0: __val[1])
        |│  │  │     │  │        ├─ Key(1: true)
        |│  │  │     │  │        ╰─ Key(src: __val)
        |│  │  │     │  ╰─ Scope(Map())
        |│  │  │     ├─ $MatchF
        |│  │  │     │  ╰─ And
        |│  │  │     │     ├─ Doc
        |│  │  │     │     │  ╰─ Expr($0 -> Type(Doc))
        |│  │  │     │     ╰─ Doc
        |│  │  │     │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│  │  │     ├─ $MapF
        |│  │  │     │  ├─ JavaScript(function (key, value) {
        |│  │  │     │  │               return [{ "0": value.src[1].foo_id }, { "left": [], "right": [value.src] }]
        |│  │  │     │  │             })
        |│  │  │     │  ╰─ Scope(Map())
        |│  │  │     ╰─ $ReduceF
        |│  │  │        ├─ JavaScript(function (key, values) {
        |│  │  │        │               var result = { "left": [], "right": [] };
        |│  │  │        │               values.forEach(
        |│  │  │        │                 function (value) {
        |│  │  │        │                   result.left = result.left.concat(value.left);
        |│  │  │        │                   result.right = result.right.concat(value.right)
        |│  │  │        │                 });
        |│  │  │        │               return result
        |│  │  │        │             })
        |│  │  │        ╰─ Scope(Map())
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ Doc
        |│  │  │     ├─ NotExpr($left -> Size(0))
        |│  │  │     ╰─ NotExpr($right -> Size(0))
        |│  │  ├─ $UnwindF(DocField(BsonField.Name("right")))
        |│  │  ├─ $UnwindF(DocField(BsonField.Name("left")))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Let(__val)
        |│  │  │  │     ├─ JsCore([[_.left[0], _.left[1]], [_.right[0], _.right[1]]])
        |│  │  │  │     ╰─ Obj
        |│  │  │  │        ├─ Key(0: __val[1][1])
        |│  │  │  │        ├─ Key(1: true)
        |│  │  │  │        ╰─ Key(src: __val)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ And
        |│  │  │     ├─ Doc
        |│  │  │     │  ╰─ Expr($0 -> Type(Doc))
        |│  │  │     ╰─ Doc
        |│  │  │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Obj
        |│  │  │  │     ├─ Key(0: _.src[1][1].id)
        |│  │  │  │     ╰─ Key(content: _.src)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$content" })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> "$0")
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; baz)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Let(__val)
        |│     │  │     ├─ JsCore([_._id, _])
        |│     │  │     ╰─ Obj
        |│     │  │        ├─ Key(0: __val[1])
        |│     │  │        ├─ Key(1: true)
        |│     │  │        ╰─ Key(src: __val)
        |│     │  ╰─ Scope(Map())
        |│     ├─ $MatchF
        |│     │  ╰─ And
        |│     │     ├─ Doc
        |│     │     │  ╰─ Expr($0 -> Type(Doc))
        |│     │     ╰─ Doc
        |│     │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) {
        |│     │  │               return [{ "0": value.src[1].bar_id }, { "left": [], "right": [value.src] }]
        |│     │  │             })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $ProjectF
        |│  ├─ Name("left" -> {
        |│  │       "$cond": [
        |│  │         { "$eq": [{ "$size": "$left" }, { "$literal": NumberInt("0") }] },
        |│  │         { "$literal": [{  }] },
        |│  │         "$left"]
        |│  │     })
        |│  ├─ Name("right" -> "$right")
        |│  ╰─ IgnoreId
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(name)
        |│  │     │  ╰─ If
        |│  │     │     ├─ BinOp(&&)
        |│  │     │     │  ├─ Call
        |│  │     │     │  │  ├─ Ident(isObject)
        |│  │     │     │  │  ╰─ Obj
        |│  │     │     │  │     ├─ Key(left: _.left[0][1])
        |│  │     │     │  │     ╰─ Key(right: _.left[1][1])
        |│  │     │     │  ╰─ UnOp(!)
        |│  │     │     │     ╰─ Call
        |│  │     │     │        ├─ JsCore(Array.isArray)
        |│  │     │     │        ╰─ Obj
        |│  │     │     │           ├─ Key(left: _.left[0][1])
        |│  │     │     │           ╰─ Key(right: _.left[1][1])
        |│  │     │     ├─ JsCore((isObject(_.left[0][1]) && (! Array.isArray(_.left[0][1]))) ? _.left[0][1].name : undefined)
        |│  │     │     ╰─ Ident(undefined)
        |│  │     ├─ Key(address)
        |│  │     │  ╰─ If
        |│  │     │     ├─ BinOp(&&)
        |│  │     │     │  ├─ Call
        |│  │     │     │  │  ├─ Ident(isObject)
        |│  │     │     │  │  ╰─ Obj
        |│  │     │     │  │     ├─ Key(left: _.left[0][1])
        |│  │     │     │  │     ╰─ Key(right: _.left[1][1])
        |│  │     │     │  ╰─ UnOp(!)
        |│  │     │     │     ╰─ Call
        |│  │     │     │        ├─ JsCore(Array.isArray)
        |│  │     │     │        ╰─ Obj
        |│  │     │     │           ├─ Key(left: _.left[0][1])
        |│  │     │     │           ╰─ Key(right: _.left[1][1])
        |│  │     │     ├─ JsCore((isObject(_.left[1][1]) && (! Array.isArray(_.left[1][1]))) ? _.left[1][1].address : undefined)
        |│  │     │     ╰─ Ident(undefined)
        |│  │     ╰─ Key(zip: (isObject(_.right[1]) && (! Array.isArray(_.right[1]))) ? _.right[1].zip : undefined)
        |│  ╰─ Scope(Map())
        |╰─ $ProjectF
        |   ├─ Name("name" -> true)
        |   ├─ Name("address" -> true)
        |   ├─ Name("zip" -> true)
        |   ╰─ IgnoreId""".stripMargin)

    "plan 3-way equi-join ($lookup)" in {
      plan3_4(
        sqlE"select foo.name, bar.address, baz.zip from foo join bar on foo.id = bar.foo_id join baz on bar.id = baz.bar_id",
        defaultStats,
        indexes(
          collection("db", "bar") -> BsonField.Name("foo_id"),
          collection("db", "baz") -> BsonField.Name("bar_id"))) must
        beWorkflow0(chain[Workflow](
          $read(collection("db", "foo")),
          $match(Selector.Doc(
            BsonField.Name("id") -> Selector.Exists(true))),
          $project(reshape(JoinDir.Left.name -> $$ROOT)),
          $lookup(
            CollectionName("bar"),
            JoinHandler.LeftName \ BsonField.Name("id"),
            BsonField.Name("foo_id"),
            JoinHandler.RightName),
          $unwind(DocField(JoinHandler.RightName)),
          $match(Selector.Doc(
            JoinHandler.RightName \ BsonField.Name("id") -> Selector.Exists(true))),
          $project(reshape(JoinDir.Left.name -> $$ROOT)),
          $lookup(
            CollectionName("baz"),
            JoinHandler.LeftName \ JoinHandler.RightName \ BsonField.Name("id"),
            BsonField.Name("bar_id"),
            JoinHandler.RightName),
          $unwind(DocField(JoinHandler.RightName)),
          $project(reshape(
            "name" ->
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
            "zip" ->
              $cond(
                $and(
                  $lte($literal(Bson.Doc()), $field(JoinDir.Right.name)),
                  $lt($field(JoinDir.Right.name), $literal(Bson.Arr()))),
                $field(JoinDir.Right.name, "zip"),
                $literal(Bson.Undefined))),
            IgnoreId)))
    }.pendingWithActual("#1560",
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $FoldLeftF
        |│  │  │  ├─ Chain
        |│  │  │  │  ├─ $ReadF(db; foo)
        |│  │  │  │  ├─ $SimpleMapF
        |│  │  │  │  │  ├─ Map
        |│  │  │  │  │  │  ╰─ JsCore([_._id, _])
        |│  │  │  │  │  ╰─ Scope(Map())
        |│  │  │  │  ├─ $ProjectF
        |│  │  │  │  │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│  │  │  │  │  ├─ Name("1" -> { "$literal": true })
        |│  │  │  │  │  ├─ Name("src" -> "$$ROOT")
        |│  │  │  │  │  ╰─ IgnoreId
        |│  │  │  │  ├─ $MatchF
        |│  │  │  │  │  ╰─ And
        |│  │  │  │  │     ├─ Doc
        |│  │  │  │  │     │  ╰─ Expr($0 -> Type(Doc))
        |│  │  │  │  │     ╰─ Doc
        |│  │  │  │  │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│  │  │  │  ├─ $SimpleMapF
        |│  │  │  │  │  ├─ Map
        |│  │  │  │  │  │  ╰─ Obj
        |│  │  │  │  │  │     ├─ Key(0: _.src[1].id)
        |│  │  │  │  │  │     ╰─ Key(content: _.src)
        |│  │  │  │  │  ╰─ Scope(Map())
        |│  │  │  │  ├─ $GroupF
        |│  │  │  │  │  ├─ Grouped
        |│  │  │  │  │  │  ╰─ Name("0" -> { "$push": "$content" })
        |│  │  │  │  │  ╰─ By
        |│  │  │  │  │     ╰─ Name("0" -> "$0")
        |│  │  │  │  ╰─ $ProjectF
        |│  │  │  │     ├─ Name("_id" -> "$_id")
        |│  │  │  │     ├─ Name("value")
        |│  │  │  │     │  ├─ Name("left" -> "$0")
        |│  │  │  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │  │  │     │  ╰─ Name("_id" -> "$_id")
        |│  │  │  │     ╰─ IncludeId
        |│  │  │  ╰─ Chain
        |│  │  │     ├─ $ReadF(db; bar)
        |│  │  │     ├─ $SimpleMapF
        |│  │  │     │  ├─ Map
        |│  │  │     │  │  ╰─ JsCore([_._id, _])
        |│  │  │     │  ╰─ Scope(Map())
        |│  │  │     ├─ $ProjectF
        |│  │  │     │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│  │  │     │  ├─ Name("1" -> { "$literal": true })
        |│  │  │     │  ├─ Name("src" -> "$$ROOT")
        |│  │  │     │  ╰─ IgnoreId
        |│  │  │     ├─ $MatchF
        |│  │  │     │  ╰─ And
        |│  │  │     │     ├─ Doc
        |│  │  │     │     │  ╰─ Expr($0 -> Type(Doc))
        |│  │  │     │     ╰─ Doc
        |│  │  │     │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│  │  │     ├─ $MapF
        |│  │  │     │  ├─ JavaScript(function (key, value) {
        |│  │  │     │  │               return [{ "0": value.src[1].foo_id }, { "left": [], "right": [value.src] }]
        |│  │  │     │  │             })
        |│  │  │     │  ╰─ Scope(Map())
        |│  │  │     ╰─ $ReduceF
        |│  │  │        ├─ JavaScript(function (key, values) {
        |│  │  │        │               var result = { "left": [], "right": [] };
        |│  │  │        │               values.forEach(
        |│  │  │        │                 function (value) {
        |│  │  │        │                   result.left = result.left.concat(value.left);
        |│  │  │        │                   result.right = result.right.concat(value.right)
        |│  │  │        │                 });
        |│  │  │        │               return result
        |│  │  │        │             })
        |│  │  │        ╰─ Scope(Map())
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ Doc
        |│  │  │     ├─ NotExpr($left -> Size(0))
        |│  │  │     ╰─ NotExpr($right -> Size(0))
        |│  │  ├─ $UnwindF(DocField(BsonField.Name("right")))
        |│  │  ├─ $UnwindF(DocField(BsonField.Name("left")))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ JsCore([[_.left[0], _.left[1]], [_.right[0], _.right[1]]])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $ProjectF
        |│  │  │  ├─ Name("0" -> {
        |│  │  │  │       "$arrayElemAt": [
        |│  │  │  │         { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] },
        |│  │  │  │         { "$literal": NumberInt("1") }]
        |│  │  │  │     })
        |│  │  │  ├─ Name("1" -> { "$literal": true })
        |│  │  │  ├─ Name("src" -> "$$ROOT")
        |│  │  │  ╰─ IgnoreId
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ And
        |│  │  │     ├─ Doc
        |│  │  │     │  ╰─ Expr($0 -> Type(Doc))
        |│  │  │     ╰─ Doc
        |│  │  │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Obj
        |│  │  │  │     ├─ Key(0: _.src[1][1].id)
        |│  │  │  │     ╰─ Key(content: _.src)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$content" })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> "$0")
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; baz)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ JsCore([_._id, _])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $ProjectF
        |│     │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│     │  ├─ Name("1" -> { "$literal": true })
        |│     │  ├─ Name("src" -> "$$ROOT")
        |│     │  ╰─ IgnoreId
        |│     ├─ $MatchF
        |│     │  ╰─ And
        |│     │     ├─ Doc
        |│     │     │  ╰─ Expr($0 -> Type(Doc))
        |│     │     ╰─ Doc
        |│     │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) {
        |│     │  │               return [{ "0": value.src[1].bar_id }, { "left": [], "right": [value.src] }]
        |│     │  │             })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(name)
        |│  │     │  ╰─ If
        |│  │     │     ├─ BinOp(&&)
        |│  │     │     │  ├─ Call
        |│  │     │     │  │  ├─ Ident(isObject)
        |│  │     │     │  │  ╰─ Obj
        |│  │     │     │  │     ├─ Key(left: _.left[0][1])
        |│  │     │     │  │     ╰─ Key(right: _.left[1][1])
        |│  │     │     │  ╰─ UnOp(!)
        |│  │     │     │     ╰─ Call
        |│  │     │     │        ├─ JsCore(Array.isArray)
        |│  │     │     │        ╰─ Obj
        |│  │     │     │           ├─ Key(left: _.left[0][1])
        |│  │     │     │           ╰─ Key(right: _.left[1][1])
        |│  │     │     ├─ JsCore((isObject(_.left[0][1]) && (! Array.isArray(_.left[0][1]))) ? _.left[0][1].name : undefined)
        |│  │     │     ╰─ Ident(undefined)
        |│  │     ├─ Key(address)
        |│  │     │  ╰─ If
        |│  │     │     ├─ BinOp(&&)
        |│  │     │     │  ├─ Call
        |│  │     │     │  │  ├─ Ident(isObject)
        |│  │     │     │  │  ╰─ Obj
        |│  │     │     │  │     ├─ Key(left: _.left[0][1])
        |│  │     │     │  │     ╰─ Key(right: _.left[1][1])
        |│  │     │     │  ╰─ UnOp(!)
        |│  │     │     │     ╰─ Call
        |│  │     │     │        ├─ JsCore(Array.isArray)
        |│  │     │     │        ╰─ Obj
        |│  │     │     │           ├─ Key(left: _.left[0][1])
        |│  │     │     │           ╰─ Key(right: _.left[1][1])
        |│  │     │     ├─ JsCore((isObject(_.left[1][1]) && (! Array.isArray(_.left[1][1]))) ? _.left[1][1].address : undefined)
        |│  │     │     ╰─ Ident(undefined)
        |│  │     ╰─ Key(zip: (isObject(_.right[1]) && (! Array.isArray(_.right[1]))) ? _.right[1].zip : undefined)
        |│  ╰─ Scope(Map())
        |╰─ $ProjectF
        |   ├─ Name("name" -> true)
        |   ├─ Name("address" -> true)
        |   ├─ Name("zip" -> true)
        |   ╰─ IgnoreId""".stripMargin)

    "plan count of $lookup" in {
      plan3_4(
        sqlE"select tp.`_id`, count(*) from `zips` as tp join `largeZips` as ti on tp.`_id` = ti.TestProgramId group by tp.`_id`",
        defaultStats,
        indexes()) must
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
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $FoldLeftF
        |│  │  │  ├─ Chain
        |│  │  │  │  ├─ $ReadF(db; zips)
        |│  │  │  │  ├─ $SimpleMapF
        |│  │  │  │  │  ├─ Map
        |│  │  │  │  │  │  ╰─ JsCore([_._id, _])
        |│  │  │  │  │  ╰─ Scope(Map())
        |│  │  │  │  ├─ $ProjectF
        |│  │  │  │  │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│  │  │  │  │  ├─ Name("1" -> { "$literal": true })
        |│  │  │  │  │  ├─ Name("src" -> "$$ROOT")
        |│  │  │  │  │  ╰─ IgnoreId
        |│  │  │  │  ├─ $MatchF
        |│  │  │  │  │  ╰─ And
        |│  │  │  │  │     ├─ Doc
        |│  │  │  │  │     │  ╰─ Expr($0 -> Type(Doc))
        |│  │  │  │  │     ╰─ Doc
        |│  │  │  │  │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│  │  │  │  ├─ $SimpleMapF
        |│  │  │  │  │  ├─ Map
        |│  │  │  │  │  │  ╰─ Obj
        |│  │  │  │  │  │     ├─ Key(0: _.src[1]._id)
        |│  │  │  │  │  │     ╰─ Key(content: _.src)
        |│  │  │  │  │  ╰─ Scope(Map())
        |│  │  │  │  ├─ $GroupF
        |│  │  │  │  │  ├─ Grouped
        |│  │  │  │  │  │  ╰─ Name("0" -> { "$push": "$content" })
        |│  │  │  │  │  ╰─ By
        |│  │  │  │  │     ╰─ Name("0" -> "$0")
        |│  │  │  │  ╰─ $ProjectF
        |│  │  │  │     ├─ Name("_id" -> "$_id")
        |│  │  │  │     ├─ Name("value")
        |│  │  │  │     │  ├─ Name("left" -> "$0")
        |│  │  │  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │  │  │     │  ╰─ Name("_id" -> "$_id")
        |│  │  │  │     ╰─ IncludeId
        |│  │  │  ╰─ Chain
        |│  │  │     ├─ $ReadF(db; largeZips)
        |│  │  │     ├─ $SimpleMapF
        |│  │  │     │  ├─ Map
        |│  │  │     │  │  ╰─ JsCore([_._id, _])
        |│  │  │     │  ╰─ Scope(Map())
        |│  │  │     ├─ $ProjectF
        |│  │  │     │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│  │  │     │  ├─ Name("1" -> { "$literal": true })
        |│  │  │     │  ├─ Name("src" -> "$$ROOT")
        |│  │  │     │  ╰─ IgnoreId
        |│  │  │     ├─ $MatchF
        |│  │  │     │  ╰─ And
        |│  │  │     │     ├─ Doc
        |│  │  │     │     │  ╰─ Expr($0 -> Type(Doc))
        |│  │  │     │     ╰─ Doc
        |│  │  │     │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│  │  │     ├─ $MapF
        |│  │  │     │  ├─ JavaScript(function (key, value) {
        |│  │  │     │  │               return [
        |│  │  │     │  │                 { "0": value.src[1].TestProgramId },
        |│  │  │     │  │                 { "left": [], "right": [value.src] }]
        |│  │  │     │  │             })
        |│  │  │     │  ╰─ Scope(Map())
        |│  │  │     ╰─ $ReduceF
        |│  │  │        ├─ JavaScript(function (key, values) {
        |│  │  │        │               var result = { "left": [], "right": [] };
        |│  │  │        │               values.forEach(
        |│  │  │        │                 function (value) {
        |│  │  │        │                   result.left = result.left.concat(value.left);
        |│  │  │        │                   result.right = result.right.concat(value.right)
        |│  │  │        │                 });
        |│  │  │        │               return result
        |│  │  │        │             })
        |│  │  │        ╰─ Scope(Map())
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ Doc
        |│  │  │     ├─ NotExpr($left -> Size(0))
        |│  │  │     ╰─ NotExpr($right -> Size(0))
        |│  │  ├─ $UnwindF(DocField(BsonField.Name("right")))
        |│  │  ├─ $UnwindF(DocField(BsonField.Name("left")))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Arr
        |│  │  │  │     ├─ Access
        |│  │  │  │     │  ├─ Obj
        |│  │  │  │     │  │  ├─ Key(left: _.left[1])
        |│  │  │  │     │  │  ╰─ Key(right: _.right[1])
        |│  │  │  │     │  ╰─ Literal(0)
        |│  │  │  │     ├─ JsCore([
        |│  │  │  │     │         (isObject(_.left[1]) && (! Array.isArray(_.left[1]))) ? _.left[1]._id : undefined])
        |│  │  │  │     ├─ JsCore(_.left[1])
        |│  │  │  │     ├─ JsCore(_.left[1]._id)
        |│  │  │  │     ╰─ Arr
        |│  │  │  │        ├─ Access
        |│  │  │  │        │  ├─ Obj
        |│  │  │  │        │  │  ├─ Key(left: _.left[1])
        |│  │  │  │        │  │  ╰─ Key(right: _.right[1])
        |│  │  │  │        │  ╰─ Literal(0)
        |│  │  │  │        ├─ JsCore([
        |│  │  │  │        │         (isObject(_.left[1]) && (! Array.isArray(_.left[1]))) ? _.left[1]._id : undefined])
        |│  │  │  │        ├─ Arr
        |│  │  │  │        │  ├─ Access
        |│  │  │  │        │  │  ├─ Obj
        |│  │  │  │        │  │  │  ├─ Key(left: _.left[1])
        |│  │  │  │        │  │  │  ╰─ Key(right: _.right[1])
        |│  │  │  │        │  │  ╰─ Literal(0)
        |│  │  │  │        │  ╰─ JsCore([
        |│  │  │  │        │            (isObject(_.left[1]) && (! Array.isArray(_.left[1]))) ? _.left[1]._id : undefined])
        |│  │  │  │        ├─ JsCore(_.right[1])
        |│  │  │  │        ╰─ If
        |│  │  │  │           ├─ JsCore(isObject(_.left[1]) && (! Array.isArray(_.left[1])))
        |│  │  │  │           ├─ SpliceObjects
        |│  │  │  │           │  ├─ JsCore(_.left[1])
        |│  │  │  │           │  ╰─ JsCore(_.right[1])
        |│  │  │  │           ╰─ Ident(undefined)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("f0" -> {
        |│  │  │  │          "$first": {
        |│  │  │  │            "$cond": [
        |│  │  │  │              {
        |│  │  │  │                "$and": [
        |│  │  │  │                  {
        |│  │  │  │                    "$lte": [
        |│  │  │  │                      { "$literal": {  } },
        |│  │  │  │                      { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("2") }] }]
        |│  │  │  │                  },
        |│  │  │  │                  {
        |│  │  │  │                    "$lt": [
        |│  │  │  │                      { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("2") }] },
        |│  │  │  │                      { "$literal": [] }]
        |│  │  │  │                  }]
        |│  │  │  │              },
        |│  │  │  │              { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("3") }] },
        |│  │  │  │              { "$literal": undefined }]
        |│  │  │  │          }
        |│  │  │  │        })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ JsCore([[_._id["0"]], _.f0])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$$ROOT" })
        |│  │  │  ╰─ By
        |│  │  │     ╰─ Name("0" -> {
        |│  │  │             "$arrayElemAt": [
        |│  │  │               { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("0") }] },
        |│  │  │               { "$literal": NumberInt("0") }]
        |│  │  │           })
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $FoldLeftF
        |│     │  ├─ Chain
        |│     │  │  ├─ $ReadF(db; zips)
        |│     │  │  ├─ $SimpleMapF
        |│     │  │  │  ├─ Map
        |│     │  │  │  │  ╰─ JsCore([_._id, _])
        |│     │  │  │  ╰─ Scope(Map())
        |│     │  │  ├─ $ProjectF
        |│     │  │  │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│     │  │  │  ├─ Name("1" -> { "$literal": true })
        |│     │  │  │  ├─ Name("src" -> "$$ROOT")
        |│     │  │  │  ╰─ IgnoreId
        |│     │  │  ├─ $MatchF
        |│     │  │  │  ╰─ And
        |│     │  │  │     ├─ Doc
        |│     │  │  │     │  ╰─ Expr($0 -> Type(Doc))
        |│     │  │  │     ╰─ Doc
        |│     │  │  │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│     │  │  ├─ $SimpleMapF
        |│     │  │  │  ├─ Map
        |│     │  │  │  │  ╰─ Obj
        |│     │  │  │  │     ├─ Key(0: _.src[1]._id)
        |│     │  │  │  │     ╰─ Key(content: _.src)
        |│     │  │  │  ╰─ Scope(Map())
        |│     │  │  ├─ $GroupF
        |│     │  │  │  ├─ Grouped
        |│     │  │  │  │  ╰─ Name("0" -> { "$push": "$content" })
        |│     │  │  │  ╰─ By
        |│     │  │  │     ╰─ Name("0" -> "$0")
        |│     │  │  ╰─ $ProjectF
        |│     │  │     ├─ Name("_id" -> "$_id")
        |│     │  │     ├─ Name("value")
        |│     │  │     │  ├─ Name("left" -> "$0")
        |│     │  │     │  ├─ Name("right" -> { "$literal": [] })
        |│     │  │     │  ╰─ Name("_id" -> "$_id")
        |│     │  │     ╰─ IncludeId
        |│     │  ╰─ Chain
        |│     │     ├─ $ReadF(db; largeZips)
        |│     │     ├─ $SimpleMapF
        |│     │     │  ├─ Map
        |│     │     │  │  ╰─ JsCore([_._id, _])
        |│     │     │  ╰─ Scope(Map())
        |│     │     ├─ $ProjectF
        |│     │     │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│     │     │  ├─ Name("1" -> { "$literal": true })
        |│     │     │  ├─ Name("src" -> "$$ROOT")
        |│     │     │  ╰─ IgnoreId
        |│     │     ├─ $MatchF
        |│     │     │  ╰─ And
        |│     │     │     ├─ Doc
        |│     │     │     │  ╰─ Expr($0 -> Type(Doc))
        |│     │     │     ╰─ Doc
        |│     │     │        ╰─ Expr($1 -> Eq(Bool(true)))
        |│     │     ├─ $MapF
        |│     │     │  ├─ JavaScript(function (key, value) {
        |│     │     │  │               return [
        |│     │     │  │                 { "0": value.src[1].TestProgramId },
        |│     │     │  │                 { "left": [], "right": [value.src] }]
        |│     │     │  │             })
        |│     │     │  ╰─ Scope(Map())
        |│     │     ╰─ $ReduceF
        |│     │        ├─ JavaScript(function (key, values) {
        |│     │        │               var result = { "left": [], "right": [] };
        |│     │        │               values.forEach(
        |│     │        │                 function (value) {
        |│     │        │                   result.left = result.left.concat(value.left);
        |│     │        │                   result.right = result.right.concat(value.right)
        |│     │        │                 });
        |│     │        │               return result
        |│     │        │             })
        |│     │        ╰─ Scope(Map())
        |│     ├─ $MatchF
        |│     │  ╰─ Doc
        |│     │     ├─ NotExpr($left -> Size(0))
        |│     │     ╰─ NotExpr($right -> Size(0))
        |│     ├─ $UnwindF(DocField(BsonField.Name("right")))
        |│     ├─ $UnwindF(DocField(BsonField.Name("left")))
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Arr
        |│     │  │     ├─ Access
        |│     │  │     │  ├─ Obj
        |│     │  │     │  │  ├─ Key(left: _.left[1])
        |│     │  │     │  │  ╰─ Key(right: _.right[1])
        |│     │  │     │  ╰─ Literal(0)
        |│     │  │     ├─ JsCore([
        |│     │  │     │         (isObject(_.left[1]) && (! Array.isArray(_.left[1]))) ? _.left[1]._id : undefined])
        |│     │  │     ├─ JsCore(_.left[1])
        |│     │  │     ├─ JsCore(_.left[1]._id)
        |│     │  │     ╰─ Arr
        |│     │  │        ├─ Access
        |│     │  │        │  ├─ Obj
        |│     │  │        │  │  ├─ Key(left: _.left[1])
        |│     │  │        │  │  ╰─ Key(right: _.right[1])
        |│     │  │        │  ╰─ Literal(0)
        |│     │  │        ├─ JsCore([
        |│     │  │        │         (isObject(_.left[1]) && (! Array.isArray(_.left[1]))) ? _.left[1]._id : undefined])
        |│     │  │        ├─ Arr
        |│     │  │        │  ├─ Access
        |│     │  │        │  │  ├─ Obj
        |│     │  │        │  │  │  ├─ Key(left: _.left[1])
        |│     │  │        │  │  │  ╰─ Key(right: _.right[1])
        |│     │  │        │  │  ╰─ Literal(0)
        |│     │  │        │  ╰─ JsCore([
        |│     │  │        │            (isObject(_.left[1]) && (! Array.isArray(_.left[1]))) ? _.left[1]._id : undefined])
        |│     │  │        ├─ JsCore(_.right[1])
        |│     │  │        ╰─ If
        |│     │  │           ├─ JsCore(isObject(_.left[1]) && (! Array.isArray(_.left[1])))
        |│     │  │           ├─ SpliceObjects
        |│     │  │           │  ├─ JsCore(_.left[1])
        |│     │  │           │  ╰─ JsCore(_.right[1])
        |│     │  │           ╰─ Ident(undefined)
        |│     │  ╰─ Scope(Map())
        |│     ├─ $GroupF
        |│     │  ├─ Grouped
        |│     │  │  ╰─ Name("f0" -> { "$sum": { "$literal": NumberInt("1") } })
        |│     │  ╰─ By
        |│     │     ╰─ Name("0" -> {
        |│     │             "$arrayElemAt": [
        |│     │               { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("4") }] },
        |│     │               { "$literal": NumberInt("1") }]
        |│     │           })
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ JsCore([[_._id["0"]], _.f0])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) { return [{ "0": value[0][0] }, { "left": [], "right": [value] }] })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |╰─ $ProjectF
        |   ├─ Name("_id" -> { "$arrayElemAt": ["$left", { "$literal": NumberInt("1") }] })
        |   ├─ Name("1" -> { "$arrayElemAt": ["$right", { "$literal": NumberInt("1") }] })
        |   ╰─ IncludeId""".stripMargin)

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
    }.pendingWithActual("#1560",
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; slamengine_commits)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Let(__val)
        |│  │  │  │     ├─ JsCore([_._id, _])
        |│  │  │  │     ╰─ JsCore([
        |│  │  │  │               __val[0],
        |│  │  │  │               [__val[0]],
        |│  │  │  │               (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1] : undefined,
        |│  │  │  │               (((isObject(
        |│  │  │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].parents[0] : undefined) && (! Array.isArray(
        |│  │  │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].parents[0] : undefined))) ? true : false) && (Array.isArray(
        |│  │  │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].parents : undefined) ? true : false)) && ((isObject(
        |│  │  │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].author : undefined) && (! Array.isArray(
        |│  │  │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].author : undefined))) ? true : false),
        |│  │  │  │               [
        |│  │  │  │                 __val[0],
        |│  │  │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1] : undefined,
        |│  │  │  │                 (isObject(
        |│  │  │  │                   (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].author : undefined) && (! Array.isArray(
        |│  │  │  │                   (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].author : undefined))) ? true : false]])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $ProjectF
        |│  │  │  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("3") }] })
        |│  │  │  ├─ Name("src" -> "$$ROOT")
        |│  │  │  ╰─ IgnoreId
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ Doc
        |│  │  │     ╰─ Expr($0 -> Eq(Bool(true)))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Obj
        |│  │  │  │     ├─ Key(0: _.src[2].parents[0].sha)
        |│  │  │  │     ├─ Key(1: _.src[2].author.login)
        |│  │  │  │     ╰─ Key(content: _.src)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$content" })
        |│  │  │  ╰─ By
        |│  │  │     ├─ Name("0" -> "$0")
        |│  │  │     ╰─ Name("1" -> "$1")
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; slamengine_commits)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Let(__val)
        |│     │  │     ├─ JsCore([_._id, _])
        |│     │  │     ╰─ JsCore([
        |│     │  │               __val[0],
        |│     │  │               [__val[0]],
        |│     │  │               (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1] : undefined,
        |│     │  │               (((isObject(
        |│     │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].parents[0] : undefined) && (! Array.isArray(
        |│     │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].parents[0] : undefined))) ? true : false) && (Array.isArray(
        |│     │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].parents : undefined) ? true : false)) && ((isObject(
        |│     │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].author : undefined) && (! Array.isArray(
        |│     │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].author : undefined))) ? true : false),
        |│     │  │               [
        |│     │  │                 __val[0],
        |│     │  │                 (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1] : undefined,
        |│     │  │                 (isObject(
        |│     │  │                   (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].author : undefined) && (! Array.isArray(
        |│     │  │                   (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].author : undefined))) ? true : false]])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $ProjectF
        |│     │  ├─ Name("0" -> {
        |│     │  │       "$arrayElemAt": [
        |│     │  │         { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("4") }] },
        |│     │  │         { "$literal": NumberInt("2") }]
        |│     │  │     })
        |│     │  ├─ Name("src" -> "$$ROOT")
        |│     │  ╰─ IgnoreId
        |│     ├─ $MatchF
        |│     │  ╰─ Doc
        |│     │     ╰─ Expr($0 -> Eq(Bool(true)))
        |│     ├─ $ProjectF
        |│     │  ├─ Name("0" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("4") }] })
        |│     │  ╰─ IgnoreId
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) {
        |│     │  │               return [
        |│     │  │                 { "0": value["0"][1].sha, "1": value["0"][1].author.login },
        |│     │  │                 { "left": [], "right": [value["0"]] }]
        |│     │  │             })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(child: (isObject(_.left[2]) && (! Array.isArray(_.left[2]))) ? _.left[2].sha : undefined)
        |│  │     ├─ Key(c_auth: ((isObject(_.left[2]) && (! Array.isArray(_.left[2]))) && (isObject(_.left[2].author) && (! Array.isArray(_.left[2].author)))) ? _.left[2].author.login : undefined)
        |│  │     ├─ Key(parent: (isObject(_.right[1]) && (! Array.isArray(_.right[1]))) ? _.right[1].sha : undefined)
        |│  │     ╰─ Key(p_auth: ((isObject(_.right[1]) && (! Array.isArray(_.right[1]))) && (isObject(_.right[1].author) && (! Array.isArray(_.right[1].author)))) ? _.right[1].author.login : undefined)
        |│  ╰─ Scope(Map())
        |╰─ $ProjectF
        |   ├─ Name("child" -> true)
        |   ├─ Name("c_auth" -> true)
        |   ├─ Name("parent" -> true)
        |   ├─ Name("p_auth" -> true)
        |   ╰─ IgnoreId""".stripMargin)

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
    }.pendingWithActual("#1560",
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; zips)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Let(__val)
        |│  │  │  │     ├─ JsCore([_._id, _])
        |│  │  │  │     ╰─ Obj
        |│  │  │  │        ├─ Key(0: Array.isArray(
        |│  │  │  │        │      (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].loc : undefined) ? true : false)
        |│  │  │  │        ╰─ Key(src: __val)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ Doc
        |│  │  │     ╰─ Expr($0 -> Eq(Bool(true)))
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Let(__val)
        |│  │  │  │     ├─ JsCore([
        |│  │  │  │     │         _.src[0],
        |│  │  │  │     │         (isObject(_.src[1]) && (! Array.isArray(_.src[1]))) ? _.src[1] : undefined,
        |│  │  │  │     │         Array.isArray(
        |│  │  │  │     │           (isObject(_.src[1]) && (! Array.isArray(_.src[1]))) ? _.src[1].loc : undefined) ? true : false])
        |│  │  │  │     ╰─ Obj
        |│  │  │  │        ├─ Key(s: __val)
        |│  │  │  │        ╰─ Key(f: __val[1].loc)
        |│  │  │  ├─ SubMap
        |│  │  │  │  ├─ JsCore(_.f)
        |│  │  │  │  ╰─ Let(m)
        |│  │  │  │     ├─ JsCore(_.f)
        |│  │  │  │     ╰─ Call
        |│  │  │  │        ├─ JsCore(Object.keys(m).map)
        |│  │  │  │        ╰─ Fun(Name(k))
        |│  │  │  │           ╰─ JsCore([k, m[k]])
        |│  │  │  ├─ Flatten
        |│  │  │  │  ╰─ JsCore(_.f)
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ JsCore([_.s, [_.s, _.f]])
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$$ROOT" })
        |│  │  │  ╰─ By
        |│  │  │     ├─ Name("0" -> {
        |│  │  │     │       "$arrayElemAt": [
        |│  │  │     │         { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] },
        |│  │  │     │         { "$literal": NumberInt("0") }]
        |│  │  │     │     })
        |│  │  │     ╰─ Name("1" -> {
        |│  │  │             "$arrayElemAt": [
        |│  │  │               {
        |│  │  │                 "$arrayElemAt": [
        |│  │  │                   { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] },
        |│  │  │                   { "$literal": NumberInt("1") }]
        |│  │  │               },
        |│  │  │               { "$literal": NumberInt("1") }]
        |│  │  │           })
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; zips)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Let(__val)
        |│     │  │     ├─ JsCore([_._id, _])
        |│     │  │     ╰─ Obj
        |│     │  │        ├─ Key(0: Array.isArray(
        |│     │  │        │      (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].loc : undefined) ? true : false)
        |│     │  │        ╰─ Key(src: __val)
        |│     │  ╰─ Scope(Map())
        |│     ├─ $MatchF
        |│     │  ╰─ Doc
        |│     │     ╰─ Expr($0 -> Eq(Bool(true)))
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Let(__val)
        |│     │  │     ├─ JsCore([
        |│     │  │     │         _.src[0],
        |│     │  │     │         (isObject(_.src[1]) && (! Array.isArray(_.src[1]))) ? _.src[1] : undefined,
        |│     │  │     │         Array.isArray(
        |│     │  │     │           (isObject(_.src[1]) && (! Array.isArray(_.src[1]))) ? _.src[1].loc : undefined) ? true : false])
        |│     │  │     ╰─ Obj
        |│     │  │        ├─ Key(s: __val)
        |│     │  │        ╰─ Key(f: __val[1].loc)
        |│     │  ├─ SubMap
        |│     │  │  ├─ JsCore(_.f)
        |│     │  │  ╰─ Let(m)
        |│     │  │     ├─ JsCore(_.f)
        |│     │  │     ╰─ Call
        |│     │  │        ├─ JsCore(Object.keys(m).map)
        |│     │  │        ╰─ Fun(Name(k))
        |│     │  │           ╰─ JsCore([k, m[k]])
        |│     │  ├─ Flatten
        |│     │  │  ╰─ JsCore(_.f)
        |│     │  ├─ Map
        |│     │  │  ╰─ JsCore([_.s, [_.s, _.f]])
        |│     │  ╰─ Scope(Map())
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) {
        |│     │  │               return [
        |│     │  │                 { "0": value[1][0], "1": value[1][1][1] },
        |│     │  │                 { "left": [], "right": [value] }]
        |│     │  │             })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(city1: (isObject(_.left[0][1]) && (! Array.isArray(_.left[0][1]))) ? _.left[0][1].city : undefined)
        |│  │     ├─ Key(loc: (isObject(_.left[0][1]) && (! Array.isArray(_.left[0][1]))) ? _.left[0][1].loc : undefined)
        |│  │     ├─ Key(city2: (isObject(_.right[0][1]) && (! Array.isArray(_.right[0][1]))) ? _.right[0][1].city : undefined)
        |│  │     ╰─ Key(pop: (isObject(_.right[0][1]) && (! Array.isArray(_.right[0][1]))) ? _.right[0][1].pop : undefined)
        |│  ╰─ Scope(Map())
        |╰─ $ProjectF
        |   ├─ Name("city1" -> true)
        |   ├─ Name("loc" -> true)
        |   ├─ Name("city2" -> true)
        |   ├─ Name("pop" -> true)
        |   ╰─ IgnoreId""".stripMargin)

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
              reshape("value" -> $field("__tmp11", "city")),
              ExcludeId)),
          false).op)
    }.pendingWithActual("#1560",
      """Chain
        |├─ $FoldLeftF
        |│  ├─ Chain
        |│  │  ├─ $ReadF(db; zips)
        |│  │  ├─ $SimpleMapF
        |│  │  │  ├─ Map
        |│  │  │  │  ╰─ Let(__val)
        |│  │  │  │     ├─ JsCore([_._id, _])
        |│  │  │  │     ╰─ Obj
        |│  │  │  │        ├─ Key(0: __val[1].pop)
        |│  │  │  │        ├─ Key(1: true)
        |│  │  │  │        ├─ Key(2: __val[1])
        |│  │  │  │        ├─ Key(3: true)
        |│  │  │  │        ╰─ Key(src: __val)
        |│  │  │  ╰─ Scope(Map())
        |│  │  ├─ $MatchF
        |│  │  │  ╰─ And
        |│  │  │     ├─ Or
        |│  │  │     │  ├─ Doc
        |│  │  │     │  │  ╰─ Expr($0 -> Type(Int32))
        |│  │  │     │  ├─ Doc
        |│  │  │     │  │  ╰─ Expr($0 -> Type(Int64))
        |│  │  │     │  ├─ Doc
        |│  │  │     │  │  ╰─ Expr($0 -> Type(Dec))
        |│  │  │     │  ├─ Doc
        |│  │  │     │  │  ╰─ Expr($0 -> Type(Text))
        |│  │  │     │  ├─ Doc
        |│  │  │     │  │  ╰─ Expr($0 -> Type(Date))
        |│  │  │     │  ╰─ Doc
        |│  │  │     │     ╰─ Expr($0 -> Type(Bool))
        |│  │  │     ├─ Doc
        |│  │  │     │  ╰─ Expr($1 -> Eq(Bool(true)))
        |│  │  │     ├─ Doc
        |│  │  │     │  ╰─ Expr($2 -> Type(Doc))
        |│  │  │     ╰─ Doc
        |│  │  │        ╰─ Expr($3 -> Eq(Bool(true)))
        |│  │  ├─ $GroupF
        |│  │  │  ├─ Grouped
        |│  │  │  │  ╰─ Name("0" -> { "$push": "$src" })
        |│  │  │  ╰─ By({ "$literal": null })
        |│  │  ╰─ $ProjectF
        |│  │     ├─ Name("_id" -> "$_id")
        |│  │     ├─ Name("value")
        |│  │     │  ├─ Name("left" -> "$0")
        |│  │     │  ├─ Name("right" -> { "$literal": [] })
        |│  │     │  ╰─ Name("_id" -> "$_id")
        |│  │     ╰─ IncludeId
        |│  ╰─ Chain
        |│     ├─ $ReadF(db; zips2)
        |│     ├─ $SimpleMapF
        |│     │  ├─ Map
        |│     │  │  ╰─ Let(__val)
        |│     │  │     ├─ JsCore([_._id, _])
        |│     │  │     ╰─ Obj
        |│     │  │        ├─ Key(0: __val[1].pop)
        |│     │  │        ├─ Key(1: true)
        |│     │  │        ├─ Key(2: __val[1])
        |│     │  │        ├─ Key(3: true)
        |│     │  │        ╰─ Key(src: __val)
        |│     │  ╰─ Scope(Map())
        |│     ├─ $MatchF
        |│     │  ╰─ And
        |│     │     ├─ Or
        |│     │     │  ├─ Doc
        |│     │     │  │  ╰─ Expr($0 -> Type(Int32))
        |│     │     │  ├─ Doc
        |│     │     │  │  ╰─ Expr($0 -> Type(Int64))
        |│     │     │  ├─ Doc
        |│     │     │  │  ╰─ Expr($0 -> Type(Dec))
        |│     │     │  ├─ Doc
        |│     │     │  │  ╰─ Expr($0 -> Type(Text))
        |│     │     │  ├─ Doc
        |│     │     │  │  ╰─ Expr($0 -> Type(Date))
        |│     │     │  ╰─ Doc
        |│     │     │     ╰─ Expr($0 -> Type(Bool))
        |│     │     ├─ Doc
        |│     │     │  ╰─ Expr($1 -> Eq(Bool(true)))
        |│     │     ├─ Doc
        |│     │     │  ╰─ Expr($2 -> Type(Doc))
        |│     │     ╰─ Doc
        |│     │        ╰─ Expr($3 -> Eq(Bool(true)))
        |│     ├─ $MapF
        |│     │  ├─ JavaScript(function (key, value) { return [null, { "left": [], "right": [value.src] }] })
        |│     │  ╰─ Scope(Map())
        |│     ╰─ $ReduceF
        |│        ├─ JavaScript(function (key, values) {
        |│        │               var result = { "left": [], "right": [] };
        |│        │               values.forEach(
        |│        │                 function (value) {
        |│        │                   result.left = result.left.concat(value.left);
        |│        │                   result.right = result.right.concat(value.right)
        |│        │                 });
        |│        │               return result
        |│        │             })
        |│        ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ├─ NotExpr($left -> Size(0))
        |│     ╰─ NotExpr($right -> Size(0))
        |├─ $UnwindF(DocField(BsonField.Name("right")))
        |├─ $UnwindF(DocField(BsonField.Name("left")))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ JsCore([
        |│  │     │         [
        |│  │     │           _.left[0],
        |│  │     │           [_.left[0]],
        |│  │     │           _.left[1],
        |│  │     │           ((((isNumber(_.left[1].pop) || ((_.left[1].pop instanceof NumberInt) || (_.left[1].pop instanceof NumberLong))) || isString(_.left[1].pop)) || ((_.left[1].pop instanceof Date) || ((typeof _.left[1].pop) === "boolean"))) ? true : false) && ((isObject(_.left[1]) && (! Array.isArray(_.left[1]))) ? true : false)],
        |│  │     │         [
        |│  │     │           _.right[0],
        |│  │     │           [_.right[0]],
        |│  │     │           _.right[1],
        |│  │     │           ((((isNumber(_.right[1].pop) || ((_.right[1].pop instanceof NumberInt) || (_.right[1].pop instanceof NumberLong))) || isString(_.right[1].pop)) || ((_.right[1].pop instanceof Date) || ((typeof _.right[1].pop) === "boolean"))) ? true : false) && ((isObject(_.right[1]) && (! Array.isArray(_.right[1]))) ? true : false)]])
        |│  │     ╰─ Obj
        |│  │        ├─ Key(0: true)
        |│  │        ├─ Key(1: __val[0][2].pop < __val[1][2].pop)
        |│  │        ╰─ Key(src: __val)
        |│  ╰─ Scope(Map())
        |├─ $MatchF
        |│  ╰─ And
        |│     ├─ Doc
        |│     │  ╰─ Expr($0 -> Eq(Bool(true)))
        |│     ╰─ Doc
        |│        ╰─ Expr($1 -> Eq(Bool(true)))
        |╰─ $SimpleMapF
        |   ├─ Map
        |   │  ╰─ JsCore((isObject(_.src[1][2]) && (! Array.isArray(_.src[1][2]))) ? _.src[1][2].city : undefined)
        |   ╰─ Scope(Map())""".stripMargin)

    def countOps(wf: Workflow, p: PartialFunction[WorkflowF[Fix[WorkflowF]], Boolean]): Int = {
      wf.foldMap(op => if (p.lift(op.unFix).getOrElse(false)) 1 else 0)
    }

    val WC = Inject[WorkflowOpCoreF, WorkflowF]

    def countAccumOps(wf: Workflow) = countOps(wf, { case WC($GroupF(_, _, _)) => true })
    def countUnwindOps(wf: Workflow) = countOps(wf, { case WC($UnwindF(_, _)) => true })
    def countMatchOps(wf: Workflow) = countOps(wf, { case WC($MatchF(_, _)) => true })

    def noConsecutiveProjectOps(wf: Workflow) =
      countOps(wf, { case WC($ProjectF(Embed(WC($ProjectF(_, _, _))), _, _)) => true }) aka "the occurrences of consecutive $project ops:" must_== 0
    def noConsecutiveSimpleMapOps(wf: Workflow) =
      countOps(wf, { case WC($SimpleMapF(Embed(WC($SimpleMapF(_, _, _))), _, _)) => true }) aka "the occurrences of consecutive $simpleMap ops:" must_== 0
    def maxAccumOps(wf: Workflow, max: Int) =
      countAccumOps(wf) aka "the number of $group ops:" must beLessThanOrEqualTo(max)
    def maxUnwindOps(wf: Workflow, max: Int) =
      countUnwindOps(wf) aka "the number of $unwind ops:" must beLessThanOrEqualTo(max)
    def maxMatchOps(wf: Workflow, max: Int) =
      countMatchOps(wf) aka "the number of $match ops:" must beLessThanOrEqualTo(max)
    def brokenProjectOps(wf: Workflow) =
      countOps(wf, { case WC($ProjectF(_, Reshape(shape), _)) => shape.isEmpty }) aka "$project ops with no fields"

    def danglingReferences(wf: Workflow) =
      wf.foldMap(_.unFix match {
        case IsSingleSource(op) =>
          simpleShape(op.src).map { shape =>
            val refs = Refs[WorkflowF].refs(op.wf)
            val missing = refs.collect { case v @ DocVar(_, Some(f)) if !shape.contains(f.flatten.head) => v }
            if (missing.isEmpty) Nil
            else List(missing.map(_.bson).mkString(", ") + " missing in\n" + Fix[WorkflowF](op.wf).render.shows)
          }.getOrElse(Nil)
        case _ => Nil
      }) aka "dangling references"

    def rootPushes(wf: Workflow) =
      wf.foldMap(_.unFix match {
        case WC(op @ $GroupF(src, Grouped(map), _)) if map.values.toList.contains($push($$ROOT)) && simpleShape(src).isEmpty => List(op)
        case _ => Nil
      }) aka "group ops pushing $$ROOT"

    def appropriateColumns0(wf: Workflow, q: Select[Fix[Sql]]) = {
      val fields = fieldNames(wf).map(_.filterNot(_ ≟ "_id"))
      fields aka "column order" must beSome(columnNames(q))
    }

    def appropriateColumns(wf: Workflow, q: Select[Fix[Sql]]) = {
      val fields = fieldNames(wf).map(_.filterNot(_ ≟ "_id"))
      (fields aka "column order" must beSome(columnNames(q))) or
        (fields must beSome(List("value"))) // NB: some edge cases (all constant projections) end up under "value" and aren't interesting anyway
    }

    "plan multiple reducing projections (all, distinct, orderBy)" >> Prop.forAll(select(distinct, maybeReducingExpr, Gen.option(filter), Gen.option(groupBySeveral), orderBySeveral)) { q =>
      plan(q.embed) must beRight.which { fop =>
        val wf = fop.op
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        maxAccumOps(wf, 2)
        maxUnwindOps(wf, 1)
        maxMatchOps(wf, 1)
        danglingReferences(wf) must_== Nil
        brokenProjectOps(wf) must_== 0
        appropriateColumns(wf, q)
        rootPushes(wf) must_== Nil
      }
    }.set(maxSize = 3).pendingUntilFixed(notOnPar)  // FIXME: with more then a few keys in the order by, the planner gets *very* slow (see SD-658)

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
    }

    "plan multiple reducing projections (all, distinct)" >> Prop.forAll(select(distinct, maybeReducingExpr, Gen.option(filter), Gen.option(groupBySeveral), noOrderBy)) { q =>
      plan(q.embed) must beRight.which { fop =>
        val wf = fop.op
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        maxAccumOps(wf, 2)
        maxUnwindOps(wf, 1)
        maxMatchOps(wf, 1)
        danglingReferences(wf) must_== Nil
        brokenProjectOps(wf) must_== 0
        appropriateColumns(wf, q)
        rootPushes(wf) must_== Nil
      }
    }.set(maxSize = 10).pendingUntilFixed(notOnPar)

    "plan multiple reducing projections (all)" >> Prop.forAll(select(notDistinct, maybeReducingExpr, Gen.option(filter), Gen.option(groupBySeveral), noOrderBy)) { q =>
      plan(q.embed) must beRight.which { fop =>
        val wf = fop.op
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        maxAccumOps(wf, 1)
        maxUnwindOps(wf, 1)
        maxMatchOps(wf, 1)
        danglingReferences(wf) must_== Nil
        brokenProjectOps(wf) must_== 0
        appropriateColumns0(wf, q)
        rootPushes(wf) must_== Nil
      }
    }.set(maxSize = 10).pendingUntilFixed(notOnPar)

    // NB: tighter constraint because we know there's no filter.
    "plan multiple reducing projections (no filter)" >> Prop.forAll(select(notDistinct, maybeReducingExpr, noFilter, Gen.option(groupBySeveral), noOrderBy)) { q =>
      plan(q.embed) must beRight.which { fop =>
        val wf = fop.op
        noConsecutiveProjectOps(wf)
        noConsecutiveSimpleMapOps(wf)
        maxAccumOps(wf, 1)
        maxUnwindOps(wf, 1)
        maxMatchOps(wf, 0)
        danglingReferences(wf) must_== Nil
        brokenProjectOps(wf) must_== 0
        appropriateColumns0(wf, q)
        rootPushes(wf) must_== Nil
      }
    }.set(maxSize = 10).pendingUntilFixed(notOnPar)
  }

  /**
    * @return The list of expected names for the projections of the selection
    */
  def columnNames(q: Select[Fix[Sql]]): List[String] =
    // TODO: Replace `get` with `valueOr` and an exception message detailing
    // what was the underlying assumption that proved to be wrong
    projectionNames(q.projections, None).toOption.get.map(_._1)

  def fieldNames(wf: Workflow): Option[List[String]] =
    simpleShape(wf).map(_.map(_.asText))

  val notDistinct = Gen.const(SelectAll)
  val distinct = Gen.const(SelectDistinct)

  val noGroupBy = Gen.const[Option[GroupBy[Fix[Sql]]]](None)
  val groupBySeveral = Gen.nonEmptyListOf(Gen.oneOf(
    sqlF.IdentR("state"),
    sqlF.IdentR("territory"))).map(keys => GroupBy(keys.distinct, None))

  val noFilter = Gen.const[Option[Fix[Sql]]](None)
  val filter = Gen.oneOf(
    for {
      x <- genInnerInt
    } yield sqlF.BinopR(x, sqlF.IntLiteralR(100), sql.Lt),
    for {
      x <- genInnerStr
    } yield sqlF.InvokeFunctionR(CIName("search"), List(x, sqlF.StringLiteralR("^BOULDER"), sqlF.BoolLiteralR(false))),
    Gen.const(sqlF.BinopR(sqlF.IdentR("p"), sqlF.IdentR("q"), sql.Eq)))  // Comparing two fields requires a $project before the $match

  val noOrderBy: Gen[Option[OrderBy[Fix[Sql]]]] = Gen.const(None)

  val orderBySeveral: Gen[Option[OrderBy[Fix[Sql]]]] = {
    val order = Gen.oneOf(ASC, DESC) tuple Gen.oneOf(genInnerInt, genInnerStr)
    (order |@| Gen.listOf(order))((h, t) => Some(OrderBy(NonEmptyList(h, t: _*))))
  }

  val maybeReducingExpr = Gen.oneOf(genOuterInt, genOuterStr)

  def select(distinctGen: Gen[IsDistinct], exprGen: Gen[Fix[Sql]], filterGen: Gen[Option[Fix[Sql]]], groupByGen: Gen[Option[GroupBy[Fix[Sql]]]], orderByGen: Gen[Option[OrderBy[Fix[Sql]]]]): Gen[Select[Fix[Sql]]] =
    for {
      distinct <- distinctGen
      projs    <- (genReduceInt ⊛ Gen.nonEmptyListOf(exprGen))(_ :: _).map(_.zipWithIndex.map {
        case (x, n) => Proj(x, Some("p" + n))
      })
      filter   <- filterGen
      groupBy  <- groupByGen
      orderBy  <- orderByGen
    } yield sql.Select(distinct, projs, Some(TableRelationAST(file("zips"), None)), filter, groupBy, orderBy)

  def genInnerInt = Gen.oneOf(
    sqlF.IdentR("pop"),
    // IntLiteralR(0),  // TODO: exposes bugs (see SD-478)
    sqlF.BinopR(sqlF.IdentR("pop"), sqlF.IntLiteralR(1), Minus), // an ExprOp
    sqlF.InvokeFunctionR(CIName("length"), List(sqlF.IdentR("city")))) // requires JS
  def genReduceInt = genInnerInt.flatMap(x => Gen.oneOf(
    x,
    sqlF.InvokeFunctionR(CIName("min"), List(x)),
    sqlF.InvokeFunctionR(CIName("max"), List(x)),
    sqlF.InvokeFunctionR(CIName("sum"), List(x)),
    sqlF.InvokeFunctionR(CIName("count"), List(sqlF.SpliceR(None)))))
  def genOuterInt = Gen.oneOf(
    Gen.const(sqlF.IntLiteralR(0)),
    genReduceInt,
    genReduceInt.flatMap(sqlF.BinopR(_, sqlF.IntLiteralR(1000), sql.Div)),
    genInnerInt.flatMap(x => sqlF.BinopR(sqlF.IdentR("loc"), sqlF.ArrayLiteralR(List(x)), Concat)))

  def genInnerStr = Gen.oneOf(
    sqlF.IdentR("city"),
    // StringLiteralR("foo"),  // TODO: exposes bugs (see SD-478)
    sqlF.InvokeFunctionR(CIName("lower"), List(sqlF.IdentR("city"))))
  def genReduceStr = genInnerStr.flatMap(x => Gen.oneOf(
    x,
    sqlF.InvokeFunctionR(CIName("min"), List(x)),
    sqlF.InvokeFunctionR(CIName("max"), List(x))))
  def genOuterStr = Gen.oneOf(
    Gen.const(sqlF.StringLiteralR("foo")),
    Gen.const(sqlF.IdentR("state")),  // possibly the grouping key, so never reduced
    genReduceStr,
    genReduceStr.flatMap(x => sqlF.InvokeFunctionR(CIName("lower"), List(x))),   // an ExprOp
    genReduceStr.flatMap(x => sqlF.InvokeFunctionR(CIName("length"), List(x))))  // requires JS

  implicit def shrinkQuery(implicit SS: Shrink[Fix[Sql]]): Shrink[Query] = Shrink { q =>
    fixParser.parseExpr(q).fold(κ(Stream.empty), SS.shrink(_).map(sel => Query(pprint(sel))))
  }

  /**
   Shrink a query by reducing the number of projections or grouping expressions. Do not
   change the "shape" of the query, by removing the group by entirely, etc.
   */
  implicit def shrinkExpr: Shrink[Fix[Sql]] = {
    /** Shrink a list, removing a single item at a time, but never producing an empty list. */
    def shortened[A](as: List[A]): Stream[List[A]] =
      if (as.length <= 1) Stream.empty
      else as.toStream.map(a => as.filterNot(_ == a))

    Shrink {
      case Embed(Select(d, projs, rel, filter, groupBy, orderBy)) =>
        val sDistinct = if (d == SelectDistinct) Stream(sqlF.SelectR(SelectAll, projs, rel, filter, groupBy, orderBy)) else Stream.empty
        val sProjs = shortened(projs).map(ps => sqlF.SelectR(d, ps, rel, filter, groupBy, orderBy))
        val sGroupBy = groupBy.map { case GroupBy(keys, having) =>
          shortened(keys).map(ks => sqlF.SelectR(d, projs, rel, filter, Some(GroupBy(ks, having)), orderBy))
        }.getOrElse(Stream.empty)
        sDistinct ++ sProjs ++ sGroupBy
      case expr => Stream(expr)
    }
  }

  "plan from LogicalPlan" should {
    import StdLib._

    "plan simple Sort" in {
      val lp =
        lpf.let(
          'tmp0, read("db/foo"),
          lpf.let(
            'tmp1, makeObj("bar" -> lpf.invoke2(ObjectProject, lpf.free('tmp0), lpf.constant(Data.Str("bar")))),
            lpf.let('tmp2,
              lpf.sort(
                lpf.free('tmp1),
                (lpf.invoke2(ObjectProject, lpf.free('tmp1), lpf.constant(Data.Str("bar"))), SortDir.asc).wrapNel),
              lpf.free('tmp2))))

      planLP(lp) must beWorkflow0(chain[Workflow](
        $read(collection("db", "foo")),
        $project(
          reshape("bar" -> $field("bar")),
          IgnoreId),
        $sort(NonEmptyList(BsonField.Name("bar") -> SortDir.Ascending))))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; foo)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ Arr
        |│  │     │  ├─ Obj
        |│  │     │  │  ╰─ Key(bar: _.bar)
        |│  │     │  ╰─ Ident(_)
        |│  │     ╰─ Obj
        |│  │        ├─ Key(0: __val[1].bar)
        |│  │        ╰─ Key(src: __val)
        |│  ╰─ Scope(Map())
        |├─ $SortF
        |│  ╰─ SortKey(0 -> Ascending)
        |╰─ $ProjectF
        |   ├─ Name("value" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ╰─ ExcludeId""".stripMargin)

    "plan Sort with expression" in {
      val lp =
        lpf.let(
          'tmp0, read("db/foo"),
          lpf.sort(
            lpf.free('tmp0),
            (math.Divide[Fix[LP]](
              lpf.invoke2(ObjectProject, lpf.free('tmp0), lpf.constant(Data.Str("bar"))),
              lpf.constant(Data.Dec(10.0))).embed, SortDir.asc).wrapNel))

      planLP(lp) must beWorkflow(chain[Workflow](
        $read(collection("db", "foo")),
        $project(
          reshape(
            "0" -> divide($field("bar"), $literal(Bson.Dec(10.0))),
            "src" -> $$ROOT),
          IgnoreId),
        $sort(NonEmptyList(BsonField.Name("0") -> SortDir.Ascending)),
        $project(
          reshape("value" -> $field("src")),
          ExcludeId)))
    }

    "plan Sort with expression and earlier pipeline op" in {
      val lp =
        lpf.let(
          'tmp0, read("db/foo"),
          lpf.let(
            'tmp1,
            lpf.invoke2(s.Filter,
              lpf.free('tmp0),
              lpf.invoke2(relations.Eq,
                lpf.invoke2(ObjectProject, lpf.free('tmp0), lpf.constant(Data.Str("baz"))),
                lpf.constant(Data.Int(0)))),
            lpf.sort(
              lpf.free('tmp1),
              (lpf.invoke2(ObjectProject, lpf.free('tmp1), lpf.constant(Data.Str("bar"))), SortDir.asc).wrapNel)))

      planLP(lp) must beWorkflow(chain[Workflow](
        $read(collection("db", "foo")),
        $match(Selector.Doc(
          BsonField.Name("baz") -> Selector.Eq(Bson.Int32(0)))),
        $sort(NonEmptyList(BsonField.Name("bar") -> SortDir.Ascending))))
    }

    "plan Sort expression (and extra project)" in {
      val lp =
        lpf.let(
          'tmp0, read("db/foo"),
          lpf.let(
            'tmp9,
            makeObj(
              "bar" -> lpf.invoke2(ObjectProject, lpf.free('tmp0), lpf.constant(Data.Str("bar")))),
            lpf.sort(
              lpf.free('tmp9),
              (math.Divide[Fix[LP]](
                lpf.invoke2(ObjectProject, lpf.free('tmp9), lpf.constant(Data.Str("bar"))),
                lpf.constant(Data.Dec(10.0))).embed, SortDir.asc).wrapNel)))

      planLP(lp) must beWorkflow0(chain[Workflow](
        $read(collection("db", "foo")),
        $project(
          reshape(
            "bar"    -> $field("bar"),
            "__tmp0" -> divide($field("bar"), $literal(Bson.Dec(10.0)))),
          IgnoreId),
        $sort(NonEmptyList(BsonField.Name("__tmp0") -> SortDir.Ascending)),
        $project(
          reshape("bar" -> $field("bar")),
          ExcludeId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; foo)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Let(__val)
        |│  │     ├─ Arr
        |│  │     │  ├─ Obj
        |│  │     │  │  ╰─ Key(bar: _.bar)
        |│  │     │  ╰─ Ident(_)
        |│  │     ╰─ Obj
        |│  │        ├─ Key(0: __val[1].bar / 10.0)
        |│  │        ╰─ Key(src: __val)
        |│  ╰─ Scope(Map())
        |├─ $SortF
        |│  ╰─ SortKey(0 -> Ascending)
        |╰─ $ProjectF
        |   ├─ Name("value" -> { "$arrayElemAt": ["$src", { "$literal": NumberInt("0") }] })
        |   ╰─ ExcludeId""".stripMargin)

    "plan with extra squash and flattening" in {
      // NB: this case occurs when a view's LP is embedded in a larger query
      //     (See SD-1403), because type-checks are inserted into the inner and
      //     outer queries separately.

      val lp =
        lpf.let(
          'tmp0,
          lpf.let(
            'check0,
            lpf.invoke1(identity.Squash, read("db/zips")),
            lpf.typecheck(
              lpf.free('check0),
              Type.Obj(Map(), Some(Type.Top)),
              lpf.free('check0),
              lpf.constant(Data.NA))),
          lpf.invoke1(identity.Squash,
            makeObj(
              "city" ->
                lpf.invoke2(ObjectProject,
                  lpf.invoke2(s.Filter,
                    lpf.free('tmp0),
                    lpf.invoke3(string.Search,
                      lpf.invoke1(FlattenArray,
                        lpf.let(
                          'check1,
                          lpf.invoke2(ObjectProject, lpf.free('tmp0), lpf.constant(Data.Str("loc"))),
                          lpf.typecheck(
                            lpf.free('check1),
                            Type.FlexArr(0, None, Type.Str),
                            lpf.free('check1),
                            lpf.constant(Data.Arr(List(Data.NA)))))),
                      lpf.constant(Data.Str("^.*MONT.*$")),
                      lpf.constant(Data.Bool(false)))),
                  lpf.constant(Data.Str("city"))))))

      planLP(lp) must beWorkflow0(chain[Workflow](
        $read(collection("db", "zips")),
        $project(
          reshape(
            "__tmp4" -> $cond(
              $and(
                $lte($literal(Bson.Doc()), $$ROOT),
                $lt($$ROOT, $literal(Bson.Arr(List())))),
              $$ROOT,
              $literal(Bson.Undefined)),
            "__tmp5" -> $$ROOT),
          IgnoreId),
        $project(
          reshape(
            "__tmp6" -> $cond(
              $and(
                $lte($literal(Bson.Arr(List())), $field("__tmp4", "loc")),
                $lt($field("__tmp4", "loc"), $literal(Bson.Binary.fromArray(Array[Byte]())))),
              $field("__tmp4", "loc"),
              $literal(Bson.Arr(List(Bson.Undefined)))),
            "__tmp7" -> $field("__tmp5")),
          IgnoreId),
        $unwind(DocField("__tmp6")),
        $match(
          Selector.Doc(
            BsonField.Name("__tmp6") -> Selector.Regex("^.*MONT.*$", false, true, false, false))),
        $project(
          reshape(
            "__tmp8" -> $cond(
              $and(
                $lte($literal(Bson.Doc()), $field("__tmp7")),
                $lt($field("__tmp7"), $literal(Bson.Arr(List())))),
              $field("__tmp7"),
              $literal(Bson.Undefined))),
          IgnoreId),
        $project(
          reshape("city" -> $field("__tmp16", "city")),
          IgnoreId),
        $group(
          grouped(),
          -\/(reshape("0" -> $field("city")))),
        $project(
          reshape("city" -> $field("_id", "0")),
          IgnoreId)))
    }.pendingWithActual(notOnPar,
      """Chain
        |├─ $ReadF(db; zips)
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ├─ Key(s: (function (__val) {
        |│  │     │      return [
        |│  │     │        __val[0],
        |│  │     │        __val[1],
        |│  │     │        [
        |│  │     │          __val[0],
        |│  │     │          (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].loc : undefined]]
        |│  │     │    })(
        |│  │     │      [_._id, _]))
        |│  │     ╰─ Key(f: (function (__val) { return Array.isArray(__val[2][1]) ? __val[2][1] : undefined })(
        |│  │            (function (__val) {
        |│  │              return [
        |│  │                __val[0],
        |│  │                __val[1],
        |│  │                [
        |│  │                  __val[0],
        |│  │                  (isObject(__val[1]) && (! Array.isArray(__val[1]))) ? __val[1].loc : undefined]]
        |│  │            })(
        |│  │              [_._id, _])))
        |│  ├─ Flatten
        |│  │  ╰─ JsCore(_.f)
        |│  ├─ Map
        |│  │  ╰─ JsCore([
        |│  │            (isObject(_.s[1]) && (! Array.isArray(_.s[1]))) ? _.s[1] : undefined,
        |│  │            (new RegExp("^.*MONT.*$", "m")).test(_.f)])
        |│  ╰─ Scope(Map())
        |├─ $ProjectF
        |│  ├─ Name("0" -> { "$arrayElemAt": ["$$ROOT", { "$literal": NumberInt("1") }] })
        |│  ├─ Name("src" -> "$$ROOT")
        |│  ╰─ IgnoreId
        |├─ $MatchF
        |│  ╰─ Doc
        |│     ╰─ Expr($0 -> Eq(Bool(true)))
        |├─ $SimpleMapF
        |│  ├─ Map
        |│  │  ╰─ Obj
        |│  │     ╰─ Key(city: _.src[0].city)
        |│  ╰─ Scope(Map())
        |╰─ $ProjectF
        |   ├─ Name("city" -> true)
        |   ╰─ IgnoreId""".stripMargin)
  }

  "planner log" should {
    "include all phases when successful" in {
      planLog(sqlE"select city from zips").map(_.name) must_===
        Vector(
          "SQL AST", "Variables Substituted", "Absolutized", "Normalized Projections",
          "Sort Keys Projected", "Annotated Tree",
          "Logical Plan", "Optimized", "Typechecked", "Rewritten Joins",
          "QScript", "QScript (ShiftRead)", "QScript (Optimized)", "QScript (Mongo-specific)",
          "Workflow Builder", "Workflow (raw)", "Workflow (crystallized)")
    }

    "include correct phases with type error" in {
      planLog(sqlE"select 'a' + 0 from zips").map(_.name) must_===
        Vector(
          "SQL AST", "Variables Substituted", "Absolutized", "Annotated Tree", "Logical Plan", "Optimized")
    }.pendingUntilFixed("SD-1249")

    "include correct phases with planner error" in {
      planLog(sqlE"""select interval(bar) from zips""").map(_.name) must_===
        Vector(
          "SQL AST", "Variables Substituted", "Absolutized", "Normalized Projections",
          "Sort Keys Projected", "Annotated Tree",
          "Logical Plan", "Optimized", "Typechecked", "Rewritten Joins",
          "QScript", "QScript (ShiftRead)", "QScript (Optimized)", "QScript (Mongo-specific)")
    }
  }
}
