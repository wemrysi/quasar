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

package quasar.qscript.qsu

import quasar.{Planner, Qspec, TreeMatchers, Type}, Planner.PlannerError
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.qscript.{
  construction,
  Hole,
  HoleF,
  MapFuncsCore,
  ReduceFuncs,
  ReduceIndex,
  SrcHole
}
import slamdata.Predef._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.data.free._
import pathy.Path, Path.Sandboxed
import scalaz.{\/-, EitherT, Equal, Free, IList, Need, StateT}
import scalaz.syntax.applicative._
import scalaz.syntax.std.option._

object MinimizeAutoJoinsSpec extends Qspec with TreeMatchers with QSUTTypes[Fix] {
  import QSUGraph.Extractors._
  import ApplyProvenance.AuthenticatedQSU
  import QScriptUniform.DTrans

  type F[A] = EitherT[StateT[Need, Long, ?], PlannerError, A]

  val qsu = QScriptUniform.DslT[Fix]
  val func = construction.Func[Fix]
  val maj = MinimizeAutoJoins[Fix]
  val qprov = QProv[Fix]

  val J = Fixed[Fix[EJson]]

  val afile = Path.rootDir[Sandboxed] </> Path.file("afile")

  implicit val eqP: Equal[qprov.P] =
    qprov.prov.provenanceEqual(Equal[qprov.D], Equal[FreeMapA[Access[Symbol]]])

  "unary node elimination" should {
    "linearize .foo + .bar" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.map(
            qsu.read(afile),
            func.ProjectKey(HoleF, func.Constant(J.str("foo")))),
          qsu.map(
            qsu.read(afile),
            func.ProjectKey(HoleF, func.Constant(J.str("bar")))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(Read(_), fm) =>
          // must_=== doesn't work
          fm must beTreeEqual(
            func.Add(
              func.ProjectKey(HoleF, func.Constant(J.str("foo"))),
              func.ProjectKey(HoleF, func.Constant(J.str("bar")))))
      }
    }

    "convert Typecheck to a Map(_, Guard)" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin3((
          qsu.read(afile),
          qsu.read(afile),
          qsu.map1((
            qsu.unreferenced(),
            MapFuncsCore.Undefined[Fix, Hole](): MapFuncCore[Hole])),
          _(MapFuncsCore.Guard(_, Type.AnyObject, _, _)))))

      runOn(qgraph) must beLike {
        case Map(Read(_), fm) =>
          // must_=== doesn't work
          fm must beTreeEqual(func.Guard(HoleF, Type.AnyObject, HoleF, func.Undefined))
      }
    }

    "coalesce two summed reductions" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsReduce(
            qsu.read(afile),
            Nil,
            List(ReduceFuncs.Count(HoleF[Fix])),
            Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
          qsu.qsReduce(
            qsu.read(afile),
            Nil,
            List(ReduceFuncs.Sum(HoleF[Fix])),
            Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(
          QSReduce(
            Read(_),
            Nil,
            List(ReduceFuncs.Count(h1), ReduceFuncs.Sum(h2)),
            repair),
          fm) =>

          // must_=== doesn't work
          h1 must beTreeEqual(HoleF[Fix])
          h2 must beTreeEqual(HoleF[Fix])

          repair must beTreeEqual(
            func.ConcatMaps(
              func.MakeMap(
                func.Constant(J.str("0")),
                Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
              func.MakeMap(
                func.Constant(J.str("1")),
                Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(1))))))

          fm must beTreeEqual(
            func.Add(
              func.ProjectKey(HoleF, func.Constant(J.str("0"))),
              func.ProjectKey(HoleF, func.Constant(J.str("1")))))
      }
    }

    "coalesce three summed reductions" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.autojoin2((
            qsu.qsReduce(
              qsu.read(afile),
              Nil,
              List(ReduceFuncs.Count(HoleF[Fix])),
              Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
            qsu.qsReduce(
              qsu.read(afile),
              Nil,
              List(ReduceFuncs.Sum(HoleF[Fix])),
              Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
            _(MapFuncsCore.Add(_, _)))),
          qsu.qsReduce(
            qsu.read(afile),
            Nil,
            List(ReduceFuncs.Max(HoleF[Fix])),
            Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(
          QSReduce(
            Read(_),
            Nil,
            List(ReduceFuncs.Count(h1), ReduceFuncs.Sum(h2), ReduceFuncs.Max(h3)),
            repair),
          fm) =>

          // must_=== doesn't work
          h1 must beTreeEqual(HoleF[Fix])
          h2 must beTreeEqual(HoleF[Fix])
          h3 must beTreeEqual(HoleF[Fix])

          repair must beTreeEqual(
            func.ConcatMaps(
              func.MakeMap(
                func.Constant(J.str("0")),
                func.ConcatMaps(
                  func.MakeMap(
                    func.Constant(J.str("0")),
                    Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
                  func.MakeMap(
                    func.Constant(J.str("1")),
                    Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(1)))))),
              func.MakeMap(
                func.Constant(J.str("1")),
                Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(2))))))

          fm must beTreeEqual(
            func.Add(
              func.Add(
                func.ProjectKey(
                  func.ProjectKey(HoleF, func.Constant(J.str("0"))),
                  func.Constant(J.str("0"))),
                func.ProjectKey(
                  func.ProjectKey(HoleF, func.Constant(J.str("0"))),
                  func.Constant(J.str("1")))),
              func.ProjectKey(HoleF, func.Constant(J.str("1")))))
      }
    }

    "coalesce two summed reductions, one downstream of an autojoin" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsReduce(
            qsu.autojoin2((
              qsu.read(afile),
              qsu.map1((
                qsu.unreferenced(),
                MapFuncsCore.Constant[Fix, Hole](J.str("hey")))),
              _(MapFuncsCore.ConcatArrays(_, _)))),
            Nil,
            List(ReduceFuncs.Count(HoleF[Fix])),
            Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
          qsu.qsReduce(
            qsu.read(afile),
            Nil,
            List(ReduceFuncs.Sum(HoleF[Fix])),
            Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(
          QSReduce(
            Read(_),
            Nil,
            List(ReduceFuncs.Count(h1), ReduceFuncs.Sum(h2)),
            repair),
          fm) =>

          // must_=== doesn't work
          h1 must beTreeEqual(
            func.ConcatArrays(
              HoleF,
              func.Constant(J.str("hey"))))

          h2 must beTreeEqual(HoleF[Fix])

          repair must beTreeEqual(
            func.ConcatMaps(
              func.MakeMap(
                func.Constant(J.str("0")),
                Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
              func.MakeMap(
                func.Constant(J.str("1")),
                Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(1))))))

          fm must beTreeEqual(
            func.Add(
              func.ProjectKey(HoleF, func.Constant(J.str("0"))),
              func.ProjectKey(HoleF, func.Constant(J.str("1")))))
      }
    }

    "rewrite filter into cond only to avoid join" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.read(afile),
          qsu.qsFilter(
            qsu.read(afile),
            func.Eq(HoleF[Fix], func.Constant(J.str("foo")))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(Read(_), fm) =>
          fm must beTreeEqual(
            func.Add(
              HoleF[Fix],
              func.Cond(
                func.Eq(HoleF[Fix], func.Constant(J.str("foo"))),
                HoleF[Fix],
                func.Undefined)))
      }
    }.pendingUntilFixed

    "coalesce two summed bucketing reductions, inlining functions into the buckets" in {
      val readAndThings =
        qsu.map1((
          qsu.read(afile),
          MapFuncsCore.Negate(SrcHole)))

      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsReduce(
            readAndThings,
            List(HoleF[Fix].map(Access.value(_))),
            List(ReduceFuncs.Count(HoleF[Fix])),
            Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
          qsu.qsReduce(
            readAndThings,
            List(HoleF[Fix].map(Access.value(_))),
            List(ReduceFuncs.Sum(HoleF[Fix])),
            Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(
          QSReduce(
            Read(_),
            List(bucket),
            List(ReduceFuncs.Count(h1), ReduceFuncs.Sum(h2)),
            repair),
          fm) =>

          // must_=== doesn't work
          bucket must beTreeEqual(func.Negate(HoleF.map(Access.value(_))))

          h1 must beTreeEqual(func.Negate(HoleF[Fix]))
          h2 must beTreeEqual(func.Negate(HoleF[Fix]))

          repair must beTreeEqual(
            func.ConcatMaps(
              func.MakeMap(
                func.Constant(J.str("0")),
                Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
              func.MakeMap(
                func.Constant(J.str("1")),
                Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(1))))))

          fm must beTreeEqual(
            func.Add(
              func.ProjectKey(HoleF, func.Constant(J.str("0"))),
              func.ProjectKey(HoleF, func.Constant(J.str("1")))))
      }
    }

    "remap coalesced bucket references in dimensions" in {
      val aqsu = QScriptUniform.AnnotatedDsl[Fix, Symbol]

      val readAndThings =
        aqsu.map('n1, (
          aqsu.dimEdit('n5, (
            aqsu.read('n0, afile),
            DTrans.Group(func.ProjectKeyS(func.Hole, "label")))),
          func.Negate(func.ProjectKeyS(func.Hole, "metric"))))

      val atree =
        aqsu.autojoin2(('n4, (
          aqsu.lpReduce('n2, (
            readAndThings,
            ReduceFuncs.Count(()))),
          aqsu.lpReduce('n3, (
            readAndThings,
            ReduceFuncs.Sum(()))),
          _(MapFuncsCore.Add(_, _)))))

      val (remap, qgraph) =
        QSUGraph.fromAnnotatedTree(atree map (_.some))

      val expDims =
        IList(qprov.prov.value(Access.bucket('qsu0, 0, 'qsu0).point[FreeMapA]))

      val ds = runOn_(qgraph).dims

      (ds(remap('n2)) must_= expDims) and (ds(remap('n3)) must_= expDims)
    }
  }

  def runOn(qgraph: QSUGraph): QSUGraph =
    runOn_(qgraph).graph

  def runOn_(qgraph: QSUGraph): AuthenticatedQSU[Fix] = {
    val resultsF = for {
      agraph0 <- ApplyProvenance[Fix].apply[F](qgraph)
      agraph <- ReifyBuckets[Fix, F](agraph0)
      back <- maj[F](agraph)
    } yield back

    val results = resultsF.run.eval(0L).value.toEither
    results must beRight

    results.right.get
  }
}
