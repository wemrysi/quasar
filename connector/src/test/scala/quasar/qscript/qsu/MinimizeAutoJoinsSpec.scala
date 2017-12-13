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

import quasar.{Planner, Qspec, TreeMatchers, Type}
import Planner.PlannerError
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.qscript.{
  construction,
  ExcludeId,
  IncludeId,
  Hole,
  HoleF,
  LeftSide,
  MapFuncsCore,
  ReduceFuncs,
  ReduceIndex,
  ReduceIndexF,
  RightSide,
  SrcHole
}
import slamdata.Predef._
import matryoshka._
import matryoshka.data.Fix
import matryoshka.data.free._
import pathy.Path
import Path.Sandboxed

import scalaz.{\/-, EitherT, Equal, Free, IList, Need, StateT}
import scalaz.syntax.applicative._
import scalaz.syntax.std.option._

object MinimizeAutoJoinsSpec extends Qspec with TreeMatchers with QSUTTypes[Fix] {
  import QSUGraph.Extractors._
  import ApplyProvenance.AuthenticatedQSU
  import QScriptUniform.{DTrans, Rotation}

  type F[A] = EitherT[StateT[Need, Long, ?], PlannerError, A]

  val qsu = QScriptUniform.DslT[Fix]
  val func = construction.Func[Fix]
  val qprov = QProv[Fix]

  val J = Fixed[Fix[EJson]]

  val afile = Path.rootDir[Sandboxed] </> Path.file("afile")
  val afile2 = Path.rootDir[Sandboxed] </> Path.file("afile2")

  implicit val eqP: Equal[qprov.P] =
    qprov.prov.provenanceEqual(Equal[qprov.D], Equal[FreeMapA[Access[Symbol]]])

  "autojoin minimization" should {
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
                func.ReduceIndex(\/-(0))),
              func.MakeMap(
                func.Constant(J.str("1")),
                func.ReduceIndex(\/-(1)))))

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
                    func.ReduceIndex(\/-(0))),
                  func.MakeMap(
                    func.Constant(J.str("1")),
                    func.ReduceIndex(\/-(1))))),
              func.MakeMap(
                func.Constant(J.str("1")),
                func.ReduceIndex(\/-(2)))))

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

          h2 must beTreeEqual(func.Hole)

          repair must beTreeEqual(
            func.ConcatMaps(
              func.MakeMap(
                func.Constant(J.str("0")),
                func.ReduceIndex(\/-(0))),
              func.MakeMap(
                func.Constant(J.str("1")),
                func.ReduceIndex(\/-(1)))))

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
            func.Eq(func.Hole, func.Constant(J.str("foo")))),
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
    }

    "not rewrite filter acting as upstream source" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsFilter(
            qsu.read(afile),
            func.Eq(HoleF[Fix], func.Constant(J.str("foo")))),
          qsu.cint(42),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(QSFilter(_, _), fm) =>
          fm must beTreeEqual(
            func.Add(HoleF[Fix], func.Constant(J.int(42))))
      }
    }

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
                func.ReduceIndex(\/-(0))),
              func.MakeMap(
                func.Constant(J.str("1")),
                func.ReduceIndex(\/-(1)))))

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
        IList(qprov.prov.value(Access.bucket('qsu3, 0, 'qsu3).point[FreeMapA]))

      val ds = runOn_(qgraph).dims

      (ds(remap('n2)) must_= expDims) and (ds(remap('n3)) must_= expDims)
    }

    "leave uncoalesced reductions of different bucketing" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsReduce(
            qsu.read(afile),
            Nil,
            List(ReduceFuncs.Sum(HoleF[Fix])),
            ReduceIndexF[Fix](\/-(0))),
          qsu.qsReduce(
            qsu.read(afile),
            List(func.ProjectKey(AccessValueHoleF[Fix], func.Constant(J.str("state")))),
            List(ReduceFuncs.Sum(HoleF[Fix])),
            ReduceIndexF[Fix](\/-(0))),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case AutoJoin2C(
          QSReduce(Read(_), Nil, _, _),
          QSReduce(Read(_), List(_), _, _),
          MapFuncsCore.Add(LeftSide, RightSide)) => ok
      }
    }

    "minimize an autojoin after prior source failure" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.autojoin2((
            qsu.read(afile),
            qsu.read(afile2),
            _(MapFuncsCore.Subtract(_, _)))),
          qsu.cint(42),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case Map(
          AutoJoin2C(
            Read(`afile`),
            Read(`afile2`),
            MapFuncsCore.Subtract(LeftSide, RightSide)),
          fm) =>

          fm must beTreeEqual(func.Add(HoleF, func.Constant(J.int(42))))
      }

      ok
    }

    "coalesce an autojoin on a single leftshift on a shared source" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.leftShift(
            qsu.read(afile),
            HoleF[Fix],
            ExcludeId,
            func.RightTarget,
            Rotation.ShiftArray),
          qsu.read(afile),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case LeftShift(
          Read(`afile`),
          struct,
          ExcludeId,
          repair,
          _) =>

          struct must beTreeEqual(HoleF[Fix])

          repair must beTreeEqual(func.Add(func.RightTarget, func.AccessLeftTarget(Access.valueHole(_))))
      }
    }

    "coalesce an autojoin on a single leftshift on a shared source (RTL)" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.read(afile),
          qsu.leftShift(
            qsu.read(afile),
            HoleF[Fix],
            ExcludeId,
            func.RightTarget,
            Rotation.ShiftArray),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case LeftShift(
          Read(`afile`),
          struct,
          ExcludeId,
          repair,
          _) =>

          struct must beTreeEqual(HoleF[Fix])
          repair must beTreeEqual(func.Add(func.AccessLeftTarget(Access.valueHole(_)), func.RightTarget))
      }
    }

    "inductively coalesce reduces on coalesced shifts" in {
      // count(a[*]) + sum(a)
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsReduce(
            qsu.leftShift(
              qsu.read(afile),
              HoleF[Fix],
              ExcludeId,
              func.RightTarget,
              Rotation.ShiftArray),
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
            LeftShift(
              Read(`afile`),
              struct,
              ExcludeId,
              repairInner,
              _),
            Nil,
            List(ReduceFuncs.Count(h1), ReduceFuncs.Sum(h2)),
            repairOuter),
          fm) =>

        struct must beTreeEqual(HoleF[Fix])

        repairInner must beTreeEqual(
          func.ConcatMaps(
            func.MakeMapS("0", func.RightTarget),
            func.MakeMapS("1", func.AccessLeftTarget(Access.valueHole(_)))))

        h1 must beTreeEqual(func.ProjectKeyS(func.Hole, "0"))
        h2 must beTreeEqual(func.ProjectKeyS(func.Hole, "1"))

        repairOuter must beTreeEqual(
          func.ConcatMaps(
            func.MakeMapS("0", Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
            func.MakeMapS("1", Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(1))))))

        fm must beTreeEqual(
          func.Add(
            func.ProjectKeyS(HoleF, "0"),
            func.ProjectKeyS(HoleF, "1")))
      }
    }

    "inductively coalesce reduces on coalesced shifts" in {
      // count(a[*]) + sum(a)
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.qsReduce(
            qsu.leftShift(
              qsu.read(afile),
              HoleF[Fix],
              ExcludeId,
              func.RightTarget,
              Rotation.ShiftArray),
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
            LeftShift(
              Read(`afile`),
              struct,
              ExcludeId,
              repairInner,
              _),
            Nil,
            List(ReduceFuncs.Count(h1), ReduceFuncs.Sum(h2)),
            repairOuter),
          fm) =>

        struct must beTreeEqual(HoleF[Fix])

        repairInner must beTreeEqual(
          func.ConcatMaps(
            func.MakeMapS("0", func.RightTarget),
            func.MakeMapS("1", func.AccessLeftTarget(Access.valueHole(_)))))

        h1 must beTreeEqual(func.ProjectKeyS(func.Hole, "0"))
        h2 must beTreeEqual(func.ProjectKeyS(func.Hole, "1"))

        repairOuter must beTreeEqual(
          func.ConcatMaps(
            func.MakeMapS("0", Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(0)))),
            func.MakeMapS("1", Free.pure[MapFunc, ReduceIndex](ReduceIndex(\/-(1))))))

        fm must beTreeEqual(
          func.Add(
            func.ProjectKeyS(HoleF, "0"),
            func.ProjectKeyS(HoleF, "1")))
      }
    }

    "coalesce an autojoin on two leftshifts on a shared source" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.leftShift(
            qsu.leftShift(
              qsu.read(afile),
              HoleF[Fix],
              IncludeId,
              func.RightTarget,
              Rotation.ShiftArray),
            HoleF[Fix],
            ExcludeId,
            func.RightTarget,
            Rotation.ShiftArray),
          qsu.read(afile),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case LeftShift(
          LeftShift(
            Read(`afile`),
            structInner,
            IncludeId,
            repairInner,
            _),
          structOuter,
          ExcludeId,
          repairOuter,
          _) =>

          structInner must beTreeEqual(func.Hole)

          repairInner must beTreeEqual(
            func.ConcatMaps(
              func.MakeMapS("original", func.AccessLeftTarget(Access.valueHole(_))),
              func.MakeMapS("results", func.RightTarget)))

          structOuter must beTreeEqual(func.ProjectKeyS(func.Hole, "results"))

          repairOuter must beTreeEqual(
            func.Add(
              func.RightTarget,
              func.ProjectKeyS(
                func.ConcatMaps(
                  func.AccessLeftTarget(Access.valueHole(_)),
                  func.MakeMapS("results", func.RightTarget)),
                "original")))
      }
    }

    "coalesce an autojoin on three leftshifts on a shared source" in {
      val qgraph = QSUGraph.fromTree[Fix](
        qsu.autojoin2((
          qsu.leftShift(
            qsu.leftShift(
              qsu.leftShift(
                qsu.read(afile),
                HoleF[Fix],
                IncludeId,
                func.RightTarget,
                Rotation.ShiftArray),
              HoleF[Fix],
              ExcludeId,
              func.RightTarget,
              Rotation.ShiftMap),
            HoleF[Fix],
            ExcludeId,
            func.RightTarget,
            Rotation.ShiftArray),
          qsu.read(afile),
          _(MapFuncsCore.Add(_, _)))))

      runOn(qgraph) must beLike {
        case LeftShift(
          LeftShift(
            LeftShift(
              Read(`afile`),
              structInnerInner,
              IncludeId,
              repairInnerInner,
              Rotation.ShiftArray),
            structInner,
            ExcludeId,
            repairInner,
            Rotation.ShiftMap),
          structOuter,
          ExcludeId,
          repairOuter,
          Rotation.ShiftArray) =>

          structInnerInner must beTreeEqual(func.Hole)

          repairInnerInner must beTreeEqual(
            func.ConcatMaps(
              func.MakeMapS("original", func.AccessLeftTarget(Access.valueHole(_))),
              func.MakeMapS("results", func.RightTarget)))

          structInner must beTreeEqual(func.ProjectKeyS(func.Hole, "results"))

          repairInner must beTreeEqual(
            func.ConcatMaps(
              func.AccessLeftTarget(Access.valueHole(_)),
              func.MakeMapS("results", func.RightTarget)))

          structOuter must beTreeEqual(func.ProjectKeyS(func.Hole, "results"))

          repairOuter must beTreeEqual(
            func.Add(
              func.RightTarget,
              func.ProjectKeyS(
                func.ConcatMaps(
                  func.AccessLeftTarget(Access.valueHole(_)),
                  func.MakeMapS("results", func.RightTarget)),
                "original")))
      }
    }
  }

  def runOn(qgraph: QSUGraph): QSUGraph =
    runOn_(qgraph).graph

  def runOn_(qgraph: QSUGraph): AuthenticatedQSU[Fix] = {
    val resultsF = for {
      agraph0 <- ApplyProvenance[Fix, F](qgraph)
      agraph <- ReifyBuckets[Fix, F](agraph0)
      back <- MinimizeAutoJoins[Fix, F](agraph)
    } yield back

    val results = resultsF.run.eval(0L).value.toEither
    results must beRight

    results.right.get
  }
}
