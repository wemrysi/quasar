/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.qsu

import slamdata.Predef.{Map => SMap, _}

import quasar.{IdStatus, Qspec, Type}, IdStatus.{ExcludeId, IncludeId}
import quasar.contrib.pathy.AFile
import quasar.ejson
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fp.ski.κ
import quasar.contrib.iota._
import quasar.qscript.{
  construction,
  LeftSide,
  OnUndefined,
  PlannerError,
  ReduceFuncs,
  RightSide
}
import quasar.qscript.MapFuncsCore.RecIntLit
import quasar.qscript.provenance.Dimensions

import matryoshka._
import matryoshka.data.Fix
import org.specs2.matcher.{Expectable, Matcher, MatchResult}
import pathy.Path, Path.{file, Sandboxed}
import scalaz.{\/, Cofree, IList, NonEmptyList}
import scalaz.syntax.apply._
import scalaz.syntax.equal._
import scalaz.syntax.show._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.map._

object ApplyProvenanceSpec extends Qspec with QSUTTypes[Fix] {

  import ApplyProvenance.{AuthenticatedQSU, computeFuncDims}
  import QScriptUniform.{DTrans, Rotation}

  type F[A] = PlannerError \/ A
  type QSU[A] = QScriptUniform[A]

  val J = Fixed[Fix[EJson]]

  val qsu = QScriptUniform.AnnotatedDsl[Fix, Symbol]
  val func = construction.Func[Fix]
  val recFunc = construction.RecFunc[Fix]

  val app = ApplyProvenance[Fix, F] _
  val qprov = QProv[Fix]

  type P = qprov.P
  val P = qprov.prov

  val root = Path.rootDir[Sandboxed]
  val afile: AFile = root </> file("foobar")
  val FooBar: P = prjPath("foobar")

  def prjPath(name: String): P =
    P.project(J.str(name), IdType.Dataset)

  def infRow(id: Symbol): P =
    P.inflate(IdAccess.identity(id), IdType.Dataset)

  def infGrp(id: Symbol, idx: Int): P =
    P.inflate(IdAccess.bucket(id, idx), IdType.Expr)

  def infMap(id: Symbol): P =
    P.inflate(IdAccess.identity(id), IdType.Map)

  def injMap(key: String): P =
    P.project(J.str(key), IdType.Map)

  def prjMap(key: String): P =
    P.inject(J.str(key), IdType.Map)

  import P.implicits._

  "provenance application" should {

    "produce provenance for map" in {
      val fm: RecFreeMap = recFunc.Add(recFunc.Hole, RecIntLit(17))

      val tree: Cofree[QSU, Symbol] =
        qsu.map('name0,
          (qsu.read('name1, (afile, ExcludeId)), fm))

      val dims: SMap[Symbol, QDims] = SMap(
        'name0 -> Dimensions.origin(infRow('name1), FooBar),
        'name1 -> Dimensions.origin(infRow('name1), FooBar))

      tree must haveDimensions(dims)
    }

    "produce normalized provenance for map" in {
      val fm1: RecFreeMap =
        recFunc.StaticMapS(
          "A" -> recFunc.ProjectKeyS(recFunc.Hole, "X"),
          "B" -> recFunc.ProjectKeyS(recFunc.Hole, "Y"))

      val fm2: RecFreeMap =
        recFunc.Add(recFunc.ProjectKeyS(recFunc.Hole, "B"), RecIntLit(17))

      val tree: Cofree[QSU, Symbol] =
        qsu.map('name2, (
          qsu.map('name1, (
            qsu.read('name0, (afile, ExcludeId)),
            fm1)),
          fm2))

      val dims: SMap[Symbol, QDims] = SMap(
        'name0 -> Dimensions.origin(infRow('name0), FooBar),
        'name1 -> Dimensions.origin(
          P.thenn(
            P.both(
              P.thenn(injMap("A"), prjMap("X")),
              P.thenn(injMap("B"), prjMap("Y"))),
            infRow('name0)),
          FooBar),
        'name2 -> Dimensions.origin(
          P.thenn(prjMap("Y"), infRow('name0)),
          FooBar))

      tree must haveDimensions(dims)
    }

    "produce provenance for map involving a union" in {
      val fm1: RecFreeMap =
        recFunc.StaticMapS(
          "x" -> recFunc.IfUndefined(
            recFunc.ProjectKeyS(recFunc.Hole, "a"),
            recFunc.ProjectKeyS(recFunc.Hole, "b")),
          "y" -> recFunc.ProjectKeyS(recFunc.Hole, "c"))

      val fm2: RecFreeMap =
        recFunc.Add(
          recFunc.ProjectKeyS(recFunc.Hole, "x"),
          recFunc.ProjectKeyS(recFunc.Hole, "y"))

      val tree: Cofree[QSU, Symbol] =
        qsu.map('name0, (
          qsu.map('name1, (
            qsu.read('name2, (afile, ExcludeId)),
            fm1)),
          fm2))

      val dims: SMap[Symbol, QDims] = SMap(
        'name0 -> Dimensions(IList(
          NonEmptyList(
            P.thenn(
              P.both(
                P.prjValue(J.str("a")),
                P.prjValue(J.str("c"))),
              P.value(IdAccess.identity('name2))),
            P.prjPath(J.str("foobar"))),
          NonEmptyList(
            P.thenn(
              P.both(
                P.prjValue(J.str("b")),
                P.prjValue(J.str("c"))),
              P.value(IdAccess.identity('name2))),
            P.prjPath(J.str("foobar"))))),
        'name1 -> Dimensions(IList(
          NonEmptyList(
            P.thenn(
              P.both(
                P.thenn(P.injValue(J.str("x")), P.prjValue(J.str("a"))),
                P.thenn(P.injValue(J.str("y")), P.prjValue(J.str("c")))),
              P.value(IdAccess.identity('name2))),
            P.prjPath(J.str("foobar"))),
          NonEmptyList(
            P.thenn(
              P.both(
                P.thenn(P.injValue(J.str("x")), P.prjValue(J.str("b"))),
                P.thenn(P.injValue(J.str("y")), P.prjValue(J.str("c")))),
              P.value(IdAccess.identity('name2))),
            P.prjPath(J.str("foobar"))))),
        'name2 -> Dimensions.origin(
          P.value(IdAccess.identity('name2)),
          P.prjPath(J.str("foobar"))))

      tree must haveDimensions(dims)
    }

    "compute correct provenance nested dimEdits" >> {
      val tree =
        qsu.lpReduce('n4, (
          qsu.map('n3, (
            qsu.dimEdit('n2, (
              qsu.dimEdit('n1, (
                qsu.read('n0, (afile, ExcludeId)),
                DTrans.Group(func.ProjectKeyS(func.Hole, "x")))),
              DTrans.Group(func.ProjectKeyS(func.Hole, "y")))),
            recFunc.ProjectKeyS(recFunc.Hole, "pop"))),
          ReduceFuncs.Sum(())))

      tree must haveDimensions(SMap(
        'n4 -> Dimensions.origin(
          infGrp('n4, 1),
          infGrp('n4, 0),
          FooBar),
        'n3 -> Dimensions.origin(
          prjMap("pop") ≺: infRow('n2),
          infGrp('n2, 1),
          infGrp('n2, 0),
          FooBar),
        'n2 -> Dimensions.origin(
          infRow('n2),
          infGrp('n2, 1),
          infGrp('n2, 0),
          FooBar),
        'n1 -> Dimensions.origin(
          infRow('n1),
          infGrp('n1, 0),
          FooBar),
        'n0 -> Dimensions.origin(
          infRow('n0),
          FooBar)
      ))
    }

    "compute provenance for squash" >> {
      val tree =
        qsu.dimEdit('n0, (
          qsu.map('n1, (
            qsu.read('n2, (afile, ExcludeId)),
            recFunc.Add(
              recFunc.Constant(J.int(7)),
              recFunc.ProjectKeyS(recFunc.Hole, "bar")))),
          DTrans.Squash[Fix]()))

      tree must haveDimensions(SMap(
        'n0 -> Dimensions.origin(prjMap("bar") ≺: infRow('n2) ≺: FooBar),
        'n1 -> Dimensions.origin(prjMap("bar") ≺: infRow('n2), FooBar),
        'n2 -> Dimensions.origin(infRow('n2), FooBar)
      ))
    }

    //      Read
    //        |
    //    Transpose
    //     /     \
    // DimEdit    Map
    //   |         |
    // QSFilter    |
    //   \        /
    //   AutoJoin2
    //        |
    //     DimEdit
    "compute provenance for graph branch with squash" >> {
      val root =
        qsu.transpose('n1, (
          qsu.read('n0, (afile, ExcludeId)),
          QScriptUniform.Retain.Identities,
          Rotation.FlattenMap))

      val tree =
        qsu.dimEdit('n6, (
          qsu._autojoin2('n5, (
            qsu.qsFilter('n4, (
              qsu.dimEdit('n3, (
                root,
                DTrans.Squash[Fix]())),
              recFunc.Gt(recFunc.Hole, recFunc.Constant(ejson.Fixed[Fix[EJson]].int(42))))),
            qsu.map('n2, (
              root,
              recFunc.ProjectKeyS(recFunc.Hole, "baz"))),
            func.ConcatMaps(func.LeftSide, func.RightSide))),
          DTrans.Squash[Fix]()))

      tree must haveDimensions(SMap(
        'n0 -> Dimensions.origin(infRow('n0), FooBar),
        'n1 -> Dimensions.origin(infMap('n1) ≺: infRow('n0), FooBar),
        'n2 -> Dimensions.origin(prjMap("baz") ≺: infMap('n1) ≺: infRow('n0), FooBar),
        'n3 -> Dimensions.origin(infMap('n1) ≺: infRow('n0), FooBar),
        'n4 -> Dimensions.origin(infMap('n1) ≺: infRow('n0), FooBar),
        // FIXME: Hmm, this doesn't seem correct, does one dominate?
        'n5 -> Dimensions.origin(
          prjMap("baz") ≺: infMap('n1) ≺: infRow('n0),
          infMap('n1) ≺: infRow('n0) ≺: FooBar),
        'n6 -> Dimensions.origin(
          P.thenn(
            P.prjValue(J.str("baz")),
            P.thenn(
              P.value(IdAccess.identity('n1)),
              P.thenn(
                P.value(IdAccess.identity('n0)),
                P.thenn(
                  P.value(IdAccess.identity('n1)),
                    P.thenn(
                      P.value(IdAccess.identity('n0)),
                      P.prjPath(J.str("foobar"))))))))
      ))
    }
  }

  "left shift provenance" >> {
    "flatten with trivial struct and repair is dimensional flatten" >> {
      val tree =
        qsu.leftShift('n0, (
          qsu.read('n1, (afile, ExcludeId)),
          recFunc.Hole,
          ExcludeId,
          OnUndefined.Omit,
          RightTarget[Fix],
          Rotation.FlattenArray))

      tree must haveDimensions(SMap(
        'n0 -> Dimensions.origin(
          P.thenn(
            P.value(IdAccess.identity('n0)),
            P.value(IdAccess.identity('n1))),
          P.prjPath(J.str("foobar"))),
        'n1 -> Dimensions.origin(
          P.value(IdAccess.identity('n1)),
          P.prjPath(J.str("foobar")))
      ))
    }

    "shift with trivial struct and repair is dimensional shift" >> {
      val tree =
        qsu.leftShift('n0, (
          qsu.read('n1, (afile, ExcludeId)),
          recFunc.Hole,
          ExcludeId,
          OnUndefined.Omit,
          RightTarget[Fix],
          Rotation.ShiftArray))

      tree must haveDimensions(SMap(
        'n0 -> Dimensions.origin(
          P.value(IdAccess.identity('n0)),
          P.value(IdAccess.identity('n1)),
          P.prjPath(J.str("foobar"))),
        'n1 -> Dimensions.origin(
          P.value(IdAccess.identity('n1)),
          P.prjPath(J.str("foobar")))
      ))
    }

    "include id affects provenance" >> {
      val tree =
        qsu.leftShift('n0, (
          qsu.read('n1, (afile, ExcludeId)),
          recFunc.Hole,
          IncludeId,
          OnUndefined.Omit,
          RightTarget[Fix],
          Rotation.ShiftArray))

      tree must haveDimensions(SMap(
        'n0 -> Dimensions.origin(
          P.thenn(
            P.both(
              P.injValue(J.int(0)),
              P.injValue(J.int(1))),
            P.value(IdAccess.identity('n0))),
          P.value(IdAccess.identity('n1)),
          P.prjPath(J.str("foobar"))),
        'n1 -> Dimensions.origin(
          P.value(IdAccess.identity('n1)),
          P.prjPath(J.str("foobar")))
      ))
    }

    "struct affects provenance prior to shift" >> {
      val tree =
        qsu.leftShift('n0, (
          qsu.read('n1, (afile, ExcludeId)),
          recFunc.ProjectKeyS(recFunc.Hole, "k"),
          ExcludeId,
          OnUndefined.Omit,
          RightTarget[Fix],
          Rotation.ShiftArray))

      tree must haveDimensions(SMap(
        'n0 -> Dimensions.origin(
          P.value(IdAccess.identity('n0)),
          P.thenn(
            P.prjValue(J.str("k")),
            P.value(IdAccess.identity('n1))),
          P.prjPath(J.str("foobar"))),
        'n1 -> Dimensions.origin(
          P.value(IdAccess.identity('n1)),
          P.prjPath(J.str("foobar")))
      ))
    }

    "repair affects provenance after shift" >> {
      val tree =
        qsu.leftShift('n0, (
          qsu.read('n1, (afile, ExcludeId)),
          recFunc.Hole,
          ExcludeId,
          OnUndefined.Omit,
          func.ProjectKeyS(RightTarget[Fix], "k"),
          Rotation.ShiftArray))

      tree must haveDimensions(SMap(
        'n0 -> Dimensions.origin(
          P.thenn(
            P.prjValue(J.str("k")),
            P.value(IdAccess.identity('n0))),
          P.value(IdAccess.identity('n1)),
          P.prjPath(J.str("foobar"))),
        'n1 -> Dimensions.origin(
          P.value(IdAccess.identity('n1)),
          P.prjPath(J.str("foobar")))
      ))
    }

    "non-structure preserving result in repair results in shift provenance" >> {
      val tree =
        qsu.leftShift('n0, (
          qsu.read('n1, (afile, ExcludeId)),
          recFunc.Hole,
          ExcludeId,
          OnUndefined.Omit,
          func.Add(RightTarget[Fix], func.Constant(J.int(42))),
          Rotation.ShiftArray))

      tree must haveDimensions(SMap(
        'n0 -> Dimensions.origin(
          P.value(IdAccess.identity('n0)),
          P.value(IdAccess.identity('n1)),
          P.prjPath(J.str("foobar"))),
        'n1 -> Dimensions.origin(
          P.value(IdAccess.identity('n1)),
          P.prjPath(J.str("foobar")))
      ))
    }
  }

  "AutoJoin provenance" >> {
    "joining on projections is equivalent to projecting from the join" >> {
      val joinOnP =
        qsu._autojoin2('n0, (
          qsu.map('n1, (
            qsu.read('n2, (afile, ExcludeId)),
            recFunc.MakeMapS("A", recFunc.ProjectKeyS(recFunc.Hole, "X")))),
          qsu.map('n3, (
            qsu.read('n4, (afile, ExcludeId)),
            recFunc.MakeMapS("B", recFunc.ProjectKeyS(recFunc.Hole, "Y")))),
          func.ConcatMaps(func.LeftSide, func.RightSide)))

      val pOnJoin =
        qsu._autojoin2('n0, (
          qsu.read('n2, (afile, ExcludeId)),
          qsu.read('n4, (afile, ExcludeId)),
          func.ConcatMaps(
            func.MakeMapS("A", func.ProjectKeyS(func.LeftSide, "X")),
            func.MakeMapS("B", func.ProjectKeyS(func.RightSide, "Y")))))

      val jop = app(QSUGraph.fromAnnotatedTree[Fix](joinOnP map (Some(_)))._2) flatMap {
        case AuthenticatedQSU(g, a) => a.lookupDimsE[F](g.root)
      }

      val poj = app(QSUGraph.fromAnnotatedTree[Fix](pOnJoin map (Some(_)))._2) flatMap {
        case AuthenticatedQSU(g, a) => a.lookupDimsE[F](g.root)
      }

      (jop |@| poj)(_ must_= _) getOrElse ko
    }
  }

  "MapFunc provenance" >> {
    val rdims =
      qprov.lshift(IdAccess.identity('a), qprov.projectPath(J.str("data"), qprov.empty))

    val topDim = Dimensions.topDimension[P]

    "make map injects" >> {
      val res = computeFuncDims(func.MakeMapS("k", func.Hole))(κ(rdims))
      res must_= Some(qprov.injectStatic(J.str("k"), rdims))
    }

    "make array injects" >> {
      val res = computeFuncDims(func.MakeArray(func.Hole))(κ(rdims))
      res must_= Some(qprov.injectStatic(J.int(0), rdims))
    }

    "project key projects" >> {
      val res = computeFuncDims(func.ProjectKeyS(func.Hole, "k"))(κ(rdims))
      res must_= Some(qprov.projectStatic(J.str("k"), rdims))
    }

    "project index projects" >> {
      val res = computeFuncDims(func.ProjectIndexI(func.Hole, 1))(κ(rdims))
      res must_= Some(qprov.projectStatic(J.int(1), rdims))
    }

    "concat map joins" >> {
      val fm = func.ConcatMaps(
        func.MakeMapS("k1", func.Hole),
        func.MakeMapS("k2", func.Hole))


      val exp = qprov.join(
        qprov.injectStatic(J.str("k1"), rdims),
        qprov.injectStatic(J.str("k2"), rdims))

      computeFuncDims(fm)(κ(rdims)) must_= Some(exp)
    }

    "concat array joins, updating rhs injects" >> {
      val l = qprov.injectStatic(J.int(0), rdims)
      val r = qprov.injectStatic(J.int(0), rdims)

      val mf = func.ConcatArrays(func.LeftSide, func.RightSide)

      val res = computeFuncDims(mf) {
        case LeftSide => l
        case RightSide => r
      }

      val exp = topDim.modify(d =>
        (P.injValue(J.int(0)) ≺: d) ∧ (P.injValue(J.int(1)) ≺: d))(rdims)

      res must_= Some(exp)
    }.pendingUntilFixed("ch1487")

    "concat array where lhs is not static makes rhs existential" >> {
      val l = qprov.injectDynamic(rdims)
      val r = qprov.injectStatic(J.int(0), rdims)

      val mf = func.ConcatArrays(func.LeftSide, func.RightSide)

      val res = computeFuncDims(mf) {
        case LeftSide => l
        case RightSide => r
      }

      res.exists(topDim all {
        case P.both(P.thenn(P.fresh(_), _), P.thenn(P.fresh(_), _)) => true
        case _ => false
      }) must beTrue
    }.pendingUntilFixed("ch1487")

    "concat array where lhs is static and rhs isn't joins" >> {
      val l = qprov.injectStatic(J.int(0), rdims)
      val r = qprov.injectDynamic(rdims)

      val mf = func.ConcatArrays(func.LeftSide, func.RightSide)

      val res = computeFuncDims(mf) {
        case LeftSide => l
        case RightSide => r
      }

      res.exists(topDim all {
        case P.thenn(P.both(P.injValue(J.int(i)), P.fresh(_)), _) if i == 0 => true
        case _ => false
      }) must beTrue
    }

    "delete key" >> {
      "identity when key is static" >> {
        computeFuncDims(func.DeleteKeyS(func.Hole, "k"))(κ(rdims)) must_= Some(rdims)
      }

      "join when key is dynamic" >> {
        val l = qprov.projectStatic(J.str("obj"), rdims)
        val r = qprov.projectStatic(J.str("keyName"), rdims)

        val mf = func.DeleteKey(
          func.ProjectKeyS(func.Hole, "obj"),
          func.ProjectKeyS(func.Hole, "keyName"))

        computeFuncDims(mf)(κ(rdims)) must_= Some(qprov.join(l, r))
      }
    }

    "if undefined unions" >> {
      val fm = func.IfUndefined(
        func.ProjectKeyS(func.Hole, "a"),
        func.ProjectKeyS(func.Hole, "b"))

      val exp = qprov.union(
        qprov.projectStatic(J.str("a"), rdims),
        qprov.projectStatic(J.str("b"), rdims))

      computeFuncDims(fm)(κ(rdims)) must_= Some(exp)
    }

    "filtering cond is identity" >> {
      val t = func.Cond(
        func.Eq(
          func.ProjectKeyS(func.Hole, "a"),
          func.ProjectKeyS(func.Hole, "b")),
        func.Hole,
        func.Undefined)

      val f = func.Cond(
        func.Eq(
          func.ProjectKeyS(func.Hole, "a"),
          func.ProjectKeyS(func.Hole, "b")),
        func.Undefined,
        func.Hole)

      computeFuncDims(t)(κ(rdims)) must_= Some(rdims)
      computeFuncDims(f)(κ(rdims)) must_= Some(rdims)
    }

    "two-branch cond is union" >> {
      val c = func.Cond(
        func.Eq(
          func.ProjectKeyS(func.Hole, "a"),
          func.ProjectKeyS(func.Hole, "b")),
        func.Hole,
        func.ProjectKeyS(func.Hole, "c"))

      val exp = qprov.union(
        rdims,
        qprov.projectStatic(J.str("c"), rdims))

      computeFuncDims(c)(κ(rdims)) must_= Some(exp)
    }

    "filtering guard is identity" >> {
      val lg = func.Guard(
        func.ProjectKeyS(func.Hole, "a"),
        Type.Str,
        func.Hole,
        func.Undefined)

      val rg = func.Guard(
        func.ProjectKeyS(func.Hole, "a"),
        Type.Str,
        func.Undefined,
        func.Hole)

      computeFuncDims(lg)(κ(rdims)) must_= Some(rdims)
      computeFuncDims(rg)(κ(rdims)) must_= Some(rdims)
    }

    "two branch guard is union" >> {
      val g = func.Guard(
        func.ProjectKeyS(func.Hole, "a"),
        Type.Str,
        func.ProjectKeyS(func.Hole, "a"),
        func.ProjectKeyS(func.Hole, "b"))

      val exp = qprov.union(
        qprov.projectStatic(J.str("a"), rdims),
        qprov.projectStatic(J.str("b"), rdims))

      computeFuncDims(g)(κ(rdims)) must_= Some(exp)
    }

    "typecheck is identity" >> {
      val fm = func.Typecheck(func.Hole, Type.Top)
      computeFuncDims(fm)(κ(rdims)) must_= Some(rdims)
    }

    "undefined is empty" >> {
      computeFuncDims(func.Undefined)(κ(rdims)) must beNone
    }

    "default is to join" >> {
      val fm = func.Add(
        func.ProjectKeyS(func.Hole, "x"),
        func.ProjectKeyS(func.Hole, "y"))

      val exp = qprov.join(
        qprov.projectStatic(J.str("x"), rdims),
        qprov.projectStatic(J.str("y"), rdims))

      computeFuncDims(fm)(κ(rdims)) must_= Some(exp)
    }
  }

  // checks the expected dimensions
  def haveDimensions(expected: SMap[Symbol, QDims])
      : Matcher[Cofree[QSU, Symbol]] =
    new Matcher[Cofree[QSU, Symbol]] {
      def apply[S <: Cofree[QSU, Symbol]](s: Expectable[S]): MatchResult[S] = {

        val (renames, inputGraph): (QSUGraph.Renames, QSUGraph) =
          QSUGraph.fromAnnotatedTree[Fix](s.value.map(Some(_)))

        val expectedDims: SMap[Symbol, QDims] =
          expected.map { case (k, v) =>
            val newP = renames.foldLeft(v)((p, t) => qprov.rename(t._1, t._2, p))
            (renames(k), newP)
          }

        val actual: PlannerError \/ AuthenticatedQSU[Fix] = app(inputGraph)

        actual.bimap[MatchResult[S], MatchResult[S]](
        { err =>
          failure(s"provenance application produced unexpected planner error: ${err}", s)
        },
        { case aqsu @ AuthenticatedQSU(resultGraph, qauth) =>
          result(
            qauth.dims ≟ expectedDims,
            s"received expected authenticated QSU:\n${aqsu.shows}",
            s"received unexpected dims:\n${printMultiline(qauth.dims.toList)}\n" + // `aqsu.shows` prunes orphan dims
            s"wth graph:\n${resultGraph.shows}\n" +
            s"expected:\n[\n${printMultiline(expectedDims.toList)}\n]",
            s)
        }).merge
      }
    }
}
