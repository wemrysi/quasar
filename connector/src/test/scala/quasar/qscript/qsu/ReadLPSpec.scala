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

import quasar.{Data, DataArbitrary, Type, Qspec}
import quasar.Planner.PlannerError
import quasar.common.{JoinType, SortDir}
import quasar.contrib.scalaz.{NonEmptyListE => NELE}
import quasar.frontend.logicalplan.{JoinCondition, LogicalPlan}
import quasar.qscript.{Drop, Hole, MapFuncsCore, ReduceFuncs, Sample, SrcHole, Take}
import quasar.qscript.qsu.{QScriptUniform => QSU}
import quasar.sql.CompilerHelpers
import quasar.std.{AggLib, IdentityLib, MathLib, RelationsLib, SetLib, StructuralLib, TemporalPart}
import slamdata.Predef._

import matryoshka.data.Fix
import matryoshka.implicits._
import org.specs2.matcher.{Expectable, Matcher, MatchResult}
import scalaz.{\/, EitherT, Inject, Need, NonEmptyList => NEL, StateT}
import scalaz.syntax.bifunctor._
import scalaz.syntax.functor._
import scalaz.syntax.show._
import pathy.Path, Path.{file, Sandboxed}

object ReadLPSpec extends Qspec with CompilerHelpers with DataArbitrary with QSUTTypes[Fix] {
  import QSUGraph.Extractors._

  type F[A] = EitherT[StateT[Need, Long, ?], PlannerError, A]

  val reader = ReadLP[Fix, F]
  val root = Path.rootDir[Sandboxed]

  val IC = Inject[MapFuncCore, MapFunc]

  "reading lp into qsu" should {
    "convert Read nodes" in {
      read("foobar") must readQsuAs {
        case Transpose(Read(path), QSU.Rotation.ShiftMap) =>
          path mustEqual (root </> file("foobar"))
      }
    }

    // we can't do this as a property test because Data and EJson don't round-trip! :-(
    "convert Constant nodes" >> {
      "Int" >> {
        val data = Data.Int(42)

        lpf.constant(data) must readQsuAs {
          case Constant(ejson) =>
            ejson.cata(Data.fromEJson) mustEqual data
        }
      }

      "String" >> {
        val data = Data.Str("foo")

        lpf.constant(data) must readQsuAs {
          case Constant(ejson) =>
            ejson.cata(Data.fromEJson) mustEqual data
        }
      }
    }

    "convert FlattenMap" in {
      lpf.invoke1(StructuralLib.FlattenMap, read("foo")) must readQsuAs {
        case AutoJoin2C(
          Transpose(TRead(_), QSU.Rotation.FlattenMap),
          DataConstant(Data.Int(i)),
          MapFuncsCore.ProjectIndex(0, 1)) => i mustEqual 1
      }
    }

    "convert FlattenMapKeys" in {
      lpf.invoke1(StructuralLib.FlattenMapKeys, read("foo")) must readQsuAs {
        case AutoJoin2C(
          Transpose(TRead(_), QSU.Rotation.FlattenMap),
          DataConstant(Data.Int(i)),
          MapFuncsCore.ProjectIndex(0, 1)) => i mustEqual 0
      }
    }

    "convert FlattenArray" in {
      lpf.invoke1(StructuralLib.FlattenArray, read("foo")) must readQsuAs {
        case AutoJoin2C(
          Transpose(TRead(_), QSU.Rotation.FlattenArray),
          DataConstant(Data.Int(i)),
          MapFuncsCore.ProjectIndex(0, 1)) => i mustEqual 1
      }
    }

    "convert FlattenArrayIndices" in {
      lpf.invoke1(StructuralLib.FlattenArrayIndices, read("foo")) must readQsuAs {
        case AutoJoin2C(
          Transpose(TRead(_), QSU.Rotation.FlattenArray),
          DataConstant(Data.Int(i)),
          MapFuncsCore.ProjectIndex(0, 1)) => i mustEqual 0
      }
    }

    "convert ShiftMap" in {
      lpf.invoke1(StructuralLib.ShiftMap, read("foo")) must readQsuAs {
        case AutoJoin2C(
          Transpose(TRead(_), QSU.Rotation.ShiftMap),
          DataConstant(Data.Int(i)),
          MapFuncsCore.ProjectIndex(0, 1)) => i mustEqual 1
      }
    }

    "convert ShiftMapKeys" in {
      lpf.invoke1(StructuralLib.ShiftMapKeys, read("foo")) must readQsuAs {
        case AutoJoin2C(
          Transpose(TRead(_), QSU.Rotation.ShiftMap),
          DataConstant(Data.Int(i)),
          MapFuncsCore.ProjectIndex(0, 1)) => i mustEqual 0
      }
    }

    "convert ShiftArray" in {
      lpf.invoke1(StructuralLib.ShiftArray, read("foo")) must readQsuAs {
        case AutoJoin2C(
          Transpose(TRead(_), QSU.Rotation.ShiftArray),
          DataConstant(Data.Int(i)),
          MapFuncsCore.ProjectIndex(0, 1)) => i mustEqual 1
      }
    }

    "convert ShiftArrayIndices" in {
      lpf.invoke1(StructuralLib.ShiftArrayIndices, read("foo")) must readQsuAs {
        case AutoJoin2C(
          Transpose(TRead(_), QSU.Rotation.ShiftArray),
          DataConstant(Data.Int(i)),
          MapFuncsCore.ProjectIndex(0, 1)) => i mustEqual 0
      }
    }

    "convert GroupBy" in {
      lpf.invoke2(SetLib.GroupBy, read("foo"), read("bar")) must readQsuAs {
        case GroupBy(TRead("foo"), TRead("bar")) => ok
      }
    }

    "convert Squash" in {
      lpf.invoke1(IdentityLib.Squash, read("foo")) must readQsuAs {
        case DimEdit(TRead("foo"), QSU.DTrans.Squash()) => ok
      }
    }

    "convert Filter" in {
      lpf.invoke2(SetLib.Filter, read("foo"), read("bar")) must readQsuAs {
        case LPFilter(TRead("foo"), TRead("bar")) => ok
      }
    }

    "convert Sample" in {
      lpf.invoke2(SetLib.Sample, read("foo"), read("bar")) must readQsuAs {
        case Subset(TRead("foo"), Sample, TRead("bar")) => ok
      }
    }

    "convert Take" in {
      lpf.invoke2(SetLib.Take, read("foo"), read("bar")) must readQsuAs {
        case Subset(TRead("foo"), Take, TRead("bar")) => ok
      }
    }

    "convert Drop" in {
      lpf.invoke2(SetLib.Drop, read("foo"), read("bar")) must readQsuAs {
        case Subset(TRead("foo"), Drop, TRead("bar")) => ok
      }
    }

    "convert reductions" in {
      lpf.invoke1(AggLib.Count, read("foo")) must readQsuAs {
        case LPReduce(TRead("foo"), ReduceFuncs.Count(())) => ok
      }
    }

    "convert unary mapping function" in {
      lpf.invoke1(MathLib.Negate, read("foo")) must readQsuAs {
        case Map(TRead("foo"), FMFC1(MapFuncsCore.Negate(SrcHole))) => ok
      }
    }

    "convert binary mapping function" in {
      lpf.invoke2(MathLib.Add, read("foo"), read("bar")) must readQsuAs {
        case AutoJoin2C(
          TRead("foo"),
          TRead("bar"),
          MapFuncsCore.Add(0, 1)) => ok
      }
    }

    "convert ternary mapping function" in {
      lpf.invoke3(RelationsLib.Cond, read("foo"), read("bar"), read("baz")) must readQsuAs {
        case AutoJoin3C(
          TRead("foo"),
          TRead("bar"),
          TRead("baz"),
          MapFuncsCore.Cond(0, 1, 2)) => ok
      }
    }

    "convert TemporalTrunc" in {
      lpf.temporalTrunc(TemporalPart.Decade, read("foo")) must readQsuAs {
        case Map(TRead("foo"), FMFC1(MapFuncsCore.TemporalTrunc(TemporalPart.Decade, SrcHole))) => ok
      }
    }

    "convert Typecheck" in {
      lpf.typecheck(read("foo"), Type.AnyObject, read("bar"), read("baz")) must readQsuAs {
        case AutoJoin3C(
          TRead("foo"),
          TRead("bar"),
          TRead("baz"),
          MapFuncsCore.Guard(0, Type.AnyObject, 1, 2)) => ok
      }
    }

    "convert join side" in {
      lpf.joinSideName('heythere) must readQsuAs {
        case JoinSideRef('heythere) => ok
      }
    }

    "convert real join" in {
      lpf.join(
        read("foo"),
        read("bar"),
        JoinType.LeftOuter,
        JoinCondition(
          'left,
          'right,
          read("baz"))) must readQsuAs {
        case LPJoin(
          TRead("foo"),
          TRead("bar"),
          TRead("baz"),
          JoinType.LeftOuter,
          'left,
          'right) => ok
      }
    }

    "import let bindings" in {
      lpf.let(
        'tmp0,
        read("foo"),
        lpf.invoke2(
          SetLib.Filter,
          lpf.free('tmp0),
          read("bar"))) must readQsuAs {
        case LPFilter(TRead("foo"), TRead("bar")) => ok
      }
    }

    "convert a sort" in {
      lpf.sort(
        read("foo"),
        NEL(
          read("bar") -> SortDir.Ascending,
          read("baz") -> SortDir.Descending)) must readQsuAs {
        case Sort(
          TRead("foo"),
          NELE(
            (TRead("bar"), SortDir.Ascending),
            (TRead("baz"), SortDir.Descending))) => ok
      }
    }
  }

  object TRead {
    def unapply(qgraph: QSUGraph): Option[String] = qgraph match {
      case Transpose(Read(path), QSU.Rotation.ShiftMap) =>
        for {
          (front, end) <- Path.peel(path)
          file <- end.toOption
          if Path.peel(front).isEmpty
        } yield file.value

      case _ => None
    }
  }

  object AutoJoin2C {
    def unapply(qgraph: QSUGraph): Option[(QSUGraph, QSUGraph, MapFuncCore[Int])] = qgraph match {
      case AutoJoin(NELE(left, right), IC(mfc)) => Some((left, right, mfc))
      case _ => None
    }
  }

  object AutoJoin3C {
    def unapply(qgraph: QSUGraph): Option[(QSUGraph, QSUGraph, QSUGraph, MapFuncCore[Int])] = qgraph match {
      case AutoJoin(NELE(left, center, right), IC(mfc)) => Some((left, center, right, mfc))
      case _ => None
    }
  }

  object DataConstant {
    def unapply(qgraph: QSUGraph): Option[Data] = qgraph match {
      case Constant(ejson) => Some(ejson.cata(Data.fromEJson))
      case _ => None
    }
  }

  // TODO doesn't guarantee only one function; could be more!
  object FMFC1 {
    def unapply(fm: FreeMap): Option[MapFuncCore[Hole]] = {
      fm.resume.swap.toOption collect {
        case IC(mfc) => mfc.map(_ => SrcHole: Hole)
      }
    }
  }

  def readQsuAs(pf: PartialFunction[QSUGraph, MatchResult[_]]): Matcher[Fix[LogicalPlan]] = {
    new Matcher[Fix[LogicalPlan]] {
      def apply[S <: Fix[LogicalPlan]](s: Expectable[S]): MatchResult[S] = {
        val resulted = evaluate(reader(s.value)) leftMap { err =>
          failure(s"reading produced planner error: ${err.shows}", s)
        }

        val continued = resulted rightMap { qgraph =>
          val mapped = pf.lift(qgraph) map { r =>
            result(
              r.isSuccess,
              s.description + " is correct: " + r.message,
              s.description + " is incorrect: " + r.message,
              s)
          }

          // TODO Show[QSUGraph[Fix]]
          mapped.getOrElse(
            failure(s"$qgraph did not match expected pattern", s))
        }

        continued.merge
      }
    }
  }

  def evaluate[A](fa: F[A]): PlannerError \/ A = fa.run.eval(0L).value
}
