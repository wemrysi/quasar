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

import slamdata.Predef._

import quasar.Qspec
import quasar.Planner.{InternalError, PlannerError}
import quasar.common.SortDir
import quasar.contrib.pathy.AFile
import quasar.fp._
import quasar.qscript.construction
import quasar.qscript.{
  HoleF,
  IncludeId,
  LeftSideF,
  ReduceFunc,
  ReduceFuncs,
  ReduceIndex,
  ReduceIndexF,
  RightSideF}
import quasar.qscript.MapFuncsCore.IntLit

import matryoshka.EqualT
import matryoshka.data.Fix, Fix._
import matryoshka.implicits._
import org.specs2.matcher.{Expectable, Matcher, MatchResult}
import pathy.Path, Path.{file, Sandboxed}
import scalaz.{\/, \/-, EitherT, Need, NonEmptyList => NEL, StateT}
import scalaz.Scalaz._

object GraduateSpec extends Qspec with QSUTTypes[Fix] {

  type F[A] = EitherT[StateT[Need, Long, ?], PlannerError, A]

  type QSU[A] = QScriptUniform[A]
  type QSE[A] = QScriptEducated[A]

  val grad = Graduate[Fix]

  val qsu = QScriptUniform.Dsl[Fix]
  val qse = construction.Dsl[Fix, QSE, Fix[QSE]](_.embed)
  val func = construction.Func[Fix]

  val root = Path.rootDir[Sandboxed]
  val afile: AFile = root </> file("foobar")

  "graduating QSU to QScript" should {

    "convert the QScript-ish nodes" >> {

      "convert Read" in {
        val qgraph: Fix[QSU] = qsu.read(afile)
        val qscript: Fix[QSE] = qse.Read[AFile](afile)

        qgraph must graduateAs(qscript)
      }

      "convert Map" in {
        val fm: FreeMap = func.Add(HoleF, IntLit(17))

        val qgraph: Fix[QSU] = qsu.map(qsu.read(afile), fm)
        val qscript: Fix[QSE] = qse.Map(qse.Read[AFile](afile), fm)

        qgraph must graduateAs(qscript)
      }

      "convert QSFilter" in {
        val fm: FreeMap = func.Add(HoleF, IntLit(17))

        val qgraph: Fix[QSU] = qsu.qsFilter(qsu.read(afile), fm)
        val qscript: Fix[QSE] = qse.Filter(qse.Read[AFile](afile), fm)

        qgraph must graduateAs(qscript)
      }

      "convert QSReduce" in {
        val buckets: List[FreeMap] = List(func.Add(HoleF, IntLit(17)))
        val reducers: List[ReduceFunc[FreeMap]] = List(ReduceFuncs.Count(HoleF))
        val repair: FreeMapA[ReduceIndex] = ReduceIndexF(\/-(0))

        val qgraph: Fix[QSU] = qsu.qsReduce(qsu.read(afile), buckets, reducers, repair)
        val qscript: Fix[QSE] = qse.Reduce(qse.Read[AFile](afile), buckets, reducers, repair)

        qgraph must graduateAs(qscript)
      }

      "convert LeftShift" in {
        val struct: FreeMap = func.Add(HoleF, IntLit(17))
        val repair: JoinFunc = func.ConcatArrays(func.MakeArray(LeftSideF), func.MakeArray(RightSideF))

        val qgraph: Fix[QSU] = qsu.leftShift(qsu.read(afile), struct, IncludeId, repair)
        val qscript: Fix[QSE] = qse.LeftShift(qse.Read[AFile](afile), struct, IncludeId, repair)

        qgraph must graduateAs(qscript)
      }

      "convert QSSort" in {
        val buckets: List[FreeMap] = List(func.Add(HoleF, IntLit(17)))
        val order: NEL[(FreeMap, SortDir)] = NEL(HoleF -> SortDir.Descending)

        val qgraph: Fix[QSU] = qsu.qsSort(qsu.read(afile), buckets, order)
        val qscript: Fix[QSE] = qse.Sort(qse.Read[AFile](afile), buckets, order)

        qgraph must graduateAs(qscript)
      }

      "convert Distinct" in {
        val qgraph: Fix[QSU] = qsu.distinct(qsu.read(afile))

        val qscript: Fix[QSE] =
          qse.Reduce(
            qse.Read[AFile](afile),
            List(HoleF),
            List(ReduceFuncs.Arbitrary(HoleF)),
            ReduceIndexF(\/-(0)))

        qgraph must graduateAs(qscript)
      }

      "convert Unreferenced" in {
        val qgraph: Fix[QSU] = qsu.unreferenced()
        val qscript: Fix[QSE] = qse.Unreferenced

        qgraph must graduateAs(qscript)
      }
    }

    "fail to convert the LP-ish nodes" >> {
      "not convert LPFilter" in {
        val qgraph: Fix[QSU] = qsu.lpFilter(qsu.read(afile), qsu.read(afile))

        qgraph must notGraduate
      }
    }
  }

  def graduateAs(expected: Fix[QSE]): Matcher[Fix[QSU]] = {
    new Matcher[Fix[QSU]] {
      def apply[S <: Fix[QSU]](s: Expectable[S]): MatchResult[S] = {
        val actual: PlannerError \/ Fix[QSE] =
          evaluate(grad[F](QSUGraph.fromTree[Fix](s.value)))

        actual.bimap[MatchResult[S], MatchResult[S]](
        { err =>
          failure(s"graduating produced unexpected planner error: ${err.shows}", s)
        },
        { qscript =>
          result(
            EqualT[Fix].equal[QSE](qscript, expected),
            s"received expected qscript:\n${qscript.shows}",
            s"received unexpected qscript:\n${qscript.shows}",
            s)
        }).merge
      }
    }
  }

  def notGraduate: Matcher[Fix[QSU]] = {
    new Matcher[Fix[QSU]] {
      def apply[S <: Fix[QSU]](s: Expectable[S]): MatchResult[S] = {
        val actual: PlannerError \/ Fix[QSE] =
          evaluate(grad[F](QSUGraph.fromTree[Fix](s.value)))

        // TODO better equality checking for PlannerError
        actual.bimap[MatchResult[S], MatchResult[S]](
        {
          case err @ InternalError(_, None) =>
            success(s"received expected InternalError: ${(err: PlannerError).shows}", s)
          case err =>
            failure(s"expected an InternalError without a cause, received: ${err.shows}", s)
        },
        { qscript =>
          failure(s"expected an error but found qscript:\n${qscript.shows}", s)
        }).merge
      }
    }
  }

  def evaluate[A](fa: F[A]): PlannerError \/ A = fa.run.eval(0L).value
}
