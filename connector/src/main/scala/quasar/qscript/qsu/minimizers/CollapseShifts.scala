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
package minimizers

import quasar.{NameGenerator, Planner}
import Planner.PlannerErrorME
import quasar.contrib.matryoshka._
import quasar.contrib.scalaz.MonadState_
import quasar.fp._
import quasar.fp.ski.κ
import quasar.qscript.{construction, Hole, SrcHole}
import quasar.qscript.qsu.{QScriptUniform => QSU}
import slamdata.Predef._
import matryoshka.{BirecursiveT, EqualT, ShowT, delayEqual}
import slamdata.Predef

import scalaz.{\/, -\/, \/-, Monad, Monoid, Scalaz}
import Scalaz._

final class CollapseShifts[T[_[_]]: BirecursiveT: EqualT: ShowT] private () extends Minimizer[T] {
  import MinimizeAutoJoins._
  import QSUGraph.Extractors._

  val func = construction.Func[T]
  val srcHole: Hole = SrcHole

  def extractShifts(candidates: List[QSUGraph]): List[QSU.LeftShift[T, QSUGraph] \/ QSU.MultiLeftShift[T, QSUGraph]] =
    candidates collect {
      case LeftShift(source, struct, ids, repair, rot) =>
        QSU.LeftShift(source, struct, ids, repair, rot).left
      case MultiLeftShift(source, shifts, repair) =>
        QSU.MultiLeftShift(source, shifts, repair).right
    }

  def couldApplyTo(candidates: Predef.List[QSUGraph]): Predef.Boolean =
    candidates.lengthCompare(extractShifts(candidates).length) === 0

  def extract[
      G[_]: Monad: NameGenerator: PlannerErrorME: MonadState_[?[_], RevIdx]: MonadState_[?[_], MinimizationState[T]]](
      qgraph: QSUGraph): Option[(QSUGraph, (QSUGraph, FreeMap) => G[QSUGraph])] = qgraph match {

    case qgraph@LeftShift(src, struct, idStatus, repair, rot) =>
      def rebuild(src: QSUGraph, fm: FreeMap): G[QSUGraph] = {
        val struct2 = struct.flatMap(κ(fm))

        val repair2 = repair flatMap[QSU.ShiftTarget[T]] {
          case QSU.AccessLeftTarget(Access.Value(_)) => fm.map(κ(QSU.AccessLeftTarget(Access.valueHole(SrcHole))))
          case QSU.AccessLeftTarget(access) => (QSU.AccessLeftTarget[T](access): QSU.ShiftTarget[T]).pure[FreeMapA]
          case QSU.LeftTarget() => scala.sys.error("QSU.LeftTarget in CollapseShifts")
          case QSU.RightTarget() => func.RightTarget
        }

        updateGraph[T, G](QSU.LeftShift(src.root, struct2, idStatus, repair2, rot)) map { rewritten =>
          qgraph.overwriteAtRoot(rewritten.vertices(rewritten.root)) :++ rewritten :++ src
        }
      }

      Some((src, rebuild _))

    case qgraph@MultiLeftShift(src, shifts, repair) =>
      def rebuild(src: QSUGraph, fm: FreeMap): G[QSUGraph] = {
        updateGraph[T, G](QSU.MultiLeftShift(src.root, shifts, repair)).map { rewritten =>
          qgraph.overwriteAtRoot(rewritten.vertices(rewritten.root)) :++ rewritten :++ src
        }
      }

      Some((src, rebuild _))

    case _ => None
  }

      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def mapAccum[A, B, C: Monoid](in: List[A])(count: A => C)(f: (A, C) => B): List[B] = {
        @tailrec def loop(in: List[A], cnt: C, out: List[B]): List[B] = in match {
          case a :: as =>
            val newCnt = cnt |+| count(a)
            loop(as, newCnt, f(a, cnt) :: out)
          case Nil => out
        }

        loop(in, Monoid[C].zero, Nil)
      }

      def apply[
      G[_] : Monad : NameGenerator : PlannerErrorME : MonadState_[?[_], RevIdx] : MonadState_[?[_], MinimizationState[T]]](
      qgraph: QSUGraph, singleSource: QSUGraph, candidates: Predef.List[QSUGraph], fm: FreeMapA[Predef.Int]): G[Predef.Option[(QSUGraph, QSUGraph)]] = {
        val shifts = extractShifts(candidates)
        val repairs = mapAccum(shifts) {
          case -\/(_) => 1
          case \/-(mls) => mls.shifts.length
        } {
          case (-\/(ls), idx) => ls.repair.map {
            case QSU.LeftTarget() => scala.sys.error("QSU.LeftTarget in CollapseShifts")
            case QSU.AccessLeftTarget(access) => access.left[Int]
            case QSU.RightTarget() => idx.right[QAccess[Hole]]
          }
          case (\/-(mls), idx) => mls.repair.map(_.map(_ + idx))
        }
        val shiftsLength = shifts.length
        val repair = fm.flatMap { idx =>
          // get is safe because fm's indices line up with updatedShifts'
          repairs.index(shiftsLength - idx - 1).getOrElse(???)
        }
        val allShifts = shifts.flatMap {
          case -\/(QSU.LeftShift(_, struct, idStatus, _, rot)) =>
            (struct, idStatus, rot) :: Nil
          case \/-(multiShift) =>
            multiShift.shifts
        }
        updateGraph[T, G](QSU.MultiLeftShift[T, Symbol](singleSource.root, allShifts, repair)) map { rewritten =>
          val back = qgraph.overwriteAtRoot(rewritten.vertices(rewritten.root)) :++ rewritten

          (back, back).some
        }
      }
  }

object CollapseShifts {
  def apply[T[_[_]]: BirecursiveT: EqualT: ShowT]: CollapseShifts[T] =
    new CollapseShifts[T]
}
