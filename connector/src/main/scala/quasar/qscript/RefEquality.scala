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

package quasar.qscript

import slamdata.Predef._

import quasar._
import quasar.RenderTree.ops._
import quasar.fp._
import quasar.fp.ski._

import matryoshka._
import matryoshka.data.free._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._

trait RefEq[T[_[_]], F[_]] {
  def refEqƒ: Algebra[F, RefEq.FreeShape[T]]
}

object RefEq extends RefEqInstances {

  sealed trait ShapeMeta[T[_[_]]]
  final case class EqualShape[T[_[_]]]() extends ShapeMeta[T]
  final case class UnequalShape[T[_[_]]]() extends ShapeMeta[T]
  final case class Reducing[T[_[_]]](func: ReduceFunc[FreeMap[T]]) extends ShapeMeta[T]
  final case class Shifting[T[_[_]]](id: IdStatus, struct: FreeMap[T]) extends ShapeMeta[T]

  implicit def equal[T[_[_]]: BirecursiveT: EqualT]: Equal[ShapeMeta[T]] = {
    Equal.equal {
      case (EqualShape(), EqualShape()) => true
      case (UnequalShape(), UnequalShape()) => true
      case (Reducing(funcL), Reducing(funcR)) => funcL ≟ funcR
      case (Shifting(idL, structL), Shifting(idR, structR)) => idL ≟ idR && structL ≟ structR
      case (_, _) => false
    }
  }

  implicit def renderTree[T[_[_]]: RenderTreeT: ShowT]: RenderTree[ShapeMeta[T]] =
    RenderTree.make {
      case EqualShape() => Terminal(List("EqualShape"), none)
      case UnequalShape() => Terminal(List("UnequalShape"), none)
      case Reducing(func) => NonTerminal(List("Reducing"), None, func.render :: Nil)
      case Shifting(id, func) => NonTerminal(List("Shifting"), None, id.render :: func.render :: Nil)
    }

  type FreeShape[T[_[_]]] = FreeMapA[T, ShapeMeta[T]]

  def normalize[T[_[_]]: BirecursiveT: EqualT](shape: FreeShape[T]): FreeShape[T] =
    shape.transCata[FreeShape[T]](MapFuncCore.normalize[T, ShapeMeta[T]])

  object annotated {
    def apply[F[_]] = new PartiallyApplied[F]

    final class PartiallyApplied[F[_]] {
      def apply[T[_[_]], S](s: S)(
          implicit
          TR: Recursive.Aux[S, F],
          Fu: Functor[F],
          Fo: Foldable[F],
          R: RefEq[T, F])
          : Cofree[F, FreeShape[T]] = {

        val tupled: Cofree[F, (FreeShape[T], List[FreeShape[T]])] =
          s.cata(attributeAlgebra[F, (FreeShape[T], List[FreeShape[T]])](selfAndChildren(R.refEqƒ)))

        tupled.map {
          case (_, children) =>
            children.headOption.getOrElse(freeShape[T](UnequalShape()))
        }
      }
    }
  }
}

sealed abstract class RefEqInstances {
  import RefEq._

  implicit def coproduct[T[_[_]], F[_], G[_]]
    (implicit F: RefEq[T, F], G: RefEq[T, G])
      : RefEq[T, Coproduct[F, G, ?]] =
    new RefEq[T, Coproduct[F, G, ?]] {
      def refEqƒ: Algebra[Coproduct[F, G, ?], FreeShape[T]] =
        _.run.fold(F.refEqƒ, G.refEqƒ)
    }

  implicit def coenv[T[_[_]], F[_]](implicit F: RefEq[T, F])
      : RefEq[T, CoEnv[Hole, F, ?]] =
    new RefEq[T, CoEnv[Hole, F, ?]] {
      def refEqƒ: Algebra[CoEnv[Hole, F, ?], FreeShape[T]] =
        _.run.fold(κ(freeShape[T](EqualShape())), F.refEqƒ)
    }

  implicit def thetaJoin[T[_[_]]]: RefEq[T, ThetaJoin[T, ?]] =
    new RefEq[T, ThetaJoin[T, ?]] {
      def refEqƒ: Algebra[ThetaJoin[T, ?], FreeShape[T]] = {
        case ThetaJoin(shape, lBranch, rBranch, _, _, combine) =>
          refEqBranches(shape, lBranch, rBranch, combine)
      }
    }

  implicit def equiJoin[T[_[_]]]: RefEq[T, EquiJoin[T, ?]] =
    new RefEq[T, EquiJoin[T, ?]] {
      def refEqƒ: Algebra[EquiJoin[T, ?], FreeShape[T]] = {
        case EquiJoin(shape, lBranch, rBranch, _, _, combine) =>
          refEqBranches(shape, lBranch, rBranch, combine)
      }
    }

  implicit def qscriptCore[T[_[_]]]: RefEq[T, QScriptCore[T, ?]] =
    new RefEq[T, QScriptCore[T, ?]] {
      def refEqƒ: Algebra[QScriptCore[T, ?], FreeShape[T]] = {

        case Map(shape, fm) => fm >> shape

        case LeftShift(shape, struct, id, repair) =>
	  repair >>= {
	    case LeftSide => shape
	    case RightSide => freeShape[T](Shifting[T](id, struct))
	  }

        case Reduce(shape, bucket, reducers, repair) =>
          repair >>= {
            case ReduceIndex(-\/(idx)) =>
              IList.fromList(bucket).index(idx).map(_ >> shape)
	        .getOrElse(freeShape[T](UnequalShape()))

            case ReduceIndex(\/-(idx)) =>
              IList.fromList(reducers).index(idx).map {
	        case ReduceFuncs.Arbitrary(inner) => inner >> shape
		case ReduceFuncs.First(inner) => inner >> shape
		case ReduceFuncs.Last(inner) => inner >> shape
		case func => freeShape[T](Reducing[T](func))
	      }.getOrElse(freeShape[T](UnequalShape()))
          }

        case Sort(shape, _, _) => shape
        case Filter(shape, _) => shape
        case Subset(shape, _, _, _) => shape

        case Union(_, _, _) => freeShape[T](UnequalShape())

        case Unreferenced() => freeShape[T](EqualShape())
      }
    }

  implicit def projectBucket[T[_[_]]]: RefEq[T, ProjectBucket[T, ?]] =
    constShape[T, ProjectBucket[T, ?]](UnequalShape())

  implicit def constRead[T[_[_]], A]: RefEq[T, Const[Read[A], ?]] =
    constShape[T, Const[Read[A], ?]](EqualShape())

  implicit def constShiftedRead[T[_[_]], A]: RefEq[T, Const[ShiftedRead[A], ?]] =
    constShape[T, Const[ShiftedRead[A], ?]](EqualShape())

  implicit def constDeadEnd[T[_[_]]]: RefEq[T, Const[DeadEnd, ?]] =
    constShape[T, Const[DeadEnd, ?]](EqualShape())

  def freeShape[T[_[_]]](shape: ShapeMeta[T]): FreeShape[T] =
    Free.point[MapFunc[T, ?], ShapeMeta[T]](shape)

  private def constShape[T[_[_]], F[_]](shape: ShapeMeta[T]): RefEq[T, F] =
    new RefEq[T, F] {
      def refEqƒ: Algebra[F, FreeShape[T]] = κ(freeShape[T](shape))
    }

  private def interpretBranch[T[_[_]]]
    (branch: FreeQS[T])
    (implicit QT: RefEq[T, QScriptTotal[T, ?]])
      : FreeShape[T] =
    branch.cata(interpret(κ(freeShape[T](EqualShape())), QT.refEqƒ))

  private def refEqBranches[T[_[_]]](
    shape: FreeShape[T],
    lBranch: FreeQS[T],
    rBranch: FreeQS[T],
    combine: JoinFunc[T])
      : FreeShape[T] =

    combine >>= {
      case LeftSide => interpretBranch(lBranch) >> shape
      case RightSide => interpretBranch(rBranch) >> shape
  }
}
