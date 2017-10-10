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

package quasar.qscript.analysis

import slamdata.Predef.{Map => _, _}

import quasar._
import quasar.RenderTree.ops._
import quasar.common.SortDir
import quasar.contrib.matryoshka._
import quasar.fp._
import quasar.fp.ski._
import quasar.qscript._

import matryoshka.{Hole => _, _}
import matryoshka.data.free._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._
import scalaz.NonEmptyList._

trait DeepShape[T[_[_]], F[_]] {
  def deepShapeƒ: Algebra[F, DeepShape.FreeShape[T]]
}

/* Computes access to the results of a qscript node,
 * seen transitively through the source.
 */
object DeepShape extends DeepShapeInstances {

  type FreeShape[T[_[_]]] = FreeMapA[T, ShapeMeta[T]]

  sealed trait ShapeMeta[T[_[_]]]
  final case class RootShape[T[_[_]]]() extends ShapeMeta[T]
  final case class UnknownShape[T[_[_]]]() extends ShapeMeta[T]
  final case class Reducing[T[_[_]]](func: ReduceFunc[FreeShape[T]]) extends ShapeMeta[T]
  final case class Shifting[T[_[_]]](id: IdStatus, struct: FreeShape[T]) extends ShapeMeta[T]

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  implicit def equal[T[_[_]]: BirecursiveT: EqualT]: Equal[ShapeMeta[T]] = {
    Equal.equal {
      case (RootShape(), RootShape()) => true
      case (UnknownShape(), UnknownShape()) => true
      case (Reducing(funcL), Reducing(funcR)) => funcL ≟ funcR
      case (Shifting(idL, structL), Shifting(idR, structR)) => idL ≟ idR && structL ≟ structR
      case (_, _) => false
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  implicit def show[T[_[_]]: ShowT]: Show[ShapeMeta[T]] =
    Show.shows {
      case RootShape() => "RootShape()"
      case UnknownShape() => "UnknownShape()"
      case Reducing(func) => s"Reducing(${func.shows})"
      case Shifting(id, func) => s"Shifting(${id.shows}, ${func.shows})"
    }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  implicit def renderTree[T[_[_]]: RenderTreeT: ShowT]: RenderTree[ShapeMeta[T]] =
    RenderTree.make {
      case RootShape() => Terminal(List("RootShape"), none)
      case UnknownShape() => Terminal(List("UnknownShape"), none)
      case Reducing(func) => NonTerminal(List("Reducing"), None, func.render :: Nil)
      case Shifting(id, func) => NonTerminal(List("Shifting"), None, id.render :: func.render :: Nil)
    }

  sealed trait RefEq[A] {
    def refEq(a1: A, a2: A): Boolean

    // this instance may not be lawful
    val equal: Equal[A] = Equal.equal(refEq)
  }

  def refEq[A](a1: A, a2: A)(implicit A0: RefEq[A]): Boolean =
    A0.refEq(a1, a2)

  /* A version of equality that compares `UnknownShape`s as unequal.
   * It is used for computing deep shape equality in source merging.
   *
   * This equality instance is not reflexive and so it is not lawful.
   */
  implicit def refEqShapeMeta[T[_[_]]: BirecursiveT: EqualT]: RefEq[ShapeMeta[T]] =
    new RefEq[ShapeMeta[T]] {
      def refEq(a1: ShapeMeta[T], a2: ShapeMeta[T]): Boolean =
        (a1, a2) match {
          case (RootShape(), RootShape()) => true
          case (UnknownShape(), UnknownShape()) => false // two unknown shapes compare as `false`
          case (Reducing(funcL), Reducing(funcR)) =>
            refEqReduceFunc[FreeShape[T]].refEq(funcL, funcR)
          case (Shifting(idL, structL), Shifting(idR, structR)) =>
            idL ≟ idR && refEqFreeShape[T].refEq(structL, structR)
          case (_, _) => false
        }
      }

  implicit def refEqFreeShape[T[_[_]]: BirecursiveT: EqualT]: RefEq[FreeShape[T]] =
    new RefEq[FreeShape[T]] {
      def refEq(a1: FreeShape[T], a2: FreeShape[T]) =
        freeEqual[MapFunc[T, ?]].apply(refEqShapeMeta.equal).equal(a1, a2)
    }

  implicit def refEqSortDir: RefEq[SortDir] =
    new RefEq[SortDir] {
      def refEq(a1: SortDir, a2: SortDir): Boolean =
        Equal[SortDir].equal(a1, a2)
    }

  implicit def refEqList[A](implicit A0: RefEq[A]): RefEq[List[A]] =
    new RefEq[List[A]] {
      def refEq(a1: List[A], a2: List[A]): Boolean =
        listEqual[A](A0.equal).equal(a1, a2)
    }

  implicit def refEqNEL[A](implicit A0: RefEq[A]): RefEq[NonEmptyList[A]] =
    new RefEq[NonEmptyList[A]] {
      def refEq(a1: NonEmptyList[A], a2: NonEmptyList[A]): Boolean =
        nonEmptyListEqual[A](A0.equal).equal(a1, a2)
    }

  implicit def refEqTuple2[A, B]
    (implicit A0: RefEq[A], B0: RefEq[B]): RefEq[(A, B)] =
    new RefEq[(A, B)] {
      def refEq(a1: (A, B), a2: (A, B)): Boolean =
        tuple2Equal(A0.equal, B0.equal).equal(a1, a2)
    }

  implicit def refEqReduceFunc[A](implicit A0: RefEq[A]): RefEq[ReduceFunc[A]] =
    new RefEq[ReduceFunc[A]] {
      def refEq(a1: ReduceFunc[A], a2: ReduceFunc[A]): Boolean =
        ReduceFunc.equal.apply(A0.equal).equal(a1, a2)
    }

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
          R: DeepShape[T, F])
          : Cofree[F, FreeShape[T]] = {

        val tupled: Cofree[F, (FreeShape[T], List[FreeShape[T]])] =
          s.cata(attributeAlgebra[F, (FreeShape[T], List[FreeShape[T]])](selfAndChildren(R.deepShapeƒ)))

        tupled.map {
          case (_, children) =>
            // this is only correct for types with a single recursive parameter
            children.headOption.getOrElse(freeShape[T](UnknownShape()))
        }
      }
    }
  }
}

sealed abstract class DeepShapeInstances {
  import DeepShape._

  implicit def coproduct[T[_[_]], F[_], G[_]]
    (implicit F: DeepShape[T, F], G: DeepShape[T, G])
      : DeepShape[T, Coproduct[F, G, ?]] =
    new DeepShape[T, Coproduct[F, G, ?]] {
      def deepShapeƒ: Algebra[Coproduct[F, G, ?], FreeShape[T]] =
        _.run.fold(F.deepShapeƒ, G.deepShapeƒ)
    }

  implicit def coenv[T[_[_]], F[_]](implicit F: DeepShape[T, F])
      : DeepShape[T, CoEnv[Hole, F, ?]] =
    new DeepShape[T, CoEnv[Hole, F, ?]] {
      def deepShapeƒ: Algebra[CoEnv[Hole, F, ?], FreeShape[T]] =
        _.run.fold(κ(freeShape[T](RootShape())), F.deepShapeƒ)
    }

  implicit def thetaJoin[T[_[_]]]: DeepShape[T, ThetaJoin[T, ?]] =
    new DeepShape[T, ThetaJoin[T, ?]] {
      def deepShapeƒ: Algebra[ThetaJoin[T, ?], FreeShape[T]] = {
        case ThetaJoin(shape, lBranch, rBranch, _, _, combine) =>
          deepShapeBranches(shape, lBranch, rBranch, combine)
      }
    }

  implicit def equiJoin[T[_[_]]]: DeepShape[T, EquiJoin[T, ?]] =
    new DeepShape[T, EquiJoin[T, ?]] {
      def deepShapeƒ: Algebra[EquiJoin[T, ?], FreeShape[T]] = {
        case EquiJoin(shape, lBranch, rBranch, _, _, combine) =>
          deepShapeBranches(shape, lBranch, rBranch, combine)
      }
    }

  // TODO We can improve the shape detection for `Sort`, `Filter`
  // and `Subset` by preserving the predicates and/or keys, similarly
  // to how we preserve reducers.
  implicit def qscriptCore[T[_[_]]]: DeepShape[T, QScriptCore[T, ?]] =
    new DeepShape[T, QScriptCore[T, ?]] {
      def deepShapeƒ: Algebra[QScriptCore[T, ?], FreeShape[T]] = {

        case Map(shape, fm) => fm >> shape

        case LeftShift(shape, struct, id, repair) =>
          repair >>= {
            case LeftSide => shape
            case RightSide => freeShape[T](Shifting[T](id, struct >> shape))
	  }

        case Reduce(shape, bucket, reducers, repair) =>
          repair >>= {
            case ReduceIndex(-\/(idx)) =>
              IList.fromList(bucket).index(idx).map(_ >> shape)
                .getOrElse(freeShape[T](UnknownShape()))

            case ReduceIndex(\/-(idx)) =>
              IList.fromList(reducers).index(idx)
                .map(func => freeShape[T](Reducing[T](func.map(_ >> shape))))
                .getOrElse(freeShape[T](UnknownShape()))
          }

        case Sort(_, _, _) => freeShape[T](UnknownShape())
        case Filter(_, _) => freeShape[T](UnknownShape())
        case Subset(_, _, _, _) => freeShape[T](UnknownShape())

        case Union(_, _, _) => freeShape[T](UnknownShape())

        case Unreferenced() => freeShape[T](RootShape())
      }
    }

  implicit def projectBucket[T[_[_]]](implicit QS: DeepShape[T, QScriptCore[T, ?]])
      : DeepShape[T, ProjectBucket[T, ?]] = {

    val proj = new SimplifiableProjectionT[T]

    new DeepShape[T, ProjectBucket[T, ?]] {
      def deepShapeƒ: Algebra[ProjectBucket[T, ?], FreeShape[T]] = {
        qs => QS.deepShapeƒ(
          proj.ProjectBucket[QScriptCore[T, ?]].simplifyProjection(qs))
      }
    }
  }

  implicit def constRead[T[_[_]], A]: DeepShape[T, Const[Read[A], ?]] =
    constShape[T, Const[Read[A], ?]](RootShape())

  implicit def constShiftedRead[T[_[_]], A]: DeepShape[T, Const[ShiftedRead[A], ?]] =
    constShape[T, Const[ShiftedRead[A], ?]](RootShape())

  implicit def constDeadEnd[T[_[_]]]: DeepShape[T, Const[DeadEnd, ?]] =
    constShape[T, Const[DeadEnd, ?]](RootShape())

  def freeShape[T[_[_]]](shape: ShapeMeta[T]): FreeShape[T] =
    Free.point[MapFunc[T, ?], ShapeMeta[T]](shape)

  private def constShape[T[_[_]], F[_]](shape: ShapeMeta[T]): DeepShape[T, F] =
    new DeepShape[T, F] {
      def deepShapeƒ: Algebra[F, FreeShape[T]] = κ(freeShape[T](shape))
    }

  private def interpretBranch[T[_[_]]]
    (branch: FreeQS[T], shape: FreeShape[T])
    (implicit QT: DeepShape[T, QScriptTotal[T, ?]])
      : FreeShape[T] =
    branch.cata(interpret(κ(shape), QT.deepShapeƒ))

  private def deepShapeBranches[T[_[_]]](
    shape: FreeShape[T],
    lBranch: FreeQS[T],
    rBranch: FreeQS[T],
    combine: JoinFunc[T])
      : FreeShape[T] =
    combine >>= {
      case LeftSide => interpretBranch(lBranch, shape)
      case RightSide => interpretBranch(rBranch, shape)
    }
}
