/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar

import quasar.Predef._
import quasar.fp._

import scala.Predef.implicitly

import matryoshka._
import matryoshka.patterns._
import monocle.macros.Lenses
import scalaz._, Scalaz._

/** Here we no longer care about provenance. Backends can’t do anything with
  * it, so we simply represent joins and crosses directly. This also means that
  * we don’t need to model certain things – project_d is just a data-level
  * function, nest_d & swap_d only modify provenance and so are irrelevant
  * here, and autojoin_d has been replaced with a lower-level join operation
  * that doesn’t include the cross portion.
  */
package object qscript extends LowPriorityImplicits {
  type Pathable[T[_[_]], A] = Coproduct[Const[DeadEnd, ?], SourcedPathable[T, ?], A]

  /** These are the operations included in all forms of QScript.
    */
  type QScriptPrim[T[_[_]], A] = Coproduct[QScriptCore[T, ?], Pathable[T, ?], A]

  /** This is the target of the core compiler. Normalization is applied to this
    * structure, and it contains no Read or EquiJoin.
    */
  type QScriptPure[T[_[_]], A] = Coproduct[ThetaJoin[T, ?], QScriptPrim[T, ?], A]
  type QScriptProject[T[_[_]], A] = Coproduct[ProjectBucket[T, ?], QScriptPure[T, ?], A]

  /** These nodes exist in all QScript structures that a backend sees.
    */
  type QScriptCommon[T[_[_]], A] = Coproduct[Read, QScriptPrim[T, ?], A]

  // The following two types are the only ones that should be seen by a backend.

  /** This is the primary form seen by a backend. It contains reads of files.
    */
  type QScript[T[_[_]], A] = Coproduct[ThetaJoin[T, ?], QScriptCommon[T, ?], A]


  /** A variant with a simpler join type. A backend can choose to operate on
    * this structure by applying the `equiJoinsOnly` transformation. Backends
    * without true join support will likely find it easier to work with this
    * than to handle full ThetaJoins.
    */
  type EquiQScript[T[_[_]], A] = Coproduct[EquiJoin[T, ?], QScriptCommon[T, ?], A]

  val ExtEJson = implicitly[ejson.Extension :<: ejson.EJson]
  val CommonEJson = implicitly[ejson.Common :<: ejson.EJson]

  sealed trait JoinSide
  final case object LeftSide extends JoinSide
  final case object RightSide extends JoinSide

  object JoinSide {
    implicit val equal: Equal[JoinSide] = Equal.equalRef
    implicit val show: Show[JoinSide] = Show.showFromToString
  }

  type FreeUnit[F[_]] = Free[F, Unit]

  type FreeMap[T[_[_]]] = FreeUnit[MapFunc[T, ?]]
  type FreeQS[T[_[_]]] = FreeUnit[QScriptProject[T, ?]]

  type JoinFunc[T[_[_]]] = Free[MapFunc[T, ?], JoinSide]

  @Lenses final case class Ann[T[_[_]]](provenance: List[FreeMap[T]], values: FreeMap[T])

  object Ann {
    implicit def equal[T[_[_]]: EqualT]: Equal[Ann[T]] =
      Equal.equal((a, b) => a.provenance ≟ b.provenance && a.values ≟ b.values)

    implicit def show[T[_[_]]: ShowT]: Show[Ann[T]] =
      Show.show(ann => Cord("Ann(") ++ ann.provenance.show ++ Cord(", ") ++ ann.values.show ++ Cord(")"))
  }

  def EmptyAnn[T[_[_]]]: Ann[T] = Ann[T](Nil, UnitF[T])

  def UnitF[T[_[_]]] = ().point[Free[MapFunc[T, ?], ?]]

  final case class SrcMerge[A, B](src: A, left: B, right: B)

  def rebase[M[_]: Bind, A](in: M[A], field: M[A]): M[A] = in >> field

  import MapFunc._
  import MapFuncs._

  def concatBuckets[T[_[_]]: Recursive: Corecursive](buckets: List[FreeMap[T]]):
      (FreeMap[T], List[FreeMap[T]]) =
    (ConcatArraysN(buckets.map(b => Free.roll(MakeArray[T, FreeMap[T]](b)))),
      buckets.zipWithIndex.map(p =>
        Free.roll(ProjectIndex[T, FreeMap[T]](
          UnitF[T],
          IntLit[T, Unit](p._2)))))

  def concat[T[_[_]]: Corecursive, A](
    l: Free[MapFunc[T, ?], A], r: Free[MapFunc[T, ?], A]):
      (Free[MapFunc[T, ?], A], FreeMap[T], FreeMap[T]) =
    (Free.roll(ConcatArrays(Free.roll(MakeArray(l)), Free.roll(MakeArray(r)))),
      Free.roll(ProjectIndex(UnitF[T], IntLit[T, Unit](0))),
      Free.roll(ProjectIndex(UnitF[T], IntLit[T, Unit](1))))

  def concat3[T[_[_]]: Corecursive, A](
    l: Free[MapFunc[T, ?], A], c: Free[MapFunc[T, ?], A], r: Free[MapFunc[T, ?], A]):
      (Free[MapFunc[T, ?], A], FreeMap[T], FreeMap[T], FreeMap[T]) =
    (Free.roll(ConcatArrays(Free.roll(ConcatArrays(Free.roll(MakeArray(l)), Free.roll(MakeArray(c)))), Free.roll(MakeArray(r)))),
      Free.roll(ProjectIndex(UnitF[T], IntLit[T, Unit](0))),
      Free.roll(ProjectIndex(UnitF[T], IntLit[T, Unit](1))),
      Free.roll(ProjectIndex(UnitF[T], IntLit[T, Unit](2))))

  // TODO: move to matryoshka

  implicit def coenvFunctor[F[_]: Functor, E]: Functor[CoEnv[E, F, ?]] =
    CoEnv.bifunctor[F].rightFunctor

  implicit def envtEqual[E: Equal, F[_]](implicit F: Delay[Equal, F]):
      Delay[Equal, EnvT[E, F, ?]] =
    new Delay[Equal, EnvT[E, F, ?]] {
      def apply[A](eq: Equal[A]) =
        Equal.equal {
          case (env1, env2) =>
            env1.ask ≟ env2.ask && F(eq).equal(env1.lower, env2.lower)
        }
    }

  implicit def envtShow[E: Show, F[_]](implicit F: Delay[Show, F]):
      Delay[Show, EnvT[E, F, ?]] =
    new Delay[Show, EnvT[E, F, ?]] {
      def apply[A](sh: Show[A]) =
        Show.show {
          envt => Cord("EnvT(") ++ envt.ask.show ++ Cord(", ") ++ F(sh).show(envt.lower) ++ Cord(")")
        }
    }

  def envtHmap[F[_], G[_], E, A](f: F ~> G): EnvT[E, F, ?] ~> EnvT[E, G, ?] =
    new (EnvT[E, F, ?] ~> EnvT[E, G, ?]) {
      def apply[A](env: EnvT[E, F, A]) = EnvT((env.ask, f(env.lower)))
    }

  def envtLowerNT[F[_], E]: EnvT[E, F, ?] ~> F = new (EnvT[E, F, ?] ~> F) {
    def apply[A](fa: EnvT[E, F, A]): F[A] = fa.lower
  }
}

abstract class LowPriorityImplicits {
  // TODO: move to matryoshka

  implicit def coenvTraverse[F[_]: Traverse, E]: Traverse[CoEnv[E, F, ?]] =
    CoEnv.bitraverse[F, Unit].rightTraverse
}

