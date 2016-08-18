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
import quasar.Planner.PlannerError
import quasar.fp._

import scala.Predef.implicitly

import matryoshka._
import matryoshka.patterns._
import monocle.macros.Lenses
import pathy.Path._
import scalaz._, Scalaz._

/** Here we no longer care about provenance. Backends can’t do anything with
  * it, so we simply represent joins and crosses directly. This also means that
  * we don’t need to model certain things – project_d is just a data-level
  * function, nest_d & swap_d only modify provenance and so are irrelevant
  * here, and autojoin_d has been replaced with a lower-level join operation
  * that doesn’t include the cross portion.
  */
package object qscript extends QScriptInstances {
  private type CommonPathable[T[_[_]], A] =
    Coproduct[Const[DeadEnd, ?], SourcedPathable[T, ?], A]

  /** Statically known path components. Provided to filesystems for potential
    * conversion to `Read`.
    */
  type Pathable[T[_[_]], A] =
    Coproduct[ProjectBucket[T, ?], CommonPathable[T, ?], A]

  /** Represents QScript with portions turned into statically-known paths.
    */
  type Pathed[F[_], A] = (NonEmptyList ∘ CoEnv[AbsFile[Sandboxed], F, ?])#λ[A]

  /** A function that converts a portion of QScript to statically-known paths.
    *
    *     ProjectField(LeftShift(ProjectField(Root, StrLit("foo")),
    *                            HoleF,
    *                            RightSide),
    *                  StrLit("bar"))
    *
    * Potentially represents the path “/foo/\*\/bar”, but it could also
    * represent “/foo/\*” followed by a projection into the data. Or it could
    * represent some mixture of files at “/foo/_” referencing a record with id
    * “bar” and files at “/foo/_/bar”. It’s up to a particular mount to tell the
    * compiler which of these is the case, and it does so by providing a
    * function with this type.
    *
    * A particular mount might return a structure like this:
    *
    *     [-\/(“/foo/a/bar”),
    *      -\/("/foo/b/bar"),
    *      \/-(ProjectField([-\/(“/foo/c”), -\/(“/foo/d”)], StrLit("bar"))),
    *      -\/("/foo/e/bar"),
    *      \/-(Map([-\/("/foo/f/bar/baz")], MakeMap(StrLit("baz"), SrcHole))),
    *      \/-(Map([-\/("/foo/f/bar/quux/ducks")],
    *              MakeMap(StrLit("quux"), MakeMap(StrLit("ducks"), SrcHole))))]
    *
    * Starting from Root becoming “/”, the first projection resulted in the
    * static path “/foo”. The LeftShift then collected everything from the next
    * level of the file system – “a”, “b”, “c”, “d”, “e”, and “f”, where “c” and
    * “d” are files while the others are directories. This means that the final
    * projection is handled differently in the two contexts – on the files it
    * remains a projection, an operation to be performed on the records in a
    * file, while on the directories, it becomes another path component.
    * Finally, the path “/foo/f/bar/” is still a directory, so we recursively
    * collect all the files under that directory, and rebuild the structure of
    * them in data.
    */
  type StaticPathTransformation[T[_[_]], F[_]] =
    AlgebraicTransformM[T, PlannerError \/ ?, Pathable[T, ?], Pathed[F, ?]]

  private type QScriptTotal0[T[_[_]], A] =
    Coproduct[QScriptCore[T, ?], Pathable[T, ?], A]
  private type QScriptTotal1[T[_[_]], A] =
    Coproduct[ThetaJoin[T, ?], QScriptTotal0[T, ?], A]
  private type QScriptTotal2[T[_[_]], A] =
    Coproduct[EquiJoin[T, ?], QScriptTotal1[T, ?], A]
  /** This type is used for join branch-like structures. It’s an unfortunate
    * consequence of not having mutually-recursive data structures. Once we do,
    * this can go away.
    */
  type QScriptTotal[T[_[_]], A] =
    Coproduct[Const[Read, ?], QScriptTotal2[T, ?], A]

  val ExtEJson = implicitly[ejson.Extension :<: ejson.EJson]
  val CommonEJson = implicitly[ejson.Common :<: ejson.EJson]

  type FreeHole[F[_]] = Free[F, Hole]

  type FreeMap[T[_[_]]] = FreeHole[MapFunc[T, ?]]
  type FreeQS[T[_[_]]] = FreeHole[QScriptTotal[T, ?]]

  type JoinFunc[T[_[_]]] = Free[MapFunc[T, ?], JoinSide]

  @Lenses final case class Ann[T[_[_]]](provenance: List[FreeMap[T]], values: FreeMap[T])

  object Ann {
    implicit def equal[T[_[_]]: EqualT]: Equal[Ann[T]] =
      Equal.equal((a, b) => a.provenance ≟ b.provenance && a.values ≟ b.values)

    implicit def show[T[_[_]]: ShowT]: Show[Ann[T]] =
      Show.show(ann => Cord("Ann(") ++ ann.provenance.show ++ Cord(", ") ++ ann.values.show ++ Cord(")"))
  }

  def HoleF[T[_[_]]]: FreeMap[T] = Free.point[MapFunc[T, ?], Hole](SrcHole)
  def EmptyAnn[T[_[_]]]: Ann[T] = Ann[T](Nil, HoleF[T])

  final case class SrcMerge[A, B](src: A, left: B, right: B)

  def rebase[M[_]: Bind, A](in: M[A], field: M[A]): M[A] = in >> field

  import MapFunc._
  import MapFuncs._

  def EquiJF[T[_[_]]]: JoinFunc[T] =
    Free.roll(Eq(Free.point(LeftSide), Free.point(RightSide)))

  def concatBuckets[T[_[_]]: Recursive: Corecursive](buckets: List[FreeMap[T]]):
      (FreeMap[T], List[FreeMap[T]]) =
    (ConcatArraysN(buckets.map(b => Free.roll(MakeArray[T, FreeMap[T]](b)))),
      buckets.zipWithIndex.map(p =>
        Free.roll(ProjectIndex[T, FreeMap[T]](
          HoleF[T],
          IntLit[T, Hole](p._2)))))

  def concat[T[_[_]]: Corecursive, A](
    l: Free[MapFunc[T, ?], A], r: Free[MapFunc[T, ?], A]):
      (Free[MapFunc[T, ?], A], FreeMap[T], FreeMap[T]) =
    (Free.roll(ConcatArrays(Free.roll(MakeArray(l)), Free.roll(MakeArray(r)))),
      Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](0))),
      Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](1))))

  def concat3[T[_[_]]: Corecursive, A](
    l: Free[MapFunc[T, ?], A], c: Free[MapFunc[T, ?], A], r: Free[MapFunc[T, ?], A]):
      (Free[MapFunc[T, ?], A], FreeMap[T], FreeMap[T], FreeMap[T]) =
    (Free.roll(ConcatArrays(Free.roll(ConcatArrays(Free.roll(MakeArray(l)), Free.roll(MakeArray(c)))), Free.roll(MakeArray(r)))),
      Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](0))),
      Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](1))),
      Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](2))))

  // TODO: move to matryoshka

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

sealed trait QScriptInstances extends LowPriorityQScriptInstances {
  implicit def composedTraverse[F[_]: Traverse, G[_]: Traverse]:
      Traverse[(F ∘ G)#λ] =
    new Traverse[(F ∘ G)#λ] {
      def traverseImpl[H[_]: Applicative, A, B](fa: F[G[A]])(f: A => H[B]) =
        fa.traverse(_.traverse(f))
    }
}

sealed trait LowPriorityQScriptInstances {
  implicit def composedFunctor[F[_]: Functor, G[_]: Functor]:
      Functor[(F ∘ G)#λ] =
    new Functor[(F ∘ G)#λ] {
      def map[A, B](fa: F[G[A]])(f: A => B) = fa ∘ (_ ∘ f)
    }
}
