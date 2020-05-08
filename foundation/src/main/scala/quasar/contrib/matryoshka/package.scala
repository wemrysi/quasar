/*
 * Copyright 2020 Precog Data
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

package quasar.contrib

import slamdata.Predef._

import _root_.monocle.{Getter, Iso}
import _root_.matryoshka._
import _root_.matryoshka.data.free._
import _root_.matryoshka.implicits._
import _root_.matryoshka.patterns._
import _root_.scalaz._, Scalaz._

package object matryoshka {
  /** Chains multiple transformations together, each of which can fail to change
    * anything.
    */
  def applyTransforms[A](first: A => Option[A], rest: (A => Option[A])*)
      : A => Option[A] =
    rest.foldLeft(
      first)(
      (prev, next) => x => prev(x).fold(next(x))(orOriginal(next)(_).some))

  def birecursiveIso[T, F[_]: Functor]
      (implicit TC: Corecursive.Aux[T, F], TR: Recursive.Aux[T, F])
      : Iso[T, F[T]] =
    Iso[T, F[T]](TR.project(_))(TC.embed(_))

  object convertToFree {
    def apply[F[_], A] = new PartiallyApplied[F, A]
    final class PartiallyApplied[F[_], A] {
      def apply[T](t: T)(implicit T: Recursive.Aux[T, F], F: Functor[F]): Free[F, A] =
        t.ana[Free[F, A]](x => CoEnv(x.project.right[A]))
    }
  }

  def envT[E, W[_], A](e: E, wa: W[A]): EnvT[E, W, A] =
    EnvT((e, wa))

  def envTIso[E, W[_], A]: Iso[EnvT[E, W, A], (E, W[A])] =
    Iso((_: EnvT[E, W, A]).runEnvT)(EnvT(_))

  def project[T, F[_]: Functor](implicit T: Recursive.Aux[T, F]): Getter[T, F[T]] =
    Getter(T.project(_))

  /** Make a partial endomorphism total by returning the argument when undefined. */
  def totally[A](pf: PartialFunction[A, A]): A => A =
    orOriginal(pf.lift)

  /** Derive a recursive instance over the functor transformed by EnvT by forgetting the annotation. */
  def forgetRecursive[T, E, F[_]](implicit T: Recursive.Aux[T, EnvT[E, F, ?]]): Recursive.Aux[T, F] =
    new Recursive[T] {
      type Base[B] = F[B]

      def project(t: T)(implicit BF: Functor[Base]) =
        T.project(t).lower
    }

  /** Derive a corecursive instance over the functor transformed by EnvT using the zero of the annotation monoid. */
  def rememberCorecursive[T, E: Monoid, F[_]](implicit T: Corecursive.Aux[T, EnvT[E, F, ?]]): Corecursive.Aux[T, F] =
    new Corecursive[T] {
      type Base[B] = F[B]

      def embed(ft: Base[T])(implicit BF: Functor[Base]) =
        T.embed(envT(∅[E], ft))
    }

  def selfAndChildren[F[_]: Functor: Foldable, A](alg: Algebra[F, A])
      : Algebra[F, (A, List[A])] =
    faas => {
      val fa = faas map (_._1)
      (alg(fa), fa.toList)
    }

  implicit def delayOrder[F[_], A](implicit F: Delay[Order, F], A: Order[A]): Order[F[A]] =
    F(A)

  implicit def coproductOrder[F[_], G[_]](implicit F: Delay[Order, F], G: Delay[Order, G]): Delay[Order, Coproduct[F, G, ?]] =
    new Delay[Order, Coproduct[F, G, ?]] {
      def apply[A](ord: Order[A]): Order[Coproduct[F, G, A]] = {
        implicit val ordA: Order[A] = ord
        Order.orderBy((_: Coproduct[F, G, A]).run)
      }
    }

  implicit def AlgebraMZip[M[_]: Applicative, F[_]: Functor]
      : Zip[AlgebraM[M, F, ?]] =
    new Zip[AlgebraM[M, F, ?]] {
      def zip[A, B](a: ⇒ AlgebraM[M, F, A], b: ⇒ AlgebraM[M, F, B]) =
        w => Bitraverse[(?, ?)].bisequence((a(w ∘ (_._1)), b(w ∘ (_._2))))
    }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def transHyloM[T, F[_], G[_]: Traverse, U, H[_]: Traverse, M[_]: Monad]
    (t: T)
    (φ: G[U] => M[H[U]], ψ: F[T] => M[G[T]])
    (implicit T: Recursive.Aux[T, F], U: Corecursive.Aux[U, H], BF: Traverse[F])
  : M[U] =
    T.traverseR(t)(ψ(_) >>= (_.traverse(transHyloM(_)(φ, ψ))) >>= φ)

  implicit def freeEqualT[A: Equal]: EqualT[Free[?[_], A]] = new EqualT[Free[?[_], A]] {
    def equal[F[_]: Functor]
    (tf1: Free[F, A], tf2: Free[F, A])
    (implicit del: Delay[Equal, F]) =
      (tf1.resume, tf2.resume) match {
        case (-\/(l1), -\/(l2)) => del(equalT[F](del)).equal(l1, l2)
        case (\/-(r1), \/-(r2)) => r1 ≟ r2
        case _ => false
      }
  }
}
