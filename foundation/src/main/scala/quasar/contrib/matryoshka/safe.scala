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

package quasar.contrib.matryoshka

import cats.Eval

import matryoshka.{
  ∘,
  Algebra,
  AlgebraM,
  AlgebraicGTransform,
  Coalgebra,
  CoalgebraM,
  Corecursive,
  ElgotAlgebraM,
  ElgotCoalgebra,
  GAlgebra,
  GAlgebraM,
  GCoalgebra,
  Recursive
}

import scalaz.{\/, Functor, Traverse, Monad}
import scalaz.std.tuple._
import scalaz.syntax.functor._
import scalaz.syntax.id._
import scalaz.syntax.traverse.{ToFunctorOps => _, _}
import scalaz.syntax.monad.{ToFunctorOps => _, _}

import shims.monadToScalaz

/** Stack-safe versions of various operations. */
object safe {
  /** Stack-safe if `M` is stack-safe. */
  def hyloM[M[_]: Monad, F[_]: Traverse, A, B](
      a: A)(
      f: AlgebraM[M, F, B],
      g: CoalgebraM[M, F, A])
      : M[B] =
    g(a).flatMap(_.traverse(hyloM(_)(f, g))).flatMap(f)

  def hylo[F[_]: Traverse, A, B](
      a: A)(
      f: Algebra[F, B],
      g: Coalgebra[F, A])
      : B =
    hyloM[Eval, F, A, B](a)(f.andThen(Eval.now(_)), g.andThen(Eval.now(_))).value

  def ana[T, F[_]: Traverse, A](
      a: A)(
      f: Coalgebra[F, A])(
      implicit T: Corecursive.Aux[T, F])
      : T =
    hylo[F, A, T](a)(T.embed(_), f)

  def anaM[M[_]: Monad, T, F[_]: Traverse, A](
      a: A)(
      f: CoalgebraM[M, F, A])(
      implicit T: Corecursive.Aux[T, F])
      : M[T] =
    hyloM[M, F, A, T](a)(T.embed(_).pure[M], f)

  def cata[T, F[_]: Traverse, A](
      t: T)(
      f: Algebra[F, A])(
      implicit T: Recursive.Aux[T, F])
      : A =
    hylo[F, T, A](t)(f, T.project(_))

  def cataM[M[_]: Monad, T, F[_]: Traverse, A](
      t: T)(
      f: AlgebraM[M, F, A])(
      implicit T: Recursive.Aux[T, F])
      : M[A] =
    hyloM[M, F, T, A](t)(f, T.project(_).pure[M])

  def para[T, F[_], A](
      t: T)(
      f: GAlgebra[(T, ?), F, A])(
      implicit T: Recursive.Aux[T, F], F: Traverse[F])
      : A =
    hylo[λ[α => F[(T, α)]], T, A](t)(
      f,
      T.project(_) ∘ (_.squared))(
      F.compose[(T, ?)])

  def paraM[M[_], T, F[_], A](
      t: T)(
      f: GAlgebraM[(T, ?), M, F, A])(
      implicit M: Monad[M], T: Recursive.Aux[T, F], F: Traverse[F])
      : M[A] =
    hyloM[M, λ[α => F[(T, α)]], T, A](t)(
      f,
      T.project(_).map(_.squared).pure[M])(
      M, F.compose[(T, ?)])

  def apo[T, F[_], A](
      a: A)(
      f: GCoalgebra[T \/ ?, F, A])(
      implicit T: Corecursive.Aux[T, F], F: Traverse[F])
      : T =
    hylo[λ[α => F[T \/ α]], A, T](
      a)(
      fa => T.embed(fa.map(_.merge)), f)(
      F.compose[T \/ ?])

  def elgotApo[T, F[_]: Traverse, A](
      a: A)(
      f: ElgotCoalgebra[T \/ ?, F, A])(
      implicit T: Corecursive.Aux[T, F])
      : T =
    hylo[λ[α => T \/ F[α]], A, T](a)(
      _.map(T.embed(_)).merge, f)(
      Traverse[T \/ ?].compose[F])

  def transCata[T, F[_]: Traverse, U, G[_]: Functor](
      t: T)(
      f: F[U] => G[U])(
      implicit T: Recursive.Aux[T, F], U: Corecursive.Aux[U, G])
      : U =
    cata[T, F, U](t)(f andThen (U.embed(_)))

  def transCataM[M[_]: Monad, T, F[_]: Traverse, U, G[_]: Functor](
      t: T)(
      f: F[U] => M[G[U]])(
      implicit T: Recursive.Aux[T, F], U: Corecursive.Aux[U, G])
      : M[U] =
    cataM[M, T, F, U](t)(f.andThen(_.map(U.embed(_))))

  def transAna[T, F[_]: Functor, U, G[_]: Traverse](
      t: T)(
      f: F[T] => G[T])(
      implicit T: Recursive.Aux[T, F], U: Corecursive.Aux[U, G])
      : U =
    ana[U, G, T](t)(f compose (T.project(_)))

  def transAnaM[M[_]: Monad, T, F[_]: Functor, U, G[_]: Traverse](
      t: T)(
      f: F[T] => M[G[T]])(
      implicit T: Recursive.Aux[T, F], U: Corecursive.Aux[U, G])
      : M[U] =
    anaM[M, U, G, T](t)(f compose (T.project(_)))

  def transApoT[T, F[_]: Traverse](
      t: T)(
      f: T => T \/ T)(
      implicit
      TR: Recursive.Aux[T, F],
      TC: Corecursive.Aux[T, F])
      : T =
    elgotApo(t)(f(_).map(TR.project(_)))

  object coelgotM {
    def apply[M[_]] = new PartiallyApplied[M]
    final class PartiallyApplied[M[_]] {
      def apply[F[_], A, B](
          a: A)(
          f: ElgotAlgebraM[(A, ?), M, F, B],
          g: CoalgebraM[M, F, A])(
          implicit M: Monad[M], F: Traverse[F])
          : M[B] =
        hyloM[M, ((A, ?) ∘ F)#λ, A, B](a)(
          f, a => g(a) strengthL a)(
          M, Traverse[(A, ?)] compose F)
    }
  }

  def transHylo[T, F[_]: Functor, G[_]: Traverse, U, H[_]: Functor](
      t: T)(
      f: G[U] => H[U],
      g: F[T] => G[T])(
      implicit T: Recursive.Aux[T, F], U: Corecursive.Aux[U, H])
      : U =
    hylo(t)(f.andThen(U.embed(_)), g.compose(T.project(_)))

  def transPara[T, F[_]: Traverse, U, G[_]: Functor](
      t: T)(
      f: AlgebraicGTransform[(T, ?), U, F, G])(
      implicit T: Recursive.Aux[T, F], U: Corecursive.Aux[U, G])
      : U = {

    implicit val nested: Traverse[λ[α => F[(T, α)]]] =
      Traverse[F].compose[(T, ?)]

    transHylo[T, F, λ[α => F[(T, α)]], U, G](t)(f, _ ∘ (_.squared))
  }

  def convert[T, F[_]: Traverse, R](
      t: T)(
      implicit T: Recursive.Aux[T, F], R: Corecursive.Aux[R, F])
      : R =
    cata[T, F, R](t)(R.embed(_))
}
