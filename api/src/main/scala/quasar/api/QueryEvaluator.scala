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

package quasar.api

import quasar.api.ResourceError.ReadError

import scalaz.{\/, Applicative, Contravariant, Functor, Profunctor, Strong, ProChoice}
import scalaz.syntax.applicative._
import scalaz.syntax.either._

trait QueryEvaluator[F[_], Q, R] extends ResourceDiscovery[F] {
  /** Returns the results of evaluating the given query. */
  def evaluate(query: Q): F[ReadError \/ R]
}

object QueryEvaluator extends QueryEvaluatorInstances

sealed abstract class QueryEvaluatorInstances extends QueryEvaluatorInstances0 {
  implicit def contravariant[F[_], R]: Contravariant[QueryEvaluator[F, ?, R]] =
    new Contravariant[QueryEvaluator[F, ?, R]] {
      def contramap[A, B](fa: QueryEvaluator[F, A, R])(f: B => A) =
        DelegatingQueryEvaluator[F, A, R, B, R](fa)(_ compose f)
    }

  implicit def functor[F[_]: Functor, Q]: Functor[QueryEvaluator[F, Q, ?]] =
    new Functor[QueryEvaluator[F, Q, ?]] {
      def map[A, B](fa: QueryEvaluator[F, Q, A])(f: A => B) =
        DelegatingQueryEvaluator[F, Q, A, Q, B](fa)(_ andThen (_.map(_.map(f))))
    }

  implicit def proChoice[F[_]: Applicative]: ProChoice[QueryEvaluator[F, ?, ?]] =
    new QueryEvaluatorProfunctor[F] with ProChoice[QueryEvaluator[F, ?, ?]] {
      def left[A, B, C](fa: QueryEvaluator[F, A, B]) =
        DelegatingQueryEvaluator[F, A, B, (A \/ C), (B \/ C)](fa) { eval => { q =>
          q.fold(
            a => eval(a).map(_.map(_.left[C])),
            c => c.right[B].right[ReadError].point[F])
        }}

      def right[A, B, C](fa: QueryEvaluator[F, A, B]) =
        DelegatingQueryEvaluator[F, A, B, (C \/ A), (C \/ B)](fa) { eval => {q =>
          q.fold(
            c => c.left[B].right[ReadError].point[F],
            a => eval(a).map(_.map(_.right[C])))
        }}
    }
}

sealed abstract class QueryEvaluatorInstances0 {
  implicit def strong[F[_]: Functor]: Strong[QueryEvaluator[F, ?, ?]] =
    new QueryEvaluatorProfunctor[F] with Strong[QueryEvaluator[F, ?, ?]] {
      def first[A, B, C](fa: QueryEvaluator[F, A, B]) =
        DelegatingQueryEvaluator[F, A, B, (A, C), (B, C)](fa) { eval => {
          case (a, c) => eval(a).map(_.strengthR(c))
        }}

      def second[A, B, C](fa: QueryEvaluator[F, A, B]) =
        DelegatingQueryEvaluator[F, A, B, (C, A), (C, B)](fa) { eval => {
          case (c, a) => eval(a).map(_.strengthL(c))
        }}
    }
}

private[api] abstract class QueryEvaluatorProfunctor[F[_]: Functor]
    extends Profunctor[QueryEvaluator[F, ?, ?]] {

  def mapfst[A, B, C](fa: QueryEvaluator[F, A, B])(f: C => A) =
    Contravariant[QueryEvaluator[F, ?, B]].contramap(fa)(f)

  def mapsnd[A, B, C](fa: QueryEvaluator[F, A, B])(f: B => C) =
    Functor[QueryEvaluator[F, A, ?]].map(fa)(f)
}

private[api] final class DelegatingQueryEvaluator[F[_], Q, R, S, T](
    underlying: QueryEvaluator[F, Q, R],
    f: (Q => F[ReadError \/ R]) => (S => F[ReadError \/ T]))
    extends QueryEvaluator[F, S, T] {

  def evaluate(query: S) = f(underlying.evaluate)(query)
  def children(path: ResourcePath) = underlying.children(path)
  def descendants(path: ResourcePath) = underlying.descendants(path)
  def isResource(path: ResourcePath) = underlying.isResource(path)
}

private[api] object DelegatingQueryEvaluator {
  def apply[F[_], Q, R, S, T](
      u: QueryEvaluator[F, Q, R])(
      f: (Q => F[ReadError \/ R]) => (S => F[ReadError \/ T]))
      : QueryEvaluator[F, S, T] =
    new  DelegatingQueryEvaluator(u, f)
}
