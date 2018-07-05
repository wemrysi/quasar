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
import quasar.higher.HFunctor

import scalaz.{~>, \/, Applicative, Contravariant, Functor, Profunctor, Strong, ProChoice}
import scalaz.syntax.applicative._
import scalaz.syntax.either._

import QueryEvaluator.mapEval

trait QueryEvaluator[F[_], G[_], Q, R] extends ResourceDiscovery[F, G] {
  /** Returns the results of evaluating the given query. */
  def evaluate(query: Q): F[ReadError \/ R]
}

object QueryEvaluator extends QueryEvaluatorInstances {
  /** A `QueryEvaluator` derived from `underlying` by transforming its
    * `evaluate` function.
    */
  object mapEval {
    def apply[F[_], G[_], Q, R](underlying: QueryEvaluator[F, G, Q, R])
        : PartiallyApplied[F, G, Q, R] =
      new PartiallyApplied(underlying)

    final class PartiallyApplied[F[_], G[_], Q, R](u: QueryEvaluator[F, G, Q, R]) {
      def apply[S, T](f: (Q => F[ReadError \/ R]) => (S => F[ReadError \/ T]))
          : QueryEvaluator[F, G, S, T] =
        new DelegatingQueryEvaluator(u, f)
    }
  }
}

sealed abstract class QueryEvaluatorInstances extends QueryEvaluatorInstances0 {
  implicit def contravariant[F[_], G[_], R]: Contravariant[QueryEvaluator[F, G, ?, R]] =
    new Contravariant[QueryEvaluator[F, G, ?, R]] {
      def contramap[A, B](fa: QueryEvaluator[F, G, A, R])(f: B => A) =
        mapEval(fa)(_ compose f)
    }

  implicit def functor[F[_]: Functor, G[_], Q]: Functor[QueryEvaluator[F, G, Q, ?]] =
    new Functor[QueryEvaluator[F, G, Q, ?]] {
      def map[A, B](fa: QueryEvaluator[F, G, Q, A])(f: A => B) =
        mapEval(fa)(_ andThen (_.map(_.map(f))))
    }

  implicit def hfunctor[G[_], Q, R]: HFunctor[QueryEvaluator[?[_], G, Q, R]] =
    new HFunctor[QueryEvaluator[?[_], G, Q, R]] {
      def hmap[A[_], B[_]](fa: QueryEvaluator[A, G, Q, R])(f: A ~> B) =
        new QueryEvaluator[B, G, Q, R] {
          def evaluate(query: Q) = f(fa.evaluate(query))
          def children(path: ResourcePath) = f(fa.children(path))
          def descendants(path: ResourcePath) = f(fa.descendants(path))
          def isResource(path: ResourcePath) = f(fa.isResource(path))
        }
    }

  implicit def proChoice[F[_]: Applicative, G[_]]: ProChoice[QueryEvaluator[F, G, ?, ?]] =
    new QueryEvaluatorProfunctor[F, G] with ProChoice[QueryEvaluator[F, G, ?, ?]] {
      def left[A, B, C](fa: QueryEvaluator[F, G, A, B]) =
        mapEval(fa) { eval => { q =>
          q.fold(
            a => eval(a).map(_.map(_.left[C])),
            c => c.right[B].right[ReadError].point[F])
        }}

      def right[A, B, C](fa: QueryEvaluator[F, G, A, B]) =
        mapEval(fa) { eval => {q =>
          q.fold(
            c => c.left[B].right[ReadError].point[F],
            a => eval(a).map(_.map(_.right[C])))
        }}
    }
}

sealed abstract class QueryEvaluatorInstances0 {
  implicit def strong[F[_]: Functor, G[_]]: Strong[QueryEvaluator[F, G, ?, ?]] =
    new QueryEvaluatorProfunctor[F, G] with Strong[QueryEvaluator[F, G, ?, ?]] {
      def first[A, B, C](fa: QueryEvaluator[F, G, A, B]) =
        mapEval(fa) { eval => {
          case (a, c) => eval(a).map(_.strengthR(c))
        }}

      def second[A, B, C](fa: QueryEvaluator[F, G, A, B]) =
        mapEval(fa) { eval => {
          case (c, a) => eval(a).map(_.strengthL(c))
        }}
    }
}

private[api] abstract class QueryEvaluatorProfunctor[F[_]: Functor, G[_]]
    extends Profunctor[QueryEvaluator[F, G, ?, ?]] {

  def mapfst[A, B, C](fa: QueryEvaluator[F, G, A, B])(f: C => A) =
    Contravariant[QueryEvaluator[F, G, ?, B]].contramap(fa)(f)

  def mapsnd[A, B, C](fa: QueryEvaluator[F, G, A, B])(f: B => C) =
    Functor[QueryEvaluator[F, G, A, ?]].map(fa)(f)
}

private[api] final class DelegatingQueryEvaluator[F[_], G[_], Q, R, S, T](
    underlying: QueryEvaluator[F, G, Q, R],
    f: (Q => F[ReadError \/ R]) => (S => F[ReadError \/ T]))
    extends QueryEvaluator[F, G, S, T] {

  def evaluate(query: S) = f(underlying.evaluate)(query)
  def children(path: ResourcePath) = underlying.children(path)
  def descendants(path: ResourcePath) = underlying.descendants(path)
  def isResource(path: ResourcePath) = underlying.isResource(path)
}
