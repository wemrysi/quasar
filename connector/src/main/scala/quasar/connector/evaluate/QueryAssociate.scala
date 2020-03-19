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

package quasar.connector.evaluate

import quasar.api.resource.ResourcePath
import quasar.qscript.{QScriptEducated, InterpretedRead}

import cats.{~>, Functor}
import cats.syntax.functor._

sealed trait QueryAssociate[T[_[_]], F[_], A] {
  import QueryAssociate._

  def map[B](f: A => B)(implicit F: Functor[F]): QueryAssociate[T, F, B] =
    mapF(_.map(f))

  def mapF[G[_], B](f: F[A] => G[B]): QueryAssociate[T, G, B] =
    this match {
      case Lightweight(k) => Lightweight(k andThen f)
      case Heavyweight(k) => Heavyweight(k andThen f)
    }

  def mapK[G[_]](f: F ~> G): QueryAssociate[T, G, A] =
    mapF(f.apply)
}

object QueryAssociate extends QueryAssociateInstances {
  final case class Lightweight[T[_[_]], F[_], A](f: InterpretedRead[ResourcePath] => F[A])
      extends QueryAssociate[T, F, A]

  final case class Heavyweight[T[_[_]], F[_], A](f: T[QScriptEducated[T, ?]] => F[A])
      extends QueryAssociate[T, F, A]

  def lightweight[T[_[_]]]: PartiallyAppliedLightweight[T] =
    new PartiallyAppliedLightweight[T]

  final class PartiallyAppliedLightweight[T[_[_]]] {
    def apply[F[_], A](f: InterpretedRead[ResourcePath] => F[A])
        : QueryAssociate[T, F, A] =
      Lightweight(f)
  }

  def heavyweight[T[_[_]], F[_], A](f: T[QScriptEducated[T, ?]] => F[A])
      : QueryAssociate[T, F, A] =
    Heavyweight(f)
}

sealed abstract class QueryAssociateInstances {
  implicit def queryAssociateFunctor[T[_[_]], F[_]: Functor]: Functor[QueryAssociate[T, F, ?]] =
    new Functor[QueryAssociate[T, F, ?]] {
      def map[A, B](qa: QueryAssociate[T, F, A])(f: A => B) =
        qa.map(f)
    }
}
