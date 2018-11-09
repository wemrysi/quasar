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

package quasar.connector

import slamdata.Predef.{Byte, Product, Serializable}
import quasar.higher.HFunctor

import fs2.Stream
import qdata.QDataDecode
import scalaz.{~>, Const, Cord, Show}
import scalaz.syntax.show._
import shims._

sealed trait QueryResult[F[_], A] extends Product with Serializable {
  def data: Stream[F, A]

  def hmap[G[_]](f: F ~> G): QueryResult[G, A] =
    modifyData(λ[Stream[F, ?] ~> Stream[G, ?]](_.translate[F, G](f.asCats)))

  def modifyData[G[_]](f: Stream[F, ?] ~> Stream[G, ?]): QueryResult[G, A] =
    (new QueryResult.ModifyData[F, G](f))(this)
}

object QueryResult extends QueryResultInstances {
  final case class Parsed[F[_], A](decode: QDataDecode[A], data: Stream[F, A]) extends QueryResult[F, A]
  final case class Typed[F[_]](tpe: ParsableType, data: Stream[F, Byte]) extends QueryResult[F, Byte]

  def parsed[F[_], A](q: QDataDecode[A], d: Stream[F, A]): QueryResult[F, A] =
    Parsed(q, d)

  def typed[F[_]](tpe: ParsableType, data: Stream[F, Byte]): QueryResult[F, Byte] =
    Typed(tpe, data)

  ////

  // Courtesy of https://issues.scala-lang.org/browse/SI-10208, thanks scalac!
  private final class ModifyData[F[_], G[_]](f: Stream[F, ?] ~> Stream[G, ?])
      extends (QueryResult[F, ?] ~> QueryResult[G, ?]) {

    def apply[A](qr: QueryResult[F, A]) =
      qr match {
        case Parsed(q, d) => Parsed(q, f(d))
        case Typed(t, d) => Typed(t, f(d))
      }
  }
}

sealed abstract class QueryResultInstances {
  import QueryResult._

  implicit def hfunctor[D]: HFunctor[QueryResult[?[_], D]] =
    new HFunctor[QueryResult[?[_], D]] {
      def hmap[A[_], B[_]](fa: QueryResult[A, D])(f: A ~> B) =
        fa hmap f
    }

  implicit def show[F[_], A]: Show[QueryResult[F, A]] =
    Show.show(qr => (new ShowImpl[F])(qr).getConst)

  ////

  private final class ShowImpl[F[_]] extends (QueryResult[F, ?] ~> Const[Cord, ?]) {
    def apply[A](qr: QueryResult[F, A]) =
      qr match {
        case Parsed(_, _) => Const(Cord("Parsed"))
        case Typed(t, _) => Const(Cord("Typed(") ++ t.show ++ Cord(")"))
      }
  }
}
