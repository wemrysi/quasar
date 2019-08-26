/*
 * Copyright 2014–2019 SlamData Inc.
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

import slamdata.Predef.{Byte, Product, Serializable, List}

import quasar.{NonTerminal, RenderTree, ScalarStages}, RenderTree.ops._
import quasar.higher.HFunctor

import fs2.Stream

import monocle.Lens

import qdata.QDataDecode

import scalaz.{~>, Show}
import scalaz.std.option._
import scalaz.syntax.show._

import shims._

sealed trait QueryResult[F[_]] extends Product with Serializable {

  def data: Stream[F, _]

  def stages: ScalarStages

  def modifyData[G[_]](f: Stream[F, ?] ~> Stream[G, ?]): QueryResult[G]

  def hmap[G[_]](f: F ~> G): QueryResult[G] =
    modifyData(λ[Stream[F, ?] ~> Stream[G, ?]](_.translate[F, G](f.asCats)))
}

object QueryResult extends QueryResultInstances {

  final case class Parsed[F[_], A](decode: QDataDecode[A], data: Stream[F, A], stages: ScalarStages)
      extends QueryResult[F] {

    def modifyData[G[_]](f: Stream[F, ?] ~> Stream[G, ?]): QueryResult[G] =
      Parsed(decode, f(data), stages)
  }

  final case class Typed[F[_]](format: DataFormat, data: Stream[F, Byte], stages: ScalarStages) extends QueryResult[F] {
    def modifyData[G[_]](f: Stream[F, ?] ~> Stream[G, ?]): QueryResult[G] =
      Typed(format, f(data), stages)
    def modifyBytes[G[_]](f: Stream[F, Byte] => Stream[G, Byte]): Typed[G] =
      Typed(format, f(data), stages)
  }

  def parsed[F[_], A](q: QDataDecode[A], d: Stream[F, A], ss: ScalarStages): QueryResult[F] =
    Parsed(q, d, ss)

  def typed[F[_]](tpe: DataFormat, data: Stream[F, Byte], ss: ScalarStages): QueryResult[F] =
    Typed(tpe, data, ss)

  def stages[F[_]]: Lens[QueryResult[F], ScalarStages] =
    Lens((_: QueryResult[F]).stages)(ss => {
      case Parsed(q, d, _) => Parsed(q, d, ss)
      case Typed(f, d, _) => Typed(f, d, ss)
    })
}

sealed abstract class QueryResultInstances {
  import QueryResult._

  implicit val hfunctor: HFunctor[QueryResult] =
    new HFunctor[QueryResult] {
      def hmap[A[_], B[_]](fa: QueryResult[A])(f: A ~> B) =
        fa hmap f
    }

  implicit def renderTree[F[_]]: RenderTree[QueryResult[F]] =
    RenderTree make {
      case Parsed(_, _, ss) => NonTerminal(List("Parsed"), none, List(ss.render))
      case Typed(f, _, ss) => NonTerminal(List("Typed"), none, List(f.shows.render, ss.render))
    }

  implicit def show[F[_]]: Show[QueryResult[F]] =
    RenderTree.toShow
}
