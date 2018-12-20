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

import slamdata.Predef.{Array, Byte, Product, Serializable, SuppressWarnings, List}

import quasar.ParseInstruction
import quasar.higher.HFunctor

import fs2.Stream

import monocle.Lens

import qdata.QDataDecode

import scalaz.{~>, Cord, Show}
import scalaz.std.list._
import scalaz.syntax.show._

import shims._

sealed trait QueryResult[F[_]] extends Product with Serializable {

  def data: Stream[F, _]

  def modifyData[G[_]](f: Stream[F, ?] ~> Stream[G, ?]): QueryResult[G]

  def hmap[G[_]](f: F ~> G): QueryResult[G] =
    modifyData(λ[Stream[F, ?] ~> Stream[G, ?]](_.translate[F, G](f.asCats)))

  val instructions: List[ParseInstruction]
}

object QueryResult extends QueryResultInstances {

  final case class Parsed[F[_], A](decode: QDataDecode[A], data: Stream[F, A], instructions: List[ParseInstruction])
      extends QueryResult[F] {
    def modifyData[G[_]](f: Stream[F, ?] ~> Stream[G, ?]): QueryResult[G] =
      Parsed(decode, f(data), instructions)
  }

  sealed trait Unparsed[F[_]] extends QueryResult[F] {
    def data: Stream[F, Byte]

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def modifyBytes[G[_]](f: Stream[F, Byte] => Stream[G, Byte]): Unparsed[G] =
      this match {
        case Compressed(s, c) => Compressed(s, c.modifyBytes(f))
        case Typed(t, d, i) => Typed(t, f(d), i)
      }

    def modifyData[G[_]](f: Stream[F, ?] ~> Stream[G, ?]): Unparsed[G] =
      modifyBytes[G](f(_))
  }

  object Unparsed {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def instructions[F[_]]: Lens[Unparsed[F], List[ParseInstruction]] =
      Lens((_: Unparsed[F]).instructions)(i => {
        case Compressed(s, u) => Compressed(s, instructions[F].set(i)(u))
        case Typed(t, d, _) => Typed(t, d, i)
      })

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    implicit def show[F[_]]: Show[Unparsed[F]] =
      Show.show {
        case Compressed(s, c) => Cord("Compressed(") ++ s.show ++ Cord(", ") ++ c.show ++ Cord(")")
        case Typed(t, _, i) => Cord("Typed(") ++ t.show ++ Cord(", ") ++ i.show ++ Cord(")")
      }
  }

  final case class Compressed[F[_]](scheme: CompressionScheme, content: Unparsed[F])
      extends Unparsed[F] {
    def data = content.data
    val instructions = content.instructions
  }

  final case class Typed[F[_]](tpe: ParsableType, data: Stream[F, Byte], instructions: List[ParseInstruction]) extends Unparsed[F]

  def compressed[F[_]](scheme: CompressionScheme, content: Unparsed[F]): Unparsed[F] =
    Compressed(scheme, content)

  def parsed[F[_], A](q: QDataDecode[A], d: Stream[F, A], i: List[ParseInstruction]): QueryResult[F] =
    Parsed(q, d, i)

  def typed[F[_]](tpe: ParsableType, data: Stream[F, Byte], i: List[ParseInstruction]): Unparsed[F] =
    Typed(tpe, data, i)

  def instructions[F[_]]: Lens[QueryResult[F], List[ParseInstruction]] =
    Lens((_: QueryResult[F]).instructions)(i => {
      case Parsed(q, d, _) => Parsed(q, d, i)
      case u: Unparsed[F] => Unparsed.instructions.set(i)(u)
    })
}

sealed abstract class QueryResultInstances {
  import QueryResult._

  implicit val hfunctor: HFunctor[QueryResult] =
    new HFunctor[QueryResult] {
      def hmap[A[_], B[_]](fa: QueryResult[A])(f: A ~> B) =
        fa hmap f
    }

  implicit def show[F[_]]: Show[QueryResult[F]] =
    Show.show {
      case Parsed(_, _, _) => Cord("Parsed")
      case u: Unparsed[F] => u.show
    }
}
