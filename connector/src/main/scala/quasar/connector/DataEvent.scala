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

package quasar.connector

import slamdata.Predef._

import cats.{Applicative, Bitraverse, Eq, Eval, Show}
import cats.data.NonEmptyList
import cats.instances.byte._
import cats.syntax.eq._
import cats.syntax.functor._
import cats.syntax.show._

import fs2.Chunk

sealed trait DataEvent[+I, +O] extends Product with Serializable

object DataEvent {
  final case class Replace[I](id: I, newRecord: Chunk[Byte]) extends DataEvent[I, Nothing]

  sealed trait Primitive[+I, +O] extends DataEvent[I, O]
  final case class Create(records: Chunk[Byte]) extends Primitive[Nothing, Nothing]
  final case class Delete[I](recordIds: NonEmptyList[I]) extends Primitive[I, Nothing]

  /** A transaction boundary, consumers should treat all events since the
    * previous `Commit` (or the start of the stream) as part of a single
    * transaction, if possible.
    */
  final case class Commit[O](offset: O) extends Primitive[Nothing, O]

  implicit val dataEventBitraverse: Bitraverse[DataEvent] =
    new Bitraverse[DataEvent] {
      def bifoldLeft[A, B, C](fab: DataEvent[A, B], c: C)(f: (C, A) => C, g: (C, B) => C): C =
        fab match {
          case Replace(a, _) => f(c, a)
          case Create(_) => c
          case Delete(as) => as.foldLeft(c)(f)
          case Commit(b) => g(c, b)
        }

      def bifoldRight[A, B, C](fab: DataEvent[A, B], c: Eval[C])(f: (A, Eval[C]) => Eval[C], g: (B, Eval[C]) => Eval[C]): Eval[C] =
        fab match {
          case Replace(a, _) => f(a, c)
          case Create(_) => c
          case Delete(as) => as.foldRight(c)(f)
          case Commit(b) => g(b, c)
        }

      def bitraverse[G[_]: Applicative, A, B, C, D](fab: DataEvent[A, B])(f: A => G[C], g: B => G[D]): G[DataEvent[C, D]] =
        fab match {
          case Replace(a, r) => f(a).map(Replace(_, r): DataEvent[C, D])
          case Create(r) => Applicative[G].pure[DataEvent[C, D]](Create(r))
          case Delete(as) => as.traverse(f).map(Delete(_): DataEvent[C, D])
          case Commit(b) => g(b).map(Commit(_): DataEvent[C, D])
        }

      override def bimap[A, B, C, D](fab: DataEvent[A, B])(f: A => C, g: B => D): DataEvent[C, D] =
        fab match {
          case Replace(a, r) => Replace(f(a), r)
          case Create(rs) => Create(rs)
          case Delete(as) => Delete(as map f)
          case Commit(b) => Commit(g(b))
        }
    }

  implicit def dataEventEq[I: Eq, O: Eq, E <: DataEvent[I, O]]: Eq[E] =
    Eq instance {
      case (Replace(idx, rx), Replace(idy, ry)) => idx === idy && rx === ry
      case (Create(rx), Create(ry)) => rx === ry
      case (Delete(idsx), Delete(idsy)) => idsx === idsy
      case (Commit(ox), Commit(oy)) => ox === oy
      case _ => false
    }

  implicit def dataEventShow[I: Show, O: Show, E <: DataEvent[I, O]]: Show[E] =
    Show show {
      case Replace(i, r) => s"Replace(${i.show}, ${r.size} bytes)"
      case Create(rs) => s"Create(${rs.size} bytes)"
      case Delete(ids) => ids.map(_.show).toList.mkString("Delete(", ", ", ")")
      case Commit(o) => s"Commit(${o.show})"
    }
}
