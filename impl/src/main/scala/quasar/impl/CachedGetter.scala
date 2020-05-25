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

package quasar.impl

import slamdata.Predef._

import cats.Eq
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.eq._
import cats.syntax.flatMap._
import cats.syntax.functor._

import CachedGetter._

trait CachedGetter[F[_], I, A] extends (I => F[Signal[A]]) {
  def apply(i: I): F[Signal[A]]
}

object CachedGetter {
  sealed trait Signal[+A] extends Product with Serializable

  object Signal {
    final case object Empty extends Signal[Nothing]
    final case class Removed[A](value: A) extends Signal[A]
    final case class Updated[A](value: A, previous: A) extends Signal[A]
    final case class Present[A](value: A) extends Signal[A]

    def fromOptions[A: Eq](incoming: Option[A], persisted: Option[A]): Signal[A] =
      (incoming, persisted) match {
        case (None, None) => Empty
        case (None, Some(a)) => Removed(a)
        case (Some(a), Some(b)) if a =!= b => Updated(a, b)
        case (Some(a), _) => Present(a)
      }
  }

  def apply[F[_]: Sync, I, A: Eq](getter: I => F[Option[A]]): F[CachedGetter[F, I, A]] =
    Ref.of[F, Map[I, A]](Map.empty) map { (cache: Ref[F, Map[I, A]]) =>
      new CachedGetter[F, I, A] {
        def apply(i: I): F[Signal[A]] = for {
          incoming <- getter(i)
          persisted <- cache.get.map(_.get(i))
          _ <- incoming match {
            case Some(a) => cache.update(_.updated(i, a))
            case None => cache.update(_ - i)
          }
        } yield Signal.fromOptions[A](incoming, persisted)
      }
    }
}
