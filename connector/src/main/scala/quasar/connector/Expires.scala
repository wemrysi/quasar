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

import java.time.LocalDateTime

import cats.Functor
import cats.effect.Sync
import cats.implicits._
import cats.Apply
import scala._

final case class Expires[A](value: A, expiresAt: Option[LocalDateTime]) {
  private def now[F[_]: Sync]: F[LocalDateTime] = 
    Sync[F].delay(LocalDateTime.now())

  def isExpired[F[_]: Sync]: F[Boolean] = 
    expiresAt match {
      case None => false.pure[F]
      case Some(exp) => now.map(exp.isBefore(_))
    }
    
  def nonExpired[F[_]: Sync]: F[Option[A]] = 
    Apply[F].ifF(isExpired)(None, Some(value))
}

object Expires {
  def never[A](value: A): Expires[A] =
    Expires(value, None)

  implicit def expiresFunctor: Functor[Expires] = new Functor[Expires] {
    def map[A, B](fa: Expires[A])(f: A => B): Expires[B] =
      Expires(f(fa.value), fa.expiresAt)
  }
}
