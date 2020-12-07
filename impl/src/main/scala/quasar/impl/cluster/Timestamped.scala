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

package quasar.impl.cluster

import slamdata.Predef._

import cats.{Applicative, Eval, Functor, Traverse}
import cats.effect.Timer
import cats.syntax.functor._

import scodec.Codec
import scodec.codecs.{either, bool, int64}

import scala.concurrent.duration._

trait Timestamped[+A] extends Product with Serializable {
  def timestamp: Long
}

object Timestamped {
  final case class Tombstone(timestamp: Long) extends Timestamped[Nothing]
  final case class Tagged[A](raw: A, timestamp: Long) extends Timestamped[A]

  def tombstone[F[_]: Functor: Timer, A]: F[Timestamped[A]] =
    Timer[F].clock.realTime(MILLISECONDS).map(Tombstone(_))

  def tagged[F[_]: Functor: Timer, A](raw: A): F[Timestamped[A]] =
    Timer[F].clock.realTime(MILLISECONDS).map(Tagged(raw, _))

  def raw[A](v: Timestamped[A]): Option[A] = v match {
    case Tombstone(_) => None
    case Tagged(raw, _) => Some(raw)
  }

  def timestamp(v: Timestamped[_]): Long =
    v.timestamp

  implicit def mapValueCodec[A](implicit a: Codec[A]): Codec[Timestamped[A]] =
    either(bool, int64, a ~ int64).xmapc({
      case Left(t) => Tombstone(t)
      case Right((v, t)) => Tagged(v, t)
    })({
      case Tombstone(t) => Left(t)
      case Tagged(v, t) => Right((v, t))
    })

  implicit val timestampedTraverse: Traverse[Timestamped] =
    new Traverse[Timestamped] {
      def traverse[F[_]: Applicative, A, B](fa: Timestamped[A])(f: A => F[B]) =
        fa match {
          case Tombstone(ts) => Applicative[F].pure[Timestamped[B]](Tombstone(ts))
          case Tagged(a, ts) => f(a).map(Tagged(_, ts))
        }

      def foldLeft[A, B](fa: Timestamped[A], b: B)(f: (B, A) => B): B =
        fa match {
          case Tombstone(_) => b
          case Tagged(a, _) => f(b, a)
        }

      def foldRight[A, B](fa: Timestamped[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
        fa match {
          case Tombstone(_) => lb
          case Tagged(a, _) => f(a, lb)
        }
    }
}
