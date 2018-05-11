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

package quasar

import quasar.api.ResourcePath

import monocle.macros.Lenses
import scalaz.{Cord, Equal, Functor, Order, Show}
import scalaz.std.tuple._
import scalaz.syntax.show._

@Lenses
final case class Source[A](path: ResourcePath, src: A) {
  def map[B](f: A => B): Source[B] =
    Source(path, f(src))
}

object Source extends SourceInstances

sealed abstract class SourceInstances extends SourceInstances0 {
  implicit val functor: Functor[Source] =
    new Functor[Source] {
      def map[A, B](sa: Source[A])(f: A => B) =
        sa map f
    }

  implicit def order[A: Order]: Order[Source[A]] =
    Order.orderBy {
      case Source(p, a) => (p, a)
    }

  implicit def show[A: Show]: Show[Source[A]] =
    Show.show {
      case Source(p, a) =>
        Cord("Source(") ++ p.show ++ Cord(", ") ++ a.show ++ Cord(")")
    }
}

sealed abstract class SourceInstances0 {
  def equal[A: Equal]: Equal[Source[A]] =
    Equal.equalBy {
      case Source(p, a) => (p, a)
    }
}
