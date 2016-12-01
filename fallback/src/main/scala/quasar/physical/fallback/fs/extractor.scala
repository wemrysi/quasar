/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.fallback.fs

import quasar.Predef._

trait Extractor[A, B] {
  def unapply(x: A): Option[B]

  def orElse(pf: PartialFunction[A, B]): Extractor[A, B] = Extractor[A, B](x => unapply(x) orElse (pf lift x))
  def map[C](f: B => C): Extractor[A, C]                 = Extractor[A, C](x => unapply(x) map f)
  def flatMap[C](f: B => Option[C]): Extractor[A, C]     = Extractor[A, C](x => unapply(x) flatMap f)
}
object Extractor {
  def empty[A](): Extractor[A, Nothing]                         = new Empty[A]()
  def partial[A, B](pf: PartialFunction[A, B]): Extractor[A, B] = new Partial(pf)
  def apply[A, B](f: A => Option[B]): Extractor[A, B]           = new Impl(f)

  final class Impl[A, B](f: A => Option[B]) extends Extractor[A, B] {
    def unapply(x: A): Option[B] = f(x)
  }
  final class Partial[A, B](pf: PartialFunction[A, B]) extends Extractor[A, B] {
    def unapply(x: A): Option[B] = pf lift x
  }
  final class Empty[A]() extends Extractor[A, Nothing] {
    def unapply(x: A): Option[Nothing] = None
  }
}
