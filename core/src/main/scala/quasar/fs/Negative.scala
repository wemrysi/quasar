/*
 * Copyright 2014 - 2015 SlamData Inc.
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
package fs

import quasar.Predef._

import scalaz.Equal

final class Negative private (val value: Int) {
  override def equals(other: scala.Any) = other match {
    case Negative(a) => value == a
    case _ => false
  }
}

object Negative {
  def apply(n: Int): Option[Negative] =
    Some(n).filter(_ < 0).map(new Negative(_))
  def unapply(n: Negative) = Some(n.value)

  implicit val equal: Equal[Negative] = Equal.equalA
}
