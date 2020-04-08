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

package quasar.impl.external

import slamdata.Predef._

import cats.{Eq, Show}
import cats.implicits._

// NB: this is actually more general, I'm just sticking it here for now
final case class DisplayName(lowercase: String, uppercase: String, plural: String)

object DisplayName {

  implicit val eq: Eq[DisplayName] =
    Eq.by(dn => (dn.lowercase, dn.uppercase, dn.plural))

  implicit val show: Show[DisplayName] =
    Show.show(dn => s"DisplayName(lowercase = ${dn.lowercase}, uppercase = ${dn.uppercase}, plural = ${dn.plural})")
}
