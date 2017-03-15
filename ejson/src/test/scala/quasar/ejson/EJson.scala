/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.ejson

import quasar.Predef._
import quasar.contrib.matryoshka._
import quasar.fp._, Helpers._

import matryoshka._
import org.specs2.scalaz._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties._

class EJsonSpecs extends Spec with EJsonArbitrary {
  checkAll(order.laws[Common[String]])
  checkAll(traverse.laws[Common])

  checkAll(order.laws[Obj[String]])
  checkAll(traverse.laws[Obj])

  checkAll(order.laws[Extension[String]])
  checkAll(traverse.laws[Extension])

  "extension" >> {
    "ordering ignores metadata" >> prop { (a: String, b: String, m: String, n: String) =>
      (meta(a, m) ?|? meta(b, n)) ≟ (a ?|? b)
    }
  }
}
