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

package quasar.sst

import slamdata.Predef._
import quasar.contrib.matryoshka._
import quasar.contrib.matryoshka.arbitrary._
import quasar.ejson.{EJson, EJsonArbitrary}
import quasar.ejson.implicits._
import quasar.fp._

import matryoshka._
import matryoshka.data._
import org.specs2.scalacheck._
import org.specs2.scalaz._
import scalaz.scalacheck.{ScalazProperties => propz}
import scalaz.std.anyVal._
import scalaz.syntax.functor._
import scalaz.syntax.monoid._

final class StructuralTypeSpec extends Spec with ScalazMatchers with StructuralTypeArbitrary with EJsonArbitrary {
  implicit val params = Parameters(maxSize = 5)

  type J = Fix[EJson]

  checkAll(propz.equal.laws[StructuralType[J, Int]])
  checkAll(propz.monoid.laws[StructuralType[J, Int]])
  checkAll(propz.traverse1.laws[StructuralType[J, ?]])
  // FIXME: Need Cogen
  //checkAll(propz.comonad.laws[StructuralType[J, ?]])

  "structural monoid" >> {
    "accumulates measure for identical structure" >> prop { (ejs: J, k0: Int) =>
      val k = scala.math.abs(k0 % 100) + 1
      val st = StructuralType.fromEJsonK[J](1, ejs)
      st.multiply(k) must equal(st as k)
    }
  }
}
