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

package quasar.qscript.provenance

import slamdata.Predef.{Char, Int}
import quasar.Qspec

import matryoshka._
import matryoshka.data.Fix
import scalaz.{IList, NonEmptyList}
import scalaz.std.anyVal._

object DimensionSpec extends Qspec {
  val P = Prov[Char, Int, Fix[ProvF[Char, Int, ?]]]
  val D = Dimension(P)

  import P.implicits._

  "project path on empty dims creates new" >> {
    D.projectPath('q', D.empty) must_= Dimensions.origin(P.prjPath('q'))
  }

  "project static on empty dims is empty" >> {
    D.projectStatic('x', D.empty) must_= D.empty
  }

  "project static key known not to exist is empty" >> {
    val d = D.injectStatic('y', D.lshift(1, D.projectStatic('c', D.projectPath('f', D.empty))))
    D.projectStatic('z', d) must_= D.empty
  }

  "project static eliminates inject static" >> {
    val init = Dimensions.origin(
      P.both(P.injValue('x'), P.injValue('y')),
      P.value(3),
      P.prjPath('a'))

    val exp = Dimensions.origin(P.value(3), P.prjPath('a'))

    D.projectStatic('x', init) must_= exp
  }

  "project static eliminates inject static within sequence" >> {
    val init = Dimensions.origin(
      P.thenn(
        P.both(P.injValue('x'), P.injValue('y')),
        P.value(3)),
      P.prjPath('a'))

    val exp = Dimensions.origin(P.value(3), P.prjPath('a'))

    D.projectStatic('x', init) must_= exp
  }

  "inject static on empty dims is empty" >> {
    D.injectStatic('x', D.empty) must_= D.empty
  }

  "autojoin keys" >> {
    "builds join keys from zipped dimensions" >> {
      val x = D.lshift(1, D.lshift(2, D.projectPath('a', D.empty)))
      val y = D.lshift(3, D.lshift(4, D.projectPath('a', D.empty)))

      val jks = JoinKeys(IList(NonEmptyList(
        NonEmptyList(JoinKey(1, 3)),
        NonEmptyList(JoinKey(2, 4)))))

      D.autojoinKeys(x, y) must_= jks
    }

    "does not include keys for dimensions after a partial join" >> {
      val x = D.lshift(3, D.flatten(2, D.lshift(1, D.projectPath('a', D.empty))))
      val y = D.lshift(9, D.lshift(8, D.lshift(7, D.projectPath('a', D.empty))))

      val jks = JoinKeys(IList(NonEmptyList(NonEmptyList(JoinKey(1, 7)))))

      D.autojoinKeys(x, y) must_= jks
    }

    "does not include keys for dimensions after mismatched projection" >> {
      val x = D.lshift(3, D.projectStatic('k', D.lshift(1, D.projectPath('a', D.empty))))
      val y = D.lshift(9, D.lshift(7, D.projectPath('a', D.empty)))

      val jks = JoinKeys(IList(NonEmptyList(NonEmptyList(JoinKey(1, 7)))))

      D.autojoinKeys(x, y) must_= jks
    }

    "does not include keys after a mismatch" >> {
      val x = D.lshift(2, D.flatten(1, D.projectPath('b', D.projectPath('a', D.empty))))
      val y = D.lshift(8, D.flatten(7, D.projectPath('c', D.projectPath('a', D.empty))))

      D.autojoinKeys(x, y) must_= JoinKeys.empty
    }

    "join with union disjoins keys for each side" >> {
      val x = D.lshift(2, D.projectPath('a', D.empty))
      val y = D.lshift(3, D.projectPath('a', D.empty))
      val z = D.lshift(4, D.projectPath('a', D.empty))

      val jks = JoinKeys(IList(
        NonEmptyList(NonEmptyList(JoinKey(2, 4))),
        NonEmptyList(NonEmptyList(JoinKey(3, 4)))))

      D.autojoinKeys(D.union(x, y), z) must_= jks
    }

  }
}
