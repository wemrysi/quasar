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

package quasar.common

import quasar.Qspec

object CPathSpec extends Qspec {

  "CPath parser" >> {

    "parse single field" >> {
      CPath.parse(".aa") must_=(
        CPath(CPathField("aa")))
    }

    "parse multiple fields" >> {
      CPath.parse(".aa.bb.cc.dd") must_=(
        CPath(CPathField("aa"), CPathField("bb"), CPathField("cc"), CPathField("dd")))
    }

    "parse single index" >> {
      CPath.parse("[3]") must_=(
        CPath(CPathIndex(3)))
    }

    "parse multiple indices" >> {
      CPath.parse("[3][5][7][11]") must_=(
        CPath(CPathIndex(3), CPathIndex(5), CPathIndex(7), CPathIndex(11)))
    }

    "parse single array" >> {
      CPath.parse("[*]") must_=(
        CPath(CPathArray))
    }

    "parse multiple arrays" >> {
      CPath.parse("[*][*][*][*]") must_=(
        CPath(CPathArray, CPathArray, CPathArray, CPathArray))
    }

    "parse mixed path starting with field" >> {
      CPath.parse(".aa[3].bb[*][5]") must_=(
        CPath(CPathField("aa"), CPathIndex(3), CPathField("bb"), CPathArray, CPathIndex(5)))
    }

    "parse mixed path starting with index" >> {
      CPath.parse("[3].aa.bb[*][5]") must_=(
        CPath(CPathIndex(3), CPathField("aa"), CPathField("bb"), CPathArray, CPathIndex(5)))
    }

    "parse mixed path starting with array" >> {
      CPath.parse("[*].aa[3].bb[5]") must_=(
        CPath(CPathArray, CPathField("aa"), CPathIndex(3), CPathField("bb"), CPathIndex(5)))
    }
  }
}
