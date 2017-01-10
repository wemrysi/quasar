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

package quasar.physical.marklogic.qscript

import quasar.physical.marklogic.fmt
import quasar.physical.marklogic.xquery.XQuery
import quasar.physical.marklogic.xquery.syntax._

import simulacrum.typeclass
import scalaz.IList

/** `cts:search` options for the given type. */
@typeclass
trait SearchOptions[A] {
  def searchOptions: IList[XQuery]
}

object SearchOptions {
  implicit val xmlOptions: SearchOptions[fmt.XML] =
    new SearchOptions[fmt.XML] {
      val searchOptions = IList("format-xml".xs)
    }

  implicit val jsonOptions: SearchOptions[fmt.JSON] =
    new SearchOptions[fmt.JSON] {
      val searchOptions = IList("format-json".xs)
    }
}
