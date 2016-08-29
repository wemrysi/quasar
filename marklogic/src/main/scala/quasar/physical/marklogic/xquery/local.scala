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

package quasar.physical.marklogic.xquery

import quasar.Predef._

import java.lang.SuppressWarnings

/** Functions local to Quasar, will likely need to break this object up once we
 *  see classes of functions emerge.
 */
@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
object local {
  import syntax._

  val nsUri: String =
    "http://quasar-analytics.org/quasar"

  val qErrorQName: XQuery =
    fn.QName(nsUri.xs, "quasar:ERROR".xs)

  def qError(desc: XQuery, errObj: Option[XQuery] = None): XQuery =
    fn.error(qErrorQName, Some(desc), errObj)
}
