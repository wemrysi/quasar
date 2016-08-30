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

package ygg.table

import ygg.common._
import scalaz.Scalaz._

final case class ColumnRef(selector: CPath, ctype: CType)

object ColumnRef {
  object id {
    def apply(ctype: CType): ColumnRef = ColumnRef(CPath.Identity, ctype)
    def unapply(x: ColumnRef): Option[CType] = x match {
      case ColumnRef(CPath.Identity, tp) => Some(tp)
      case _                             => None
    }
  }

  implicit val columnRefOrder: Ord[ColumnRef] = Ord.orderBy(r => r.selector -> r.ctype)
}
