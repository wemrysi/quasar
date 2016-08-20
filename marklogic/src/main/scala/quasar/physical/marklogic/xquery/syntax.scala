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

object syntax {
  // TODO: Make XQuery a newtype and add these to that class.
  final implicit class XQueryOps(val xqy: XQuery) extends scala.AnyVal {
    def ?(predicate: String): XQuery = s"$xqy[$predicate]"
    def -(other: XQuery): XQuery = s"$xqy - $other"
    def +(other: XQuery): XQuery = s"$xqy + $other"
    def and(other: XQuery): XQuery = expr.and(xqy, other)
    def or(other: XQuery): XQuery = expr.or(xqy, other)
    def seq: XQuery = mkSeq_(xqy)
    def xp(xpath: String): XQuery = xqy + xpath
  }

  final implicit class XQueryStringOps(val str: String) extends scala.AnyVal {
    def xs: XQuery = expr.string(str)
  }
}
