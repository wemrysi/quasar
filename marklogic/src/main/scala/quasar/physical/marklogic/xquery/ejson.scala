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

import scalaz._, Id.Id
import scalaz.syntax.foldable._

// TODO: Optimize using XQuery seq as much as possible.
object ejson {
  import syntax._, expr.for_

  val nsUri: String =
    "http://quasar-analytics.org/ejson"

  val arrayName   : String = "ejson:array"
  val arrayEltName: String = "ejson:array-element"
  val mapName     : String = "ejson:map"
  val mapEntryName: String = "ejson:map-entry"
  val mapKeyName  : String = "ejson:map-key"
  val mapValueName: String = "ejson:map-value"

  val arrayQName   : XQuery = ejsonQName(arrayName)
  val arrayEltQName: XQuery = ejsonQName(arrayEltName)
  val mapQName     : XQuery = ejsonQName(mapName)
  val mapEntryQName: XQuery = ejsonQName(mapEntryName)
  val mapKeyQName  : XQuery = ejsonQName(mapKeyName)
  val mapValueQName: XQuery = ejsonQName(mapValueName)

  def arrayConcat(arr1: XQuery, arr2: XQuery): XQuery =
    XQuery(xmlElement(arrayName, s"{${mkSeq_(arr1 `/` arrayEltName.xs, arr2 `/` arrayEltName.xs)}}"))

  def arrayLeftShift(arr: XQuery): XQuery =
    arr `/` arrayEltName.xs `/` "child::node()".xs

  def isArray(item: XQuery): XQuery =
    fn.nodeName(item) === arrayQName

  def isMap(item: XQuery): XQuery =
    fn.nodeName(item) === mapQName

  def mkArray[F[_]: Foldable](items: F[XQuery]): XQuery = {
    def mkArrayElt(item: XQuery): XQuery =
      XQuery(xmlElement(arrayEltName, s"{$item}"))

    XQuery(xmlElement(arrayName, items.toList.map(mkArrayElt).mkString))
  }

  def mkMap(entries: XQuery): XQuery =
    XQuery(xmlElement(mapName, s"{$entries}"))

  def mkMapEntry(key: XQuery, value: XQuery): XQuery =
    XQuery(xmlElement(mapEntryName,
      xmlElement(mapKeyName, s"{$key}") +
      xmlElement(mapValueName, s"{$value}")))

  def singletonArray(item: XQuery): XQuery =
    mkArray[Id](item)

  def singletonMap(key: XQuery, value: XQuery): XQuery =
    mkMap(mkMapEntry(key, value))

  def zipMapKeys(emap: XQuery): XQuery =
    mkMap(
      for_("$e" -> emap `/` mapEntryName.xs)
      .let_(
        "$k" -> "$e".xqy `/` mapKeyName.xs `/` "child::node()".xs,
        "$v" -> "$e".xqy `/` mapValueName.xs `/` "child:node()".xs)
      .return_(mkMapEntry("$k".xqy, mkArray(IList("$k".xqy, "$v".xqy)))))

  ////

  private def ejsonQName(name: String): XQuery =
    fn.QName(nsUri.xs, name.xs)
}
