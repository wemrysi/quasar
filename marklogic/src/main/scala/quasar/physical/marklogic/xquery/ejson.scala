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
import quasar.NameGenerator

import scalaz._
import scalaz.syntax.apply._

object ejson {
  import syntax._, expr.{element, for_, func, let_}

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

  def arrayConcat[F[_]: NameGenerator: Apply](arr1: XQuery, arr2: XQuery): F[XQuery] =
    (freshVar[F] |@| freshVar[F])((x, y) =>
      mkSeq_(let_(
        x -> arr1,
        y -> arr2
      ) return_ {
        mkArray(mkSeq_(
          x.xqy `/` arrayEltName.xs,
          y.xqy `/` arrayEltName.xs))
      }))

  def arrayLeftShift(arr: XQuery): XQuery =
    mkSeq_(arr) `/` arrayEltName.xs `/` "child::node()".xs

  def isArray(item: XQuery): XQuery =
    fn.nodeName(item) === arrayQName

  def isMap(item: XQuery): XQuery =
    fn.nodeName(item) === mapQName

  def mapLeftShift(map: XQuery): XQuery =
    mkSeq_(map) `/` mapEntryName.xs `/` mapValueName.xs `/` "child::node()".xs

  def mapLookup[F[_]: NameGenerator: Apply](map: XQuery, key: XQuery): F[XQuery] =
    (freshVar[F] |@| freshVar[F])((m, k) =>
      let_(m -> map, k -> key) return_ {
        (m.xqy `/` mapEntryName.xs)(mapKeyName.xqy === k.xqy) `/` mapValueName.xs `/` "child::node()".xs
      })

  def mkArray(elements: XQuery): XQuery =
    element { arrayName.xs } { elements }

  def mkArrayElt(item: XQuery): XQuery =
    element { arrayEltName.xs } { item }

  def mkMap(entries: XQuery): XQuery =
    element { mapName.xs } { entries }

  def mkMapEntry(key: XQuery, value: XQuery): XQuery =
    element { mapEntryName.xs } {
      mkSeq_(
        element { mapKeyName.xs } { key },
        element { mapValueName.xs } { value }
      )
    }

  def nodeSeqToArray[F[_]: NameGenerator: Functor](seq: XQuery): F[XQuery] =
    freshVar[F] map (x =>
      mkArray(fn.map(func(x) { mkArrayElt(x.xqy) }, seq)))

  def singletonArray(item: XQuery): XQuery =
    mkArray(mkArrayElt(item))

  def singletonMap(key: XQuery, value: XQuery): XQuery =
    mkMap(mkMapEntry(key, value))

  def zipMapKeys[F[_]: NameGenerator: Apply](emap: XQuery): F[XQuery] =
    (freshVar[F] |@| freshVar[F] |@| freshVar[F])((e, k, v) =>
      mkMap(
        for_(
          e -> emap `/` mapEntryName.xs)
        .let_(
          k -> e.xqy `/` mapKeyName.xs `/` "child::node()".xs,
          v -> e.xqy `/` mapValueName.xs `/` "child::node()".xs)
        .return_(
          mkMapEntry(k.xqy, mkArray(mkSeq_(mkArrayElt(k.xqy), mkArrayElt(v.xqy)))))))

  ////

  private def ejsonQName(name: String): XQuery =
    fn.QName(nsUri.xs, name.xs)
}
