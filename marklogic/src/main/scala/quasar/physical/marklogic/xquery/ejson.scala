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

import eu.timepit.refined.auto._
import scalaz.syntax.monad._

object ejson {
  import syntax._, expr.{element, for_, func, let_}, axes._

  val ejs = namespace("ejson", "http://quasar-analytics.org/ejson")

  val arrayN    = ejs name "array"
  val arrayEltN = ejs name "array-element"
  val mapN      = ejs name "map"
  val mapEntryN = ejs name "map-entry"
  val mapKeyN   = ejs name "map-key"
  val mapValueN = ejs name "map-value"

  def arrayConcat[F[_]: NameGenerator: PrologW](arr1: XQuery, arr2: XQuery): F[XQuery] =
    (freshVar[F] |@| freshVar[F] |@| arrayN.qn) { (x, y, an) =>
      mkArray[F](mkSeq_(x.xqy `/` child(an), y.xqy `/` child(an)))
        .map(arrs => let_(x -> arr1, y -> arr2) return_ arrs)
    }.join

  def arrayLeftShift[F[_]: PrologW](arr: XQuery): F[XQuery] =
    arrayN.qn[F] map (aname => arr `/` child(aname) `/` child.node())

  def isArray[F[_]: PrologW](item: XQuery): F[XQuery] =
    arrayN.xqy[F] map (fn.nodeName(item) === _)

  def isMap[F[_]: PrologW](item: XQuery): F[XQuery] =
    mapN.xqy[F] map (fn.nodeName(item) === _)

  def mapLeftShift[F[_]: PrologW](map: XQuery): F[XQuery] =
    (mapEntryN.qn[F] |@| mapValueN.qn[F])((mentry, mval) =>
      map `/` child(mentry) `/` child(mval) `/` child.node())

  def mapLookup[F[_]: NameGenerator: PrologW](map: XQuery, key: XQuery): F[XQuery] =
    (freshVar[F] |@| freshVar[F] |@| mapEntryN.qn |@| mapKeyN.xqy |@| mapValueN.qn) {
      (m, k, mentry, mkey, mval) =>

      let_(m -> map, k -> key) return_ {
        (m.xqy `/` child(mentry))(mkey === k.xqy) `/` child(mval) `/` child.node()
      }
    }

  def mkArray[F[_]: PrologW](elements: XQuery): F[XQuery] =
    arrayN.xs[F] map (aname => element { aname } { elements })

  def mkArrayElt[F[_]: PrologW](item: XQuery): F[XQuery] =
    arrayEltN.xs[F] map (aelt => element { aelt } { item })

  def mkMap[F[_]: PrologW](entries: XQuery): F[XQuery] =
    mapN.xs[F] map (mname => element { mname } { entries })

  def mkMapEntry[F[_]: PrologW](key: XQuery, value: XQuery): F[XQuery] =
    (mapEntryN.xs[F] |@| mapKeyN.xs[F] |@| mapValueN.xs[F])((mentry, mkey, mval) =>
      element { mentry } {
        mkSeq_(
          element { mkey } { key },
          element { mval } { value }
        )
      })

  def nodeSeqToArray[F[_]: NameGenerator: PrologW](seq: XQuery): F[XQuery] =
    for {
      x      <- freshVar[F]
      arrElt <- mkArrayElt[F](x.xqy)
      arr    <- mkArray[F](fn.map(func(x) { arrElt }, seq))
    } yield arr

  def singletonArray[F[_]: PrologW](item: XQuery): F[XQuery] =
    mkArrayElt[F](item) flatMap mkArray[F]

  def singletonMap[F[_]: PrologW](key: XQuery, value: XQuery): F[XQuery] =
    mkMapEntry[F](key, value) flatMap mkMap[F]

  def zipMapKeys[F[_]: NameGenerator: PrologW](emap: XQuery): F[XQuery] =
    (freshVar[F] |@| freshVar[F] |@| freshVar[F] |@| mapEntryN.qn |@| mapKeyN.qn |@| mapValueN.qn) {
      (e, k, v, mentry, mkey, mval) =>

      for {
        kelt <- mkArrayElt[F](k.xqy)
        velt <- mkArrayElt[F](v.xqy)
        arr  <- mkArray[F](mkSeq_(kelt, velt))
        ment <- mkMapEntry[F](k.xqy, arr)
        ents =  for_(
                  e -> emap `/` child(mentry))
                .let_(
                  k -> e.xqy `/` child(mkey) `/` child.node(),
                  v -> e.xqy `/` child(mval) `/` child.node())
                .return_(ment)
        zmap <- mkMap[F](ents)
      } yield zmap
    }.join
}
