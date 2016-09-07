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

import java.lang.SuppressWarnings

import eu.timepit.refined.auto._
import scalaz.syntax.monad._

/** Functions local to Quasar, will likely need to break this object up once we
 *  see classes of functions emerge.
 */
@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
object local {
  import syntax._, expr.{element, for_, if_, let_}, axes._

  val qsr = namespace("quasar", "http://quasar-analytics.org/quasar")

  val dataN = qsr name "data"
  val errorN = qsr name "error"

  def isDocumentNode(node: XQuery): XQuery =
    xdmp.nodeKind(node) === "document".xs

  def leftShiftNode(node: XQuery): XQuery =
    node `/` child.node() `/` child.node()

  // TODO: Convert to a typeswitch
  def leftShift[F[_]: NameGenerator: PrologW](item: XQuery): F[XQuery] =
    for {
      x     <- freshVar[F]
      isArr <- ejson.isArray[F](x.xqy)
      arrLs <- ejson.arrayLeftShift[F](x.xqy)
      isMap <- ejson.isMap[F](x.xqy)
      mapLs <- ejson.mapLeftShift[F](x.xqy)
    } yield {
      let_(x -> item) return_ {
        if_(isArr)
        .then_ { arrLs }
        .else_ {
          if_ (isMap)
          .then_ { mapLs }
          .else_ { leftShiftNode(x.xqy) }
        }
      }
    }

  def mkData[F[_]: PrologW](children: XQuery): F[XQuery] =
    dataN.xs[F] map (data => element { data } { children })

  def qError[F[_]: PrologW](desc: XQuery, errObj: Option[XQuery] = None): F[XQuery] =
    errorN.xqy[F] map (err => fn.error(err, Some(desc), errObj))

  def zipMapNodeKeys[F[_]: NameGenerator: PrologW](node: XQuery): F[XQuery] =
    for {
      c       <- freshVar[F]
      n       <- freshVar[F]
      kelt    <- ejson.mkArrayElt[F](n.xqy)
      velt    <- ejson.mkArrayElt[F](c.xqy `/` child.node())
      kvArr   <- ejson.mkArray[F](mkSeq_(kelt, velt))
      kvEnt   <- ejson.mkMapEntry[F](n.xqy, kvArr)
      entries =  for_(c -> node `/` child.node())
                 .let_(n -> fn.nodeName(c.xqy))
                 .return_(kvEnt)
      zMap    <- ejson.mkMap[F](entries)
    } yield zMap
}
