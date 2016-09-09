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
  import syntax._, expr.{element, for_, if_}, axes._

  val qsr = namespace("quasar", "http://quasar-analytics.org/quasar")

  val dataN = qsr name "data"
  val errorN = qsr name "error"

  def isDocumentNode(node: XQuery): XQuery =
    xdmp.nodeKind(node) === "document".xs

  def nodeLeftShift[F[_]: PrologW]: F[FunctionDecl.FunctionDecl1] =
    qsr.name("node-left-shift").qn[F] map { fname =>
      declare(fname)(
        $("node") as SequenceType("node()*")
      ).as(SequenceType("item()*")) { n =>
        n `/` child.node() `/` child.node()
      }
    }

  // TODO: Convert to a typeswitch
  def leftShift[F[_]: PrologW]: F[FunctionDecl.FunctionDecl1] =
    (qsr.name("left-shift").qn[F] |@| ejson.arrayLeftShift[F] |@| ejson.mapLeftShift[F] |@| nodeLeftShift[F]) {
      (fname, arrayLs, mapLs, nodeLs) =>

      declare(fname)(
        $("node") as SequenceType("node()")
      ).as(SequenceType("item()*")) { node: XQuery =>
        for {
          isArr       <- ejson.isArray[F](node)
          shiftedArr  <- arrayLs(node)
          isMap       <- ejson.isMap[F](node)
          shiftedMap  <- mapLs(node)
          shiftedNode <- nodeLs(node)
        } yield {
          if_(isArr)
          .then_ { shiftedArr }
          .else_ {
            if_ (isMap)
            .then_ { shiftedMap }
            .else_ { shiftedNode }
          }
        }
      }
    }.join

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
