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
import quasar.physical.marklogic.xml.namespaces._

import java.lang.SuppressWarnings

import eu.timepit.refined.auto._
import scalaz.syntax.monad._

/** Functions related to qscript planning. */
@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
object qscript {
  import syntax._, expr._, axes.{attribute, child}
  import FunctionDecl.{FunctionDecl1, FunctionDecl2, FunctionDecl4}

  val qs     = NamespaceDecl(qscriptNs)
  val errorN = qs name qscriptError.local

  // qscript:as-map-key($item as item()) as xs:string
  def asMapKey[F[_]: PrologW]: F[FunctionDecl1] =
    qs.name("as-map-key").qn[F] map { fname =>
      declare(fname)(
        $("item") as SequenceType("item()")
      ).as(SequenceType("xs:string")) { item =>
        typeswitch(item)(
          ($("a") as SequenceType("attribute()")) return_ (a =>
            fn.stringJoin(mkSeq_(fn.string(fn.nodeName(a)), fn.string(a)), "_".xs)),

          ($("e") as SequenceType("element()"))   return_ (e =>
            fn.stringJoin(mkSeq_(
              fn.string(fn.nodeName(e)),
              fn.map(fname :# 1, mkSeq_(e `/` attribute.node(), e `/` child.node()))
            ), "_".xs))

        ) default ($("i"), fn.string)
      }
    }

  // qscript:grouped($bucket as function(item()) as item(), $seq as item()*) as map:map
  def grouped[F[_]: PrologW]: F[FunctionDecl2] =
    (qs.name("grouped").qn[F] |@| asMapKey[F]) { (fname, asMKey) =>
      declare(fname)(
        $("bucket") as SequenceType("function(item()) as item()"),
        $("seq") as SequenceType("item()*")
      ).as(SequenceType("map:map")) { (bucket: XQuery, xs: XQuery) =>
        val (m, x, k, o) = ("$m", "$x", "$k", "$_")

        asMKey(bucket fnapply (x.xqy)) map { theKey =>
          let_(
            m -> map.map(),
            o -> for_ (x -> xs) .let_ (
                   k -> theKey,
                   o -> map.put(m.xqy, k.xqy, mkSeq_(map.get(m.xqy, k.xqy), x.xqy))
                 ) .return_ (emptySeq)
          ) .return_ (m.xqy)
        }
      }
    }.join

  def isDocumentNode(node: XQuery): XQuery =
    xdmp.nodeKind(node) === "document".xs

  // qscript:node-left-shift($node as node()*) as item()*
  def nodeLeftShift[F[_]: PrologW]: F[FunctionDecl1] =
    qs.name("node-left-shift").qn[F] map { fname =>
      declare(fname)(
        $("node") as SequenceType("node()*")
      ).as(SequenceType("item()*")) { n =>
        n `/` child.node() `/` child.node()
      }
    }

  def qError[F[_]: PrologW](desc: XQuery, errObj: Option[XQuery] = None): F[XQuery] =
    errorN.xqy[F] map (err => fn.error(err, Some(desc), errObj))

  // qscript:reduce(
  //   $src as item()*,
  //   $bucket as function(item()) as item(),
  //   $reductions as (function(item()*) as item())*,
  //   $repair as function(item()) as item()*
  // ) as item()*
  def reduce[F[_]: PrologW]: F[FunctionDecl4] =
    (qs.name("reduce").qn[F] |@| grouped[F]) { (fname, grpd) =>
      declare(fname)(
        $("src")        as SequenceType("item()*"),
        $("bucket")     as SequenceType("function(item()) as item()"),
        $("reductions") as SequenceType("(function(item()*) as item())*"),
        $("repair")     as SequenceType("function(item()*) as item()*")
      ).as(SequenceType("item()*")) { (src: XQuery, bucket: XQuery, reductions: XQuery, repair: XQuery) =>
        grpd(bucket, src) map { m =>
          val (key, bckt, reduced, res, f) = ("$key", "$bckt", "$reduced", "$res", "$f")
          for_(
            key -> map.keys(m))
          .let_(
            bckt    -> map.get(m, key.xqy),
            reduced -> fn.map(func(f) { f.xqy fnapply (bckt.xqy) }, reductions),
            res     -> repair.fnapply(reduced.xqy))
          .return_(res.xqy)
        }
      }
    }.join

  // qscript:shifted-read($uri as xs:string, $include-id as xs:boolean) as element()*
  def shiftedRead[F[_]: NameGenerator: PrologW]: F[FunctionDecl2] =
    qs.name("shifted-read").qn[F].map { fname =>
      declare(fname)(
        $("uri") as SequenceType("xs:string"),
        $("include-id") as SequenceType("xs:boolean")
      ).as(SequenceType(s"element()*")) { (uri: XQuery, includeId: XQuery) =>
        for {
          d     <- freshVar[F]
          c     <- freshVar[F]
          b     <- freshVar[F]
          xform <- json.transformFromJson[F](c.xqy)
          mkArr <- ejson.seqToArray[F].getApply
        } yield
          for_(d -> cts.search(fn.doc(), cts.directoryQuery(uri, "1".xs)))
            .let_(
              c -> d.xqy `/` child.node(),
              b -> (if_ (json.isObject(c.xqy)) then_ xform else_ c.xqy))
            .return_ {
              if_ (includeId)
              .then_ { mkArr(mkSeq_(fn.concat("_".xs, xdmp.hmacSha1("quasar".xs, fn.documentUri(d.xqy))), b.xqy)) }
              .else_ { b.xqy }
            }
      }
    }.join

  // qscript:zip-map-node-keys($node as node()) as element(ejson:ejson)
  def zipMapNodeKeys[F[_]: NameGenerator: PrologW]: F[FunctionDecl1] =
    (qs.name("zip-map-node-keys").qn[F] |@| ejson.ejsonN.qn) { (fname, ename) =>
      declare(fname)(
        $("node") as SequenceType("node()")
      ).as(SequenceType(s"element($ename)")) { (node: XQuery) =>
        val c = "$child"
        val n = "$name"

        for {
          kelt    <- ejson.mkArrayElt[F] apply n.xqy
          velt    <- ejson.mkArrayElt[F] apply (c.xqy `/` child.node())
          kvArr   <- ejson.mkArray[F] apply mkSeq_(kelt, velt)
          kvEnt   <- ejson.mkObjectEntry[F] apply (n.xqy, kvArr)
          entries =  for_(c -> node `/` child.node())
                     .let_(n -> fn.nodeName(c.xqy))
                     .return_(kvEnt)
          zMap    <- ejson.mkObject[F] apply entries
        } yield zMap
      }
    }.join
}
