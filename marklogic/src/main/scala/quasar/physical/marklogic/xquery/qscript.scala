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
import quasar.fp.ski.ι
import quasar.physical.marklogic.xml.namespaces._

import java.lang.SuppressWarnings

import eu.timepit.refined.auto._
import scalaz.IList
import scalaz.syntax.monad._

/** Functions related to qscript planning. */
@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
object qscript {
  import syntax._, expr._, axes.{attribute, child}
  import FunctionDecl.{FunctionDecl1, FunctionDecl2, FunctionDecl5}

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

  // qscript:combine-n($combiners as (function(item()*, item()) as item()*)*) as function(item()*, item()) as item()*
  def combineN[F[_]: PrologW]: F[FunctionDecl1] =
    qs.name("combine-n").qn[F] map { fname =>
      declare(fname)(
        $("combiners") as SequenceType("(function(item()*, item()) as item()*)*")
      ).as(SequenceType("function(item()*, item()) as item()*")) { combiners: XQuery =>
        val (len, acc, i, x) = ("$len", "$acc", "$i", "$x")

        let_ (len -> fn.count(combiners)) return_ {
          func(acc, x) {
            for_ (i -> (1.xqy to len.xqy)) return_ {
              combiners(i.xqy) fnapply (acc.xqy(i.xqy), x.xqy)
            }
          }
        }
      }
    }

  // qscript:identity($x as item()*) as item()*
  def identity[F[_]: PrologW]: F[FunctionDecl1] =
    qs.name("identity").qn[F] map { fname =>
      declare(fname)($("x") as SequenceType.Top).as(SequenceType.Top)(ι)
    }

  // qscript:inc-avg($st as map:map, $x as item()*) as map:map
  def incAvg[F[_]: PrologW]: F[FunctionDecl2] =
    qs.name("inc-avg").qn[F] flatMap { fname =>
      declare(fname)(
        $("st") as SequenceType("map:map"),
        $("x")  as SequenceType.Top
      ).as(SequenceType("map:map")) { (st: XQuery, x: XQuery) =>
        val (c, a, y) = ("$c", "$a", "$y")
        incAvgState[F].apply(c.xqy, y.xqy) map { nextSt =>
          let_(
            c -> (map.get(st, "cnt".xqy) + 1.xqy),
            a -> map.get(st, "avg".xqy),
            y -> (a.xqy + mkSeq_(mkSeq_(x - a.xqy) div c.xqy)))
          .return_(nextSt)
        }
      }
    }

  // qscript:inc-avg-state($cntavg as map:map, $x as item()*) as map:map
  def incAvgState[F[_]: PrologW]: F[FunctionDecl2] =
    qs.name("inc-avg-state").qn[F] map { fname =>
      declare(fname)(
        $("cnt") as SequenceType("xs:integer"),
        $("avg") as SequenceType("xs:decimal")
      ).as(SequenceType("map:map")) { (cnt, avg) =>
        map.new_(IList(
          map.entry("cnt".xs, cnt),
          map.entry("avg".xs, avg)))
      }
    }

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

  // qscript:reduce-with(
  //   $initial  as function(item()*        ) as item()*,
  //   $combine  as function(item()*, item()) as item()*,
  //   $finalize as function(item()*        ) as item()*,
  //   $bucket   as function(item()*        ) as item(),
  //   $seq      as item()*
  // ) as item()*
  def reduceWith[F[_]: PrologW]: F[FunctionDecl5] =
    (qs.name("reduce-with").qn[F] |@| asMapKey[F]) { (fname, asMKey) =>
      declare(fname)(
        $("initial")  as SequenceType("function(item()*) as item()*"),
        $("combine")  as SequenceType("function(item()*, item()) as item()*"),
        $("finalize") as SequenceType("function(item()*) as item()*"),
        $("bucket")   as SequenceType("function(item()*) as item()"),
        $("seq")      as SequenceType("item()*")
      ).as(SequenceType("item()*")) { (initial: XQuery, combine: XQuery, finalize: XQuery, bucket: XQuery, xs: XQuery) =>
        val (m, x, k, v, o) = ("$m", "$x", "$k", "$v", "$_")

        asMKey(bucket fnapply (x.xqy)) map { theKey =>
          let_(
            m -> map.map(),
            o -> for_ (x -> xs) .let_ (
                   k -> theKey,
                   v -> if_(map.contains(m.xqy, k.xqy))
                        .then_(combine fnapply (map.get(m.xqy, k.xqy), x.xqy))
                        .else_(initial fnapply (x.xqy)),
                   o -> map.put(m.xqy, k.xqy, v.xqy)
                 ) .return_ (emptySeq)
          ) .return_ {
            for_ (k -> map.keys(m.xqy)) .return_ {
              finalize fnapply (map.get(m.xqy, k.xqy))
            }
          }
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

  // qscript:zip-apply($fns as (function(item()*) as item()*)*) as function(item()*) as item()*
  // TODO: This and combine-n are similar, DRY them up if we go this route
  def zipApply[F[_]: PrologW]: F[FunctionDecl1] =
    qs.name("zip-apply").qn[F] map { fname =>
      declare(fname)(
        $("fns") as SequenceType("(function(item()*) as item()*)*")
      ).as(SequenceType("function(item()*) as item()*")) { fns: XQuery =>
        val (len, i, x) = ("$len", "$i", "$x")

        let_ (len -> fn.count(fns)) return_ {
          func(x) {
            for_ (i -> (1.xqy to len.xqy)) return_ {
              fns(i.xqy) fnapply (x.xqy(i.xqy))
            }
          }
        }
      }
    }

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
