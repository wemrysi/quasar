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

  private val epoch = xs.dateTime("1970-01-01T00:00:00Z".xs)

  // qscript:as-date($item as item()) as xs:date?
  def asDate[F[_]: PrologW]: F[FunctionDecl1] =
    qs.name("as-date").qn[F] map { fname =>
      declare(fname)(
        $("item") as SequenceType("item()")
      ).as(SequenceType("xs:date?")) { item =>
        if_(isCastable(item, SequenceType("xs:date")))
        .then_ { xs.date(item) }
        .else_ {
          if_(isCastable(item, SequenceType("xs:dateTime")))
          .then_ { xs.date(xs.dateTime(item)) }
          .else_ { emptySeq }
        }
      }
    }

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

  // qscript:delete-field($src as element(), $field as xs:QName) as element()
  def deleteField[F[_]: PrologW]: F[FunctionDecl2] =
    qs.name("delete-field").qn[F] map { fname =>
      declare(fname)(
        $("src")   as SequenceType("element()"),
        $("field") as SequenceType("xs:QName")
      ).as(SequenceType("element()")) { (src: XQuery, field: XQuery) =>
        val n = "$n"
        element { fn.nodeName(src) } {
          for_    (n -> (src `/` child.element()))
          .where_ (fn.nodeName(n.xqy) ne field)
          .return_(n.xqy)
        }
      }
    }

  // qscript:element-dup-keys($elt as element()) as element()
  def elementDupKeys[F[_]: PrologW]: F[FunctionDecl1] =
    qs.name("element-dup-keys").qn[F] map { fname =>
      declare(fname)(
        $("elt") as SequenceType("element()")
      ).as(SequenceType("element()")) { elt: XQuery =>
        val (c, n) = ("$c", "$n")
        element { fn.nodeName(elt) } {
          for_    (c -> (elt `/` child.element()))
          .let_   (n -> fn.nodeName(c.xqy))
          .return_(element { n.xqy } { n.xqy })
        }
      }
    }

  // qscript:element-left-shift($elt as element()) as item()*
  def elementLeftShift[F[_]: PrologW]: F[FunctionDecl1] =
    (qs.name("element-left-shift").qn[F] |@| ejson.arrayEltN.qn |@| ejson.isArray) { (fname, aelt, isArr) =>
      declare(fname)(
        $("elt") as SequenceType("element()")
      ).as(SequenceType("item()*")) { elt: XQuery =>
        isArr(elt) map { eltIsArray =>
          if_ (eltIsArray)
          .then_ { elt `/` child(aelt)  }
          .else_ { elt `/` child.node() }
        }
      }
    }.join

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
            c -> (map.get(st, "cnt".xs) + 1.xqy),
            a -> map.get(st, "avg".xs),
            y -> (a.xqy + mkSeq_(mkSeq_(x - a.xqy) div c.xqy)))
          .return_(nextSt)
        }
      }
    }

  // qscript:inc-avg-state($cnt as xs:integer, $avg as xs:double) as map:map
  def incAvgState[F[_]: PrologW]: F[FunctionDecl2] =
    qs.name("inc-avg-state").qn[F] map { fname =>
      declare(fname)(
        $("cnt") as SequenceType("xs:integer"),
        $("avg") as SequenceType("xs:double")
      ).as(SequenceType("map:map")) { (cnt, avg) =>
        map.new_(IList(
          map.entry("cnt".xs, cnt),
          map.entry("avg".xs, avg)))
      }
    }

  def isDocumentNode(node: XQuery): XQuery =
    xdmp.nodeKind(node) === "document".xs

  def length[F[_]: PrologW]: F[FunctionDecl1] =
    qs.name("length").qn[F] map { fname =>
      declare(fname)(
        $("arrOrStr") as SequenceType("item()")
      ).as(SequenceType("xs:integer")) { arrOrStr: XQuery =>
        typeswitch(arrOrStr)(
          $("str") as SequenceType("xs:string") return_ (fn.stringLength(_)),
          $("arr") as SequenceType("element()") return_ (arr => fn.count(arr `/` child.node()))
        ) default 0.xqy
      }
    }

  // qscript:project-field($src as element(), $field as xs:QName) as item()*
  def projectField[F[_]: PrologW]: F[FunctionDecl2] =
    qs.name("project-field").qn[F] map { fname =>
      declare(fname)(
        $("src")   as SequenceType("element()"),
        $("field") as SequenceType("xs:QName")
      ).as(SequenceType.Top) { (src: XQuery, field: XQuery) =>
        fn.filter(func("$n")(fn.nodeName("$n".xqy) eq field), src `/` child.element())
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

  // qscript:seconds-since-epoch($dt as xs:dateTime) as xs:double
  def secondsSinceEpoch[F[_]: PrologW]: F[FunctionDecl1] =
    qs.name("seconds-since-epoch").qn[F] map { fname =>
      declare(fname)(
        $("dt") as SequenceType("xs:dateTime")
      ).as(SequenceType("xs:double")) { dt =>
        mkSeq_(dt - epoch) div xs.dayTimeDuration("PT1S".xs)
      }
    }

  // qscript:shifted-read($uri as xs:string, $include-id as xs:boolean) as element()*
  def shiftedRead[F[_]: NameGenerator: PrologW]: F[FunctionDecl2] =
    qs.name("shifted-read").qn[F] flatMap { fname =>
      declare(fname)(
        $("uri") as SequenceType("xs:string"),
        $("include-id") as SequenceType("xs:boolean")
      ).as(SequenceType(s"element()*")) { (uri: XQuery, includeId: XQuery) =>
        for {
          d     <- freshVar[F]
          c     <- freshVar[F]
          b     <- freshVar[F]
          xform <- json.transformFromJson[F](c.xqy)
          incId <- ejson.seqToArray_[F](mkSeq_(
                     fn.concat("_".xs, xdmp.hmacSha1("quasar".xs, fn.documentUri(d.xqy))),
                     b.xqy))
        } yield {
          for_(d -> cts.search(fn.doc(), cts.directoryQuery(uri, "1".xs)))
            .let_(
              c -> d.xqy `/` child.node(),
              b -> (if_ (json.isObject(c.xqy)) then_ xform else_ c.xqy))
            .return_ {
              if_ (includeId) then_ { incId } else_ { b.xqy }
            }
        }
      }
    }

  // qscript:timestamp-to-dateTime($millis as xs:integer) as xs:dateTime
  def timestampToDateTime[F[_]: PrologW]: F[FunctionDecl1] =
    qs.name("timestamp-to-dateTime").qn[F] map { fname =>
      declare(fname)(
        $("millis") as SequenceType("xs:integer")
      ).as(SequenceType("xs:dateTime")) { millis =>
        epoch + xs.dayTimeDuration(fn.concat("PT".xs, xs.string(millis div 1000.xqy), "S".xs))
      }
    }

  // qscript:timezone-offset-seconds($dt as xs:dateTime) as xs:integer
  def timezoneOffsetSeconds[F[_]: PrologW]: F[FunctionDecl1] =
    qs.name("timezone-offset-seconds").qn[F] map { fname =>
      declare(fname)(
        $("dt") as SequenceType("xs:dateTime")
      ).as(SequenceType("xs:integer")) { dt =>
        fn.timezoneFromDateTime(dt) div xs.dayTimeDuration("PT1S".xs)
      }
    }

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

  // qscript:zip-map-element-keys($elt as element()) as element()
  def zipMapElementKeys[F[_]: NameGenerator: PrologW]: F[FunctionDecl1] =
    qs.name("zip-map-element-keys").qn[F] flatMap { fname =>
      declare(fname)(
        $("elt") as SequenceType("element()")
      ).as(SequenceType(s"element()")) { (elt: XQuery) =>
        val c = "$child"
        val n = "$name"

        for {
          kelt    <- ejson.mkArrayElt[F](n.xqy)
          velt    <- ejson.mkArrayElt[F](c.xqy)
          kvArr   <- ejson.mkArray_[F](mkSeq_(kelt, velt))
          kvEnt   <- ejson.renameOrWrap[F] apply (n.xqy, kvArr)
          entries =  for_ (c -> elt `/` child.element())
                     .let_(n -> fn.nodeName(c.xqy))
                     .return_(kvEnt)
          zMap    <- ejson.mkObject[F] apply entries
        } yield zMap
      }
    }
}
