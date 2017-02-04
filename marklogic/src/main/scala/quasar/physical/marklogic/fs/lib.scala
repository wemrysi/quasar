/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.physical.marklogic.fs

import quasar.Predef._
import quasar.contrib.pathy._
import quasar.physical.marklogic.qscript.StructuralPlanner
import quasar.physical.marklogic.xml.namespaces._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._

import eu.timepit.refined.auto._
import scalaz._, Scalaz._

object lib {
  import expr.{emptySeq, for_, func, if_, let_}, axes.child
  import FunctionDecl._

  val fs = NamespaceDecl(filesystemNs)

  /** Appends the given nodes as children of `dst` in a format-appropriate manner. */
  def appendChildNodes[F[_]: PrologW: Monad, T](implicit SP: StructuralPlanner[F, T]): F[FunctionDecl2] =
    fs.declare[F]("append-child-nodes") flatMap (_(
      $("dst")   as ST("node()"),
      $("nodes") as ST("node()*")
    ).as(ST.Top) { (dst: XQuery, nodes: XQuery) =>
      val (dstType, node, key, base, i) = ($("dstType"), $("node"), $("key"), $("base"), $("i"))
      val unsupportedRoot = "Unsupported root node, expected an array or object, found: "

      (
        SP.nodeType(dst)                                                 |@|
        SP.singletonArray(~node)                                         |@|
        SP.singletonObject(~key, ~node)                                  |@|
        lpadToLength[F].apply("0".xs, 12.xqy, xdmp.random(baseSeed.xqy)) |@|
        lpadToLength[F].apply("0".xs, 8.xqy, ~i)
      )((typ, arr, obj, pbase, pidx) =>
        let_(dstType := typ) return_ {
          if_(~dstType eq "array".xs)
          .then_(
            for_(node in nodes)
            .return_(xdmp.nodeInsertChild(dst, arr `/` child.node())))
          .else_(if_(~dstType eq "object".xs)
          .then_(
            let_(base := pbase)
            .for_(node at i in nodes)
            .let_(key := fn.concat("k".xs, ~base, "-".xs, pidx))
            .return_(xdmp.nodeInsertChild(dst, obj `/` child.node())))
          .else_(fn.error(filesystemError.xqy, some(fn.concat(unsupportedRoot.xs, ~dstType)))))
        })
    })

  def copyDirectory[F[_]: PrologW: Functor]: F[FunctionDecl2] =
    fs.declare[F]("copy-directory") map (_(
      $("src") as ST("xs:string"),
      $("dst") as ST("xs:string")
    ).as(ST("xs:string*")) { (src: XQuery, dst: XQuery) =>
      val (srcDoc, dstDoc) = ($("srcDoc"), $("dstDoc"))

      fn.map(func(srcDoc.render) {
        let_(
          dstDoc := fn.concat(dst, fn.substringAfter(~srcDoc, src)),
          $("_") := if_(fn.docAvailable(~srcDoc))
                    .then_(xdmp.documentInsert(~dstDoc, fn.doc(~srcDoc)))
                    .else_(emptySeq))
        .return_(~dstDoc)
      }, cts.uriMatch(fn.concat(src, "*".xs), IList()))
    })

  // TODO: Is it a performance improvement to use cts:directory-query if `uri`
  //       is an ML directory?
  def directoryContents[F[_]: PrologW: Functor]: F[FunctionDecl1] =
    fs.declare[F]("directory-contents") map (_(
      $("uri") as ST("xs:string")
    ).as(ST("xs:string*")) { uri: XQuery =>
      val (descendant, suffix, segment) = ($("descendant"), $("suffix"), $("segment"))

      fn.distinctValues(fn.map(func(descendant.render) {
        let_(
          suffix  := fn.substringAfter(~descendant, uri),
          segment := fn.substringBefore(~suffix, "/".xs))
        .return_(
          if_(~descendant eq uri)
          .then_(emptySeq)
          .else_(if_(fn.stringLength(~segment) eq 0.xqy)
          .then_(fn.concat(uri, ~suffix))
          .else_(fn.concat(uri, ~segment, "/".xs))))
      }, cts.uriMatch(fn.concat(uri, "*".xs), IList())))
    })

  def lpadToLength[F[_]: PrologW: Functor]: F[FunctionDecl3] =
    fs.declare[F]("lpad-to-length") map (_(
      $("padchar") as ST("xs:string"),
      $("length")  as ST("xs:integer"),
      $("str")     as ST("xs:string")
    ).as(ST("xs:string")) { (padchar: XQuery, length: XQuery, str: XQuery) =>
      val (slen, padct, prefix) = ($("slen"), $("padct"), $("prefix"))
      let_(
        slen   := fn.stringLength(str),
        padct  := fn.max(mkSeq_("0".xqy, length - (~slen))),
        prefix := fn.stringJoin(for_($("_") in (1.xqy to ~padct)) return_ padchar, "".xs))
      .return_(
        fn.concat(~prefix, str))
    })

  def rootNode(file: AFile): XQuery =
    fn.doc(pathUri(file).xs) `/` child.node()

  ////

  // 0xffffffffffff
  private val baseSeed: Long = 281474976710655L
}
