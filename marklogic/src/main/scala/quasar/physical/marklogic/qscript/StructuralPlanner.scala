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

package quasar.physical.marklogic.qscript

import quasar.Predef._
import quasar.fp.ski.κ
import quasar.physical.marklogic.fmt
import quasar.physical.marklogic.xml.namespaces._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.expr._
import quasar.physical.marklogic.xquery.syntax._

import eu.timepit.refined.auto._
import scalaz._, Scalaz._

/** Planner for QScript structural operations.
  *
  * @tparam F   the effects employed by the library.
  * @tparam FMT type index representing the data format supported by the library.
  */
trait StructuralPlanner[F[_], FMT] {
  import FunctionDecl._
  import StructuralPlanner.ejs

  /** The representation of EJson `Null`. */
  def null_ : F[XQuery]

  /** Returns the result of appending the value to the array. */
  def arrayAppend(array: XQuery, value: XQuery): F[XQuery]

  /** Returns the concatenation of the given arrays. */
  def arrayConcat(a1: XQuery, a2: XQuery): F[XQuery]

  /** Returns the value at the (zero-based) index in the array or the empty seq if none. */
  def arrayElementAt(array: XQuery, index: XQuery): F[XQuery]

  /** Returns whether the given `item()` represents an EJson array. */
  def isArray(item: XQuery): F[XQuery]

  /** Returns the inner array elements or object entries of the given `node()`
    * or the empty seq if it is neither an array or object.
    */
  def leftShift(node: XQuery): F[XQuery]

  /** Returns a representation of an EJson array given a sequence of array
    * elements obtained via `mkArrayElt`.
    */
  def mkArray(elements: XQuery): F[XQuery]

  /** Returns a representation of an array element given an, possibly empty,
    * `item()`.
    */
  def mkArrayElt(item: XQuery): F[XQuery]

  /** Returns a representation of an EJson object given a sequence of object
    * entries obtained via `mkObjectEntry`.
    */
  def mkObject(entries: XQuery): F[XQuery]

  /** Returns a representation of an EJson object entry given a key name and,
    * possibly empty, value `item()`.
    */
  def mkObjectEntry(key: XQuery, value: XQuery): F[XQuery]

  /** Returns the given `node()` casted to a more precise XQuery type, if
    * possible, or the node itself otherwise.
    */
  def nodeCast(node: XQuery): F[XQuery]

  /** Returns any metadata associated with the given `node()` or the empty seq
    * if none.
    */
  def nodeMetadata(node: XQuery): F[XQuery]

  /** Returns the name of the given `node()`'s type as an `xs:string` or the
    * empty seq if unknown.
    */
  def nodeType(node: XQuery): F[XQuery]

  /** Returns an updated version of the given EJson object where the given key
    * is not associated with a value.
    */
  def objectDelete(obj: XQuery, key: XQuery): F[XQuery]

  /** Returns an updated version of the given EJson object where the given value
    * is associated with the given key.
    */
  def objectInsert(obj: XQuery, key: XQuery, value: XQuery): F[XQuery]

  /** Returns the value associated with the given key in the given EJson object
    * or the empty seq if none.
    */
  def objectLookup(obj: XQuery, key: XQuery): F[XQuery]

  /** Returns the right-biased merge of the given EJson objects. */
  def objectMerge(o1: XQuery, o2: XQuery): F[XQuery]

  //// Derived expressions. ////

  /** Returns an array consisting of the given value. */
  def singletonArray(value: XQuery)(implicit F: Monad[F]): F[XQuery] =
    mkArrayElt(value) >>= mkArray

  /** Returns an object with the given value associated with the given key. */
  def singletonObject(key: XQuery, value: XQuery)(implicit F: Monad[F]): F[XQuery] =
    mkObjectEntry(key, value) >>= mkObject

  /** Converts a sequence of items into an array. */
  def seqToArray(seq: XQuery)(implicit F0: Bind[F], F1: PrologW[F]): F[XQuery] =
    seqToArrayFn.apply(seq)

  /** Returns the name of the type of the given item or the empty seq if unknown. */
  def typeOf(item: XQuery)(implicit F0: Bind[F], F1: PrologW[F]): F[XQuery] =
    typeOfFn.apply(item)

  /** Attempts to cast the given `item()` to a more specific type if it is a `node()`. */
  def castIfNode(item: XQuery)(implicit F0: Bind[F], F1: PrologW[F]): F[XQuery] =
    castIfNodeFn.apply(item)

  ////

  // ejson:seq-to-array($items as item()*) as node()
  private def seqToArrayFn(implicit F0: Bind[F], F1: PrologW[F]): F[FunctionDecl1] =
    ejs.declare[F]("seq-to-array") flatMap (_(
      $("items") as ST("item()*")
    ).as(ST(s"node()")) { items: XQuery =>
      val x = $("x")
      for {
        arrElt <- mkArrayElt(~x)
        arr    <- mkArray(fn.map(func(x.render) { arrElt }, items))
      } yield arr
    })

  // ejson:type-of($item as item()*) as xs:string?
  private def typeOfFn(implicit F0: Bind[F], F1: PrologW[F]): F[FunctionDecl1] =
    ejs.declare[F]("type-of") flatMap (_(
      $("item") as ST.Top
    ).as(ST("xs:string?")) { item: XQuery =>
      val node = $("node")
      nodeType(~node) map { nType =>
        if_(fn.empty(item))
        .then_ { "na".xs }
        .else_ {
          typeswitch(item)(
            node as ST("node()")  return_ κ(nType),
            ST("xs:boolean")      return_ "boolean".xs,
            ST("xs:dateTime")     return_ "timestamp".xs,
            ST("xs:date")         return_ "date".xs,
            ST("xs:time")         return_ "time".xs,
            ST("xs:integer")      return_ "integer".xs,
            ST("xs:decimal")      return_ "decimal".xs,
            ST("xs:double")       return_ "decimal".xs,
            ST("xs:float")        return_ "decimal".xs,
            ST("xs:base64Binary") return_ "binary".xs,
            ST("xs:hexBinary")    return_ "binary".xs,
            ST("xs:QName")        return_ "string".xs,
            ST("xs:string")       return_ "string".xs
          ) default emptySeq
        }
      }
    })

  // ejson:cast-if-node($item as item()?) as item()?
  private def castIfNodeFn(implicit F0: Bind[F], F1: PrologW[F]): F[FunctionDecl1] =
    ejs.declare[F]("cast-if-node") flatMap (_(
      $("item") as ST("item()?")
    ).as(ST("item()?")) { item: XQuery =>
      val n = $("n")
      nodeCast(~n) map { casted =>
        typeswitch(item)(
          n as ST("node()") return_ κ(casted)
        ) default item
      }
    })
}

object StructuralPlanner {
  val ejs = NamespaceDecl(ejsonNs)

  def apply[F[_], T](implicit L: StructuralPlanner[F, T]): StructuralPlanner[F, T] = L

  implicit def xmlStructuralPlanner[F[_]: Monad: MonadPlanErr: PrologW: QNameGenerator]: StructuralPlanner[F, fmt.XML] =
    new XmlStructuralPlanner[F]
}
