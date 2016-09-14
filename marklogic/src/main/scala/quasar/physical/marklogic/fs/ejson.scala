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

package quasar.physical.marklogic.fs

import quasar.Predef._
import quasar.{ejson => ejs}
//import quasar.physical.marklogic.xquery.xml.QName

import scala.xml._

import jawn._
import matryoshka._
import scalaz.{Node => _, _}, Scalaz._

object ejson {
  def jsonParser[T[_[_]]: Corecursive, F[_]: Functor](implicit C: ejs.Common :<: F, E: ejs.Extension :<: F): SupportParser[T[F]] =
    new SupportParser[T[F]] {
      implicit val facade: Facade[T[F]] =
        new SimpleFacade[T[F]] {
          def jarray(arr: List[T[F]])         = C(ejs.Arr(arr)).embed
          // TODO: Should `ListMap` really be in the interface, or just used as impl?
          def jobject(obj: Map[String, T[F]]) = E(ejs.Map(obj.toList.map(_.leftMap(k => C(ejs.Str[T[F]](k)).embed)))).embed
          def jnull()                         = C(ejs.Null[T[F]]()).embed
          def jfalse()                        = C(ejs.Bool[T[F]](false)).embed
          def jtrue()                         = C(ejs.Bool[T[F]](true)).embed
          def jnum(n: String)                 = C(ejs.Dec[T[F]](BigDecimal(n))).embed
          def jint(n: String)                 = E(ejs.Int[T[F]](BigInt(n))).embed
          def jstring(s: String)              = C(ejs.Str[T[F]](s)).embed
        }
    }

  // TODO: Error type for when data keys aren't valid QNames
  // TODO: Reuse names from xquery.ejson
  // TODO: Should only need quasar:data for top-level objects, inner objects can use property names as element names
  // TODO: Do we need to declare the top-level quasar/ejson namespaces?
  /** Example
    *
    * {
    *   "foo": ["bar", ["baz", {"quux": 1}]],
    *   "bat": 3,
    *   "bag": {
    *     "one": 1,
    *     "two": 2
    *   }
    * }
    *
    * <quasar:data>
    *   <foo>
    *     <ejson:array>
    *       <ejson:array-element quasar:type>"bar"</ejson:array-element>
    *       <ejson:array-element>
    *         <ejson:array>
    *           <ejson:array-element>baz</ejson:array-element>
    *           <ejson:array-element quasar:type="object">
    *             <quux quasar:type="int">1</quux>
    *           </ejson:array-element>
    *         </ejson:array>
    *       </ejson:array-element>
    *     </ejson:array>
    *   </foo>
    * </quasar:data>
    *
    */
  val commonToXML: Algebra[ejs.Common, Node] = {
    case ejs.Arr(elements) => ???
    case ejs.Bool(b)       => ???
    case ejs.Dec(d)        => ???
    case ejs.Null()        => ???
    case ejs.Str(s)        => ???
  }

  val extensionToXML: AlgebraM[String \/ ?, ejs.Extension, Node] = {
    case ejs.Byte(b)           => ???
    case ejs.Char(c)           => ???
    case ejs.Int(i)            => ???
    case ejs.Map(entries)      => ???
    case ejs.Meta(value, meta) => ???
  }

  def xmlToEJson[F[_]](implicit C: ejs.Common :<: F, E: ejs.Extension :<: F): Coalgebra[F, Node] = {
    case _ => ???
  }
}
