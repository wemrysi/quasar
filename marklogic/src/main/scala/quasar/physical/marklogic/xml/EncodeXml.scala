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

package quasar.physical.marklogic.xml

import quasar.Predef._
import quasar.{ejson => ejs}
import quasar.physical.marklogic.MonadErrMsg

import scala.xml.Node

import matryoshka._
import scalaz.{Node => _, _}
import scalaz.syntax.applicative._

trait EncodeXml[M[_], F[_]] {
  def encodeXml: AlgebraM[M, F, Node]
}

object EncodeXml extends EncodeXmlInstances {
  def apply[M[_], F[_]](implicit E: EncodeXml[M, F]): EncodeXml[M, F] = E
}

sealed abstract class EncodeXmlInstances {
  implicit def encodeCoproduct[M[_], F[_], G[_]](implicit F: EncodeXml[M, F], G: EncodeXml[M, G]): EncodeXml[M, Coproduct[F, G, ?]] =
    new EncodeXml[M, Coproduct[F, G, ?]] {
      val encodeXml: AlgebraM[M, Coproduct[F, G, ?], Node] =
        _.run.fold(F.encodeXml, G.encodeXml)
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
  implicit def encodeCommon[F[_]: Applicative]: EncodeXml[F, ejs.Common] =
    new EncodeXml[F, ejs.Common] {
      val encodeXmlAlg: Algebra[ejs.Common, Node] = {
        case ejs.Arr(elements) => ???
        case ejs.Bool(b)       => ???
        case ejs.Dec(d)        => ???
        case ejs.Null()        => ???
        case ejs.Str(s)        => ???
      }

      val encodeXml = encodeXmlAlg andThen (_.point[F])
    }

  implicit def encodeExtension[F[_]: MonadErrMsg]: EncodeXml[F, ejs.Extension] =
    new EncodeXml[F, ejs.Extension] {
      val encodeXml: AlgebraM[F, ejs.Extension, Node] = {
        case ejs.Byte(b)           => ???
        case ejs.Char(c)           => ???
        case ejs.Int(i)            => ???
        case ejs.Map(entries)      => ???
        case ejs.Meta(value, meta) => ???
      }
    }
}
