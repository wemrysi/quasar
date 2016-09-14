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

package quasar.physical.marklogic

import quasar.Predef._
import quasar.Data

import scala.xml._

import scalaz.{Node => _, _}, Scalaz._

package object xml {

  final case class KeywordConfig(attributesKeyName: String, textKeyName: String)

  import Data._

  /** Example
    *
    * <foo type="baz" id="1">
    *   <bar>
    *     <baz>37</baz>
    *     <bat>one</bat>
    *     <bat>two</bat>
    *   </bar>
    *   <quux>lorem ipsum</quux>
    * </foo>
    *
    * {
    *   "foo": {
    *     "_attributes": {
    *       "type": "baz",
    *       "id": "1"
    *     },
    *     "bar": {
    *       "baz": "37",
    *       "bat": ["one", "two"]
    *     },
    *     "quux": "lorem ipsum"
    *   }
    * }
    */
  def toData(elem: Elem, config: KeywordConfig): Data = {

    def impl(nodes: List[Node], m: Option[MetaData]): Data = nodes match {
      case List(Text(str)) =>
        m.flatMap(attrToData).cata(
          m => Obj(ListMap(
            config.attributesKeyName  -> m,
            config.textKeyName        -> Str(str)
          )),
          Str(str)
        )
      case xs =>
        val elementChildren = xs.collect { case a: Elem => a}.groupBy(fullName)
        val childrenData = elementChildren.mapValues {
          case List(single) => impl(single.child.toList, single.attributes.some)
          case xs           => Arr(xs.map(x => impl(x.child.toList, x.attributes.some)))
        }.toList
        val attributeData = m.flatMap(attrToData).strengthL(config.attributesKeyName).toList
        Obj(ListMap((attributeData ++ childrenData): _*))
    }

    def attrToData(meta: MetaData): Option[Data] = meta match {
      case scala.xml.Null => None
      case m    =>
        Obj(ListMap(meta.iterator.map(m => m.key -> impl(m.value.toList, none)).toList: _*)).some
    }

    Obj(ListMap(fullName(elem) -> impl(elem.child.toList, elem.attributes.some)))
  }

  def toData(elem: Elem): Data = toData(elem, KeywordConfig(attributesKeyName = "_attributes", textKeyName = "_text"))

  private def fullName(elem: Elem): String =
    Option(elem.prefix).map(_ + ":").getOrElse("") + elem.label
}
