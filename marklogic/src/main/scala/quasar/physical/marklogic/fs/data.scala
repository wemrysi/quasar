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
import quasar.Data
import quasar.SKI.κ
import quasar.physical.marklogic.MonadErrMsg
import quasar.physical.marklogic.xml._
import quasar.physical.marklogic.xml.namespaces._

import scala.xml._

import eu.timepit.refined.auto._
import jawn._
import scalaz.{Node => _, _}, Scalaz._

object data {
  type EJsonType = String

  object JsonParser extends SupportParser[Data] {
    implicit val facade: Facade[Data] =
      new SimpleFacade[Data] {
        def jarray(arr: List[Data]) = Data.Arr(arr)
        // TODO: Should `ListMap` really be in the interface, or just used as impl?
        def jobject(obj: Map[String, Data]) = Data.Obj(ListMap(obj.toList: _*))
        def jnull() = Data.Null
        def jfalse() = Data.False
        def jtrue() = Data.True
        def jnum(n: String) = Data.Dec(BigDecimal(n))
        def jint(n: String) = Data.Int(BigInt(n))
        def jstring(s: String) = Data.Str(s)
      }
  }

  def toXml[F[_]: MonadErrMsg](data: Data): F[Elem] = {
    def typeAttr(tpe: EJsonType): Attribute =
      Attribute(ejsBinding.prefix, ejsonType.local.shows, tpe, Null)

    def ejsElem(name: QName, tpe: Option[EJsonType], ns: NamespaceBinding, children: Seq[Node]): Elem =
      Elem(name.prefix.map(_.shows).orNull, name.local.shows, tpe.cata(typeAttr, Null), ns, true, children: _*)

    def innerElem(name: QName, tpe: EJsonType, children: Seq[Node]): Elem =
      ejsElem(name, Some(tpe), TopScope, children)

    def innerStr(name: QName, str: String): Elem =
      ejsElem(name, None, TopScope, Text(str))

    def rootElem(name: QName, tpe: EJsonType, children: Seq[Node]): Elem =
      ejsElem(name, Some(tpe), ejsBinding, children)

    def rootStr(name: QName, str: String): Elem =
      ejsElem(name, None, ejsBinding, Text(str))

    def toXml0(
      str: (QName, String) => Elem,
      elem: (QName, EJsonType, Seq[Node]) => Elem,
      loop: QName => Data => F[Elem]
    ): QName => Data => F[Elem] = {
      val mapEntryToXml: ((String, Data)) => F[Elem] = {
        case (k, v) => for {
          qn <- NCName(k).fold(
                  κ(invalidQName[F, QName](k)),
                  QName.local(_).point[F])
          el <- loop(qn)(v)
        } yield el
      }

      elementName => {
        case Data.Binary(bytes) => ???
        case Data.Bool(b)       => elem(elementName, "boolean", Text(b.fold("true", "false"))).point[F]
        case Data.Date(d)       => ???
        case Data.Dec(d)        => elem(elementName, "decimal", Text(d.toString)             ).point[F]
        case Data.Id(id)        => elem(elementName, "id"     , Text(id)                     ).point[F]
        case Data.Int(i)        => elem(elementName, "integer", Text(i.toString)             ).point[F]
        case Data.Interval(ivl) => ???
        case Data.Null          => elem(elementName, "null"   , Nil                          ).point[F]
        case Data.Str(s)        => str(elementName            , s                            ).point[F]
        case Data.Time(t)       => ???
        case Data.Timestamp(ts) => ???

        case Data.Arr(elements) =>
          elements traverse loop(ejsonArrayElt) map (elem(elementName, "array", _))

        case Data.Obj(entries)  =>
          entries.toList traverse mapEntryToXml map (elem(elementName, "object", _))

        case other              => noReprErr[F, Elem](other)
      }
    }

    def inner: QName => Data => F[Elem] =
      name => toXml0(innerStr, innerElem, inner)(name)

    toXml0(rootStr, rootElem, inner)(ejsonEjson)(data)
  }

  private val ejsBinding: NamespaceBinding =
    NamespaceBinding(ejsonNs.prefix.shows, ejsonNs.uri.shows, TopScope)

  private def noReprErr[F[_]: MonadErrMsg, A](data: Data): F[A] =
    s"No representation for '$data' in XML.".raiseError[F, A]

  private def invalidQName[F[_]: MonadErrMsg, A](s: String): F[A] =
    s"'$s' is not a valid XML QName.".raiseError[F, A]
}
