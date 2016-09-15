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
import quasar.ejson.EJson
import quasar.fp.{interpret, interpretM}
import quasar.physical.marklogic.MonadErrMsg
import quasar.physical.marklogic.xml._
import quasar.physical.marklogic.xml.namespaces._

import scala.xml._

import eu.timepit.refined.auto._
import jawn._
import matryoshka._
import matryoshka.patterns._
import scalaz.{Node => _, _}, Scalaz._

object data {
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
    def typeAttr(tpe: String): Attribute =
      Attribute(ejsBinding.prefix, ejsonType.local.shows, tpe, Null)

    def elem(name: QName, attrs: MetaData, children: Node*): Elem =
      Elem(name.prefix.map(_.shows).orNull, name.local.shows, attrs, TopScope, true, children: _*)

    def typed(name: QName, tpe: String, children: Node*): F[Elem] =
      elem(name, typeAttr(tpe), children: _*).point[F]

    def rootElem(name: QName, attrs: MetaData, children: Node*): Elem =
      Elem(name.prefix.map(_.shows).orNull, name.local.shows, attrs, ejsBinding, true, children: _*)

    def literal(tpe: String, children: Node*): F[Elem] =
      rootElem(ejsonLiteral, typeAttr(tpe), children: _*).point[F]

    def toXml0(elementName: QName): Data => F[Elem] = {
      case Data.Binary(bytes) => ???
      case Data.Bool(b)       => typed(elementName, "boolean", Text(b.fold("true", "false")))
      case Data.Date(d)       => ???
      case Data.Dec(d)        => typed(elementName, "decimal", Text(d.toString))
      case Data.Id(id)        => typed(elementName, "id", Text(id))
      case Data.Int(i)        => typed(elementName, "integer", Text(i.toString))
      case Data.Interval(ivl) => ???
      case Data.Null          => typed(elementName, "null")
      case Data.Str(s)        => typed(elementName, "string", Text(s))
      case Data.Time(t)       => ???
      case Data.Timestamp(ts) => ???

      case Data.Arr(elements) =>
        elements.traverse(toXml0(ejsonArrayElt)) map { kids =>
          elem(elementName, Null, elem(ejsonArray, Null, kids: _*))
        }

      case Data.Obj(entries)  =>
        entries.toList.traverse { case (k, v) =>
          for {
            qn <- NCName(k).fold(κ(invalidQName[F, QName](k)), QName.local(_).point[F])
            el <- toXml0(qn)(v)
          } yield el
        } map { kids =>
          elem(elementName, Null, kids: _*)
        }

      case Data.NA            => ???
      case Data.Set(xs)       => ???
    }

    data match {
      case Data.Binary(bytes) => ???
      case Data.Bool(b)       => literal("boolean", Text(b.fold("true", "false")))
      case Data.Date(d)       => ???
      case Data.Dec(d)        => literal("decimal", Text(d.toString))
      case Data.Id(id)        => literal("id", Text(id))
      case Data.Int(i)        => literal("integer", Text(i.toString))
      case Data.Interval(ivl) => ???
      case Data.Null          => literal("null")
      case Data.Str(s)        => literal("string", Text(s))
      case Data.Time(t)       => ???
      case Data.Timestamp(ts) => ???

      case Data.Arr(elements) =>
        elements.traverse(toXml0(ejsonArrayElt)) map { kids =>
          rootElem(ejsonArray, Null, kids: _*)
        }

      case Data.Obj(entries)  =>
        entries.toList.traverse { case (k, v) =>
          for {
            qn <- NCName(k).fold(κ(invalidQName[F, QName](k)), QName.local(_).point[F])
            el <- toXml0(qn)(v)
          } yield el
        } map { kids =>
          rootElem(ejsonMap, Null, kids: _*)
        }

      case Data.NA            => ???
      case Data.Set(xs)       => ???
    }
  }

  def encodeXml[F[_]: MonadErrMsg](data: Data): F[Node] =
    data.hyloM[F, CoEnv[Data, EJson, ?], Node](
      interpretM(noReprErr[F, Node], EncodeXml[F, EJson].encodeXml),
      Data.toEJson[EJson] andThen (_.point[F]))

  def decodeXml(node: Node): Data =
    node.hylo[CoEnv[Node, EJson, ?], Data](
      interpret(κ(Data.NA), Data.fromEJson),
      DecodeXml[Id.Id, EJson].decodeXml andThen (CoEnv(_)))

  private val ejsBinding: NamespaceBinding =
    NamespaceBinding(qscriptNs.prefix.shows, qscriptNs.uri.shows, TopScope)

  // TODO{matryoshka}: Remove once we've upgraded to 0.11.2+
  private implicit def coenvTraverse[E]: Traverse[CoEnv[E, EJson, ?]] =
    Bitraverse[CoEnv[?, EJson, ?]].rightTraverse

  private def noReprErr[F[_]: MonadErrMsg, A](data: Data): F[A] =
    s"No representation for '$data' in XML.".raiseError[F, A]

  private def invalidQName[F[_]: MonadErrMsg, A](s: String): F[A] =
    s"'$s' is not a valid XML QName.".raiseError[F, A]
}
