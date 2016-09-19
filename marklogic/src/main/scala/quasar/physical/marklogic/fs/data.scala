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
import quasar.physical.marklogic.xml._
import quasar.physical.marklogic.xml.namespaces._

import scala.xml._
import scala.collection.immutable.Seq
import java.util.Base64

import eu.timepit.refined.auto._
import jawn._
import monocle.Prism
import org.threeten.bp._
import org.threeten.bp.format._
import org.threeten.bp.temporal.TemporalAccessor
import scalaz.{Node => _, _}, Scalaz._

object data {
  object JsonParser extends SupportParser[Data] {
    implicit val facade: Facade[Data] =
      new SimpleFacade[Data] {
        def jarray(arr: List[Data])         = Data.Arr(arr)
        def jobject(obj: Map[String, Data]) = Data.Obj(ListMap(obj.toList: _*))
        def jnull()                         = Data.Null
        def jfalse()                        = Data.False
        def jtrue()                         = Data.True
        def jnum(n: String)                 = Data.Dec(BigDecimal(n))
        def jint(n: String)                 = Data.Int(BigInt(n))
        def jstring(s: String)              = Data.Str(s)
      }
  }

  def toXml[F[_]: MonadErrMsgs](data: Data): F[Elem] = {
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
      loop: QName => Data => Validation[ErrorMessages, Elem]
    ): QName => Data => Validation[ErrorMessages, Elem] = {
      val mapEntryToXml: ((String, Data)) => ErrorMessages \/ Elem = {
        case (k, v) => for {
          nc <- NCName(k) leftAs s"'$k' is not a valid XML QName.".wrapNel
          el <- loop(QName.local(nc))(v).disjunction
        } yield el
      }

      elementName => {
        case Data.Binary(bytes) => elem(elementName, "binary"   , Text(base64(bytes))          ).success
        case Data.Bool(b)       => elem(elementName, "boolean"  , Text(b.fold("true", "false"))).success
        case Data.Date(d)       => elem(elementName, "date"     , Text(localDate(d))           ).success
        case Data.Dec(d)        => elem(elementName, "decimal"  , Text(d.toString)             ).success
        case Data.Id(id)        => elem(elementName, "id"       , Text(id)                     ).success
        case Data.Int(i)        => elem(elementName, "integer"  , Text(i.toString)             ).success
        case Data.Interval(d)   => elem(elementName, "interval" , Text(duration(d))            ).success
        case Data.NA            => elem(elementName, "na"       , Nil                          ).success
        case Data.Null          => elem(elementName, "null"     , Nil                          ).success
        case Data.Str(s)        => str(elementName              , s                            ).success
        case Data.Time(t)       => elem(elementName, "time"     , Text(localTime(t))           ).success
        case Data.Timestamp(ts) => elem(elementName, "timestamp", Text(instant(ts))            ).success

        case Data.Arr(elements) =>
          elements traverse loop(ejsonArrayElt) map (elem(elementName, "array", _))

        case Data.Obj(entries)  =>
          entries.toList traverse (mapEntryToXml andThen (_.validation)) map (elem(elementName, "object", _))

        case other              => s"No representation for '$other' in XML.".failureNel[Elem]
      }
    }

    def inner: QName => Data => Validation[ErrorMessages, Elem] =
      name => toXml0(innerStr, innerElem, inner)(name)

    toXml0(rootStr, rootElem, inner)(ejsonEjson)(data)
      .fold(_.raiseError[F, Elem], _.point[F])
  }

  def fromXml[F[_]: MonadErrMsgs](elem: Elem): F[Data] = {
    def fromXml0: Node => Validation[ErrorMessages, Data] = {
      case DataNode("array", children) =>
        elements(children).toList traverse fromXml0 map (Data._arr(_))

      case DataNode("binary", Txt(b64)) =>
        base64.getOption(b64)
          .map(bytes => Data._binary(bytes))
          .toSuccessNel(s"Expected Base64-encoded binary data, found: $b64")

      case DataNode("binary", Seq()   ) =>
        Data._binary(ImmutableArray.fromArray(Array())).success

      case DataNode("boolean", Txt("true"))  => Data._bool(true).success
      case DataNode("boolean", Txt("false")) => Data._bool(false).success

      case DataNode("date", Txt(d)) =>
        localDate.getOption(d)
          .map(Data._date(_))
          .toSuccessNel(s"Expected an ISO-8601 formatted local date, found: $d")

      case DataNode("decimal", Txt(d)) =>
        Validation.fromTryCatchNonFatal(BigDecimal(d))
          .map(Data._dec(_))
          .leftAs(s"Expected a decimal number, found: $d".wrapNel)

      case DataNode("id", Txt(id)) => Data._id(id).success
      case DataNode("id", Seq()  ) => Data._id("").success

      case DataNode("integer", Txt(n)) =>
        Validation.fromTryCatchNonFatal(BigInt(n))
          .map(Data._int(_))
          .leftAs(s"Expected an integral number, found: $n".wrapNel)

      case DataNode("interval", Txt(ivl)) =>
        duration.getOption(ivl)
          .map(Data._interval(_))
          .toSuccessNel(s"Expected a duration in seconds, found: $ivl")

      case DataNode("na", Seq()) => (Data.NA: Data).success

      case DataNode("null", Seq()) => (Data.Null: Data).success

      case DataNode("object", children) =>
        elements(children).toList
          .traverse(el => fromXml0(el) strengthL qualifiedName(el))
          .map(entries => Data._obj(ListMap(entries: _*)))

      case DataNode("string", Txt(s)) => Data._str(s).success
      case DataNode("string", Seq() ) => Data._str("").success

      case DataNode("time", Txt(t)) =>
        localTime.getOption(t)
          .map(Data._time(_))
          .toSuccessNel(s"Expected an ISO-8601 formatted local time, found: $t")

      case DataNode("timestamp", Txt(ts)) =>
        instant.getOption(ts)
          .map(Data._timestamp(_))
          .toSuccessNel(s"Expected an ISO-8601 formatted date-time, found: $ts")

      case DataNode(tpe, _) => s"Unrecognized type: $tpe".failureNel
      case other            => s"Unrecognized Data XML: $other.".failureNel
    }

    fromXml0(elem).fold(_.raiseError[F, Data], _.point[F])
  }

  ////

  private type EJsonType = String

  private object DataNode {
    def unapply(node: Node): Option[(EJsonType, Seq[Node])] = {
      val tpe = node.attributes collectFirst {
        case PrefixedAttribute(p, n, Seq(Text(t)), _) if s"$p:$n" === ejsonType.shows => t
      } getOrElse "string"

      Some((tpe, node.child))
    }
  }

  private object Txt {
    def unapply(nodes: Seq[Node]): Option[String] =
      nodes.headOption collect { case Text(s) => s }
  }

  // xs:base64Binary
  private val base64 = Prism[String, ImmutableArray[Byte]](
    s => \/.fromTryCatchNonFatal(Base64.getDecoder.decode(s)).map(ImmutableArray.fromArray).toOption)(
    (Base64.getEncoder.encodeToString(_)) compose (_.toArray))

  // xs:date
  private val localDate: Prism[String, LocalDate] = temporal(LocalDate.from, DateTimeFormatter.ISO_LOCAL_DATE)
  // xs:time
  private val localTime: Prism[String, LocalTime] = temporal(LocalTime.from, DateTimeFormatter.ISO_LOCAL_TIME)
  // xs:dateTime
  private val instant: Prism[String, Instant]     = temporal(Instant.from  , DateTimeFormatter.ISO_INSTANT)

  private def temporal[T <: TemporalAccessor](f: TemporalAccessor => T, fmt: DateTimeFormatter): Prism[String, T] =
    Prism[String, T](s => \/.fromTryCatchNonFatal(f(fmt.parse(s))).toOption)(fmt.format)

  private val DurationEncoding = "(-?\\d+)(?:\\.(\\d+))?".r

  private val duration = Prism[String, Duration] {
    case DurationEncoding(secs, nanos) =>
      \/.fromTryCatchNonFatal(Duration.ofSeconds(secs.toLong, nanos.toLong)).toOption

    case DurationEncoding(secs) =>
      \/.fromTryCatchNonFatal(Duration.ofSeconds(secs.toLong)).toOption

    case _ => None
  } (d => s"${d.getSeconds}.${d.getNano}")

  private val ejsBinding: NamespaceBinding =
    NamespaceBinding(ejsonNs.prefix.shows, ejsonNs.uri.shows, TopScope)
}
