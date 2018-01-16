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

package quasar

import java.time.{LocalDate => JLocalDate, LocalTime => JLocalTime, LocalDateTime => JLocalDateTime, ZoneOffset}
import java.time.{OffsetTime => JOffsetTime, OffsetDateTime => JOffsetDateTime}

import slamdata.Predef._
import quasar.pkg.tests._

import scalaz.Store
import Data.{OffsetDate => DOffsetDate, _}

trait DataGenerators {
  import DataGenerators._
  import Data.Obj

  def genKey = Gen.alphaChar ^^ (_.toString)

  implicit val dataArbitrary: Arbitrary[Data] = Arbitrary(
    Gen.oneOf(
      simpleData,
      genNested(genKey, simpleData),
      // Tricky cases: (TODO: These belong in MongoDB tests somewhere.)
      const(Obj("$date" -> Str("Jan 1"))),
      SafeInt ^^ (x => Obj("$obj" -> Obj("$obj" -> Data.Int(x))))))

  implicit def dataShrink(implicit l: Shrink[List[Data]], m: Shrink[ListMap[String, Data]]): Shrink[Data] = Shrink {
    case Data.Arr(value) => l.shrink(value).map(Data.Arr(_))
    case Data.Obj(value) => m.shrink(value).map(Data.Obj(_))
    case _               => Stream.empty
  }
}

object DataGenerators extends DataGenerators {
  import DateGenerators._

  // Too big for Long
  val LargeInt = BigInt(Long.MaxValue.toString + "0")

  /** Long value that can safely be represented in any possible backend
    * (including those using JavaScript.)
    */
  val SafeInt: Gen[Long] = choose(-1000L, 1000L)
  val SafeBigInt         = SafeInt ^^ (x => BigInt(x))
  val IntAsDouble        = SafeInt ^^ (_.toDouble) // NB: Decimals that look like ints, may need special handling

  val defaultInt: Gen[BigInt] = Gen.oneOf[BigInt](SafeBigInt, LargeInt)

  // NB: Decimals that look like ints, may need special handling
  val defaultDec: Gen[BigDecimal] = Gen.oneOf[Double](Gen.choose(-1000.0, 1000.0), IntAsDouble) ^^ (x => BigDecimal(x))

  // NB: a (nominally) valid MongoDB id, because we use this generator to test BSON conversion, too
  val defaultId: Gen[String] = Gen.oneOf[Char]("0123456789abcdef") * 24 ^^ (_.mkString)

  // TODO: make this very conservative so as likely to work with as many backends as possible
  val simpleData: Gen[Data] = genAtomicData(Gen.alphaStr, defaultInt, defaultDec, defaultId)

  def genNested(genKey: Gen[String], genAtomicData: Gen[Data]): Gen[Data] = Gen.oneOf[Data](
    (genKey, genAtomicData).zip.list ^^ (xs => Data.Obj(xs: _*)),
    genAtomicData.list ^^ Data.Arr)

  /** Generator of atomic Data (everything but Obj and Arr). */
  def genAtomicData(strSrc: Gen[String], intSrc: Gen[BigInt], decSrc: Gen[BigDecimal], idSrc: Gen[String]): Gen[Data] = {
    Gen.oneOf[Data](
      Null,
      True,
      False,
      NA,
      strSrc            ^^ Str,
      intSrc            ^^ Data.Int,
      decSrc            ^^ Dec,
      genInterval       ^^ Interval,
      genOffsetDateTime ^^ OffsetDateTime,
      genOffsetDate     ^^ DOffsetDate,
      genOffsetTime     ^^ OffsetTime,
      genLocalDateTime  ^^ LocalDateTime,
      genLocalDate      ^^ LocalDate,
      genLocalTime      ^^ LocalTime,
      arrayOf(genByte)  ^^ Binary.fromArray,
      idSrc             ^^ Id)
  }

  import TemporalPart._

  implicit val arbTemporalPart: Arbitrary[TemporalPart] = genTemporalPart

  implicit def genTemporalPart: Gen[TemporalPart] = Gen.oneOf(
    Century, Day, Decade, Hour, Microsecond, Millennium,
    Millisecond, Minute, Month, Quarter, Second, Week, Year)

  implicit val genDateStore: Arbitrary[Store[JLocalDate, Data]] = Gen.oneOf[Store[JLocalDate, Data]](
    genOffsetDate.map(datetime.lensDateOffsetDate(_).map[Data](DOffsetDate)),
    genLocalDate.map(datetime.lensDateLocalDate(_).map[Data](LocalDate)),
    genLocalDateTime.map(datetime.lensDateLocalDateTime(_).map[Data](LocalDateTime)),
    genOffsetDateTime.map(datetime.lensDateOffsetDateTime(_).map[Data](OffsetDateTime)))

  implicit val genTimeStore: Arbitrary[Store[JLocalTime, Data]] = Gen.oneOf[Store[JLocalTime, Data]](
    genOffsetTime.map(datetime.lensTimeOffsetTime(_).map[Data](OffsetTime)),
    genLocalTime.map(datetime.lensTimeLocalTime(_).map[Data](LocalTime)),
    genLocalDateTime.map(datetime.lensTimeLocalDateTime(_).map[Data](LocalDateTime)),
    genOffsetDateTime.map(datetime.lensTimeOffsetDateTime(_).map[Data](OffsetDateTime)))

  implicit val genDateTimeStore: Arbitrary[Store[JLocalDateTime, Data]] = Gen.oneOf[Store[JLocalDateTime, Data]](
    genLocalDateTime.map(datetime.lensDateTimeLocalDateTime(_).map[Data](LocalDateTime)),
    genOffsetDateTime.map(datetime.lensDateTimeOffsetDateTime(_).map[Data](OffsetDateTime)))

  final case class Builder[-I, +O](f: I => O)

  implicit val genDateBuilder: Arbitrary[Builder[JLocalDate, Data]] = Gen.oneOf[Builder[JLocalDate, Data]](
    Builder[JLocalDate, Data](ld => DOffsetDate(OffsetDate(ld, ZoneOffset.UTC))),
    Builder[JLocalDate, Data](LocalDate),
    Builder[JLocalDate, Data](ld => LocalDateTime(JLocalDateTime.of(ld, JLocalTime.MIN))),
    Builder[JLocalDate, Data](ld => OffsetDateTime(JOffsetDateTime.of(ld, JLocalTime.MIN, ZoneOffset.UTC))))

  implicit val genTimeBuilder: Arbitrary[Builder[JLocalTime, Data]] = Gen.oneOf[Builder[JLocalTime, Data]](
    Builder[JLocalTime, Data](lt => OffsetTime(JOffsetTime.of(lt, ZoneOffset.UTC))),
    Builder[JLocalTime, Data](LocalTime),
    Builder[JLocalTime, Data](lt => LocalDateTime(JLocalDateTime.of(JLocalDate.MIN, lt))),
    Builder[JLocalTime, Data](lt => OffsetDateTime(JOffsetDateTime.of(JLocalDate.MIN, lt, ZoneOffset.UTC))))

  implicit val genDateTimeBuilder: Arbitrary[Builder[JLocalDateTime, Data]] = Gen.oneOf[Builder[JLocalDateTime, Data]](
    Builder[JLocalDateTime, Data](LocalDateTime),
    Builder[JLocalDateTime, Data](ldt => OffsetDateTime(JOffsetDateTime.of(ldt, ZoneOffset.UTC))))
}
