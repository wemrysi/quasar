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

package quasar

import quasar.Predef._

import org.scalacheck.{Arbitrary, Gen, Shrink}
import org.threeten.bp._

trait DataArbitrary {
  import DataArbitrary._

  implicit val dataArbitrary: Arbitrary[Data] = Arbitrary {
    Gen.oneOf(
      simpleData,
      genData(Gen.alphaChar.map(_.toString), Gen.alphaStr, defaultInt, defaultDec, defaultId),
      // TODO: These belong in MongoDB tests somewhere.
      // Tricky cases:
      Gen.const(Data.Obj(ListMap("$date" -> Data.Str("Jan 1")))),
      SafeInt.map(x =>
        Data.Obj(ListMap(
          "$obj" -> Data.Obj(ListMap(
            "$obj" -> Data.Int(x)))))))
  }

  implicit def dataShrink(implicit l: Shrink[List[Data]], m: Shrink[ListMap[String, Data]]): Shrink[Data] = Shrink {
    case Data.Arr(value) => l.shrink(value).map(Data.Arr(_))
    case Data.Obj(value) => m.shrink(value).map(Data.Obj(_))
    case _               => Stream.empty
  }
}

object DataArbitrary extends DataArbitrary {
  import Arbitrary.arbitrary

  // Too big for Long
  val LargeInt = BigInt(Long.MaxValue.toString + "0")

  /** Long value that can safely be represented in any possible backend
    * (including those using JavaScript.)
    */
  val SafeInt: Gen[Long] = Gen.choose(-1000L, 1000L)

  val defaultInt: Gen[BigInt] =
    Gen.oneOf(SafeInt.map(BigInt(_)), Gen.const(LargeInt))

  // NB: Decimals that look like ints, may need special handling
  val defaultDec: Gen[BigDecimal] =
    Gen.oneOf(Gen.choose(-1000.0, 1000.0), SafeInt.map(_.toDouble)) map (BigDecimal(_))

  // NB: a (nominally) valid MongoDB id, because we use this generator to test
  //     BSON conversion, too
  val defaultId = Gen.listOfN(24, Gen.oneOf[Char]("0123456789abcdef")) map (_.mkString)

  // TODO: make this very conservative so as likely to work with as many backends as possible
  val simpleData: Gen[Data] =
    genAtomicData(Gen.alphaStr, defaultInt, defaultDec, defaultId)

  def genData(objKeySrc: Gen[String], strSrc: Gen[String], intSrc: Gen[BigInt], decSrc: Gen[BigDecimal], idSrc: Gen[String]): Gen[Data] = {
    val atomic = genAtomicData(strSrc, intSrc, decSrc, idSrc)
    Gen.oneOf(
      atomic,
      Gen.listOf(Gen.zip(objKeySrc, atomic)) map (xs => Data.Obj(ListMap(xs: _*))),
      Gen.listOf(atomic)                     map (Data.Arr(_)))
  }

  /** Generator of atomic Data (everything but Obj and Arr). */
  def genAtomicData(strSrc: Gen[String], intSrc: Gen[BigInt], decSrc: Gen[BigDecimal], idSrc: Gen[String]): Gen[Data] =
    Gen.oneOf(
      Gen                    const (Data.Null        ),
      Gen                    const (Data.True        ),
      Gen                    const (Data.False       ),
      Gen                    const (Data.NA          ),
      strSrc                 map   (Data.Str(_)      ),
      intSrc                 map   (Data.Int(_)      ),
      decSrc                 map   (Data.Dec(_)      ),
      genInstant             map   (Data.Timestamp(_)),
      genDuration            map   (Data.Interval(_) ),
      genDate                map   (Data.Date(_)     ),
      genTime                map   (Data.Time(_)     ),
      arbitrary[Array[Byte]] map   (Data.Binary.fromArray(_)),
      idSrc                  map   (Data.Id(_)       ))

  private def genInstant: Gen[Instant] =
    Gen.zip(arbitrary[Int], Gen.choose[Long](0, 999)) map {
      case (sec, millis) => Instant.ofEpochSecond(sec.toLong, millis * 1000000)
    }

  private def genDuration: Gen[Duration] =
    Gen.zip(arbitrary[Int], Gen.choose[Long](0, 999)) map {
      case (sec, millis) => Duration.ofSeconds(sec.toLong, millis * 1000000)
    }

  private def genDate: Gen[LocalDate] =
    arbitrary[Int] map (d => LocalDate.ofEpochDay(d.toLong))

  private def genTime: Gen[LocalTime] =
    Gen.choose[Long](0, 24 * 60 * 60 - 1) map (LocalTime.ofSecondOfDay(_))
}
