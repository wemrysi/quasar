package quasar

import quasar.Predef._

import org.scalacheck.{Arbitrary, Gen}
import org.threeten.bp._

trait DataArbitrary {
  implicit val dataArbitrary: Arbitrary[Data] = Arbitrary {
    Gen.oneOf(
      DataArbitrary.simpleData,
      Gen.oneOf(
        Data.Obj(ListMap("a" -> Data.Int(0), "b" -> Data.Int(1))),
        Data.Arr(List(Data.Int(0), Data.Int(1))),
        Data.Set(List(Data.Int(0), Data.Int(1))),
        // Tricky cases:
        Data.Obj(ListMap("$date" -> Data.Str("Jan 1"))),
        Data.Obj(ListMap(
          "$obj" -> Data.Obj(ListMap(
            "$obj" -> Data.Int(1)))))))
  }
}

object DataArbitrary extends DataArbitrary {
  // Too big for Long
  val LargeInt = Data.Int(new java.math.BigInteger(Long.MaxValue.toString + "0"))

  val simpleData: Gen[Data] =
    Gen.oneOf(
      Data.Null, Data.True, Data.False,
      Data.Str("abc"), Data.Int(0), Data.Dec(1.1),
      Data.Timestamp(Instant.now),
      Data.Interval(Duration.ofSeconds(1)),
      Data.Date(LocalDate.now),
      Data.Time(LocalTime.now),
      Data.Binary(Array[Byte](0, 1, 2, 3)),
      // NB: a (nominally) valid MongoDB id, because we use this generator to test
      //     BSON conversion, too
      Data.Id("123456789012345678901234"),
      Data.NA,

      // Tricky cases:
      DataArbitrary.LargeInt,
      Data.Dec(2.0)) // Looks like an Int, so needs special handling
}
