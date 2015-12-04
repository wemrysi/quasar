package quasar.api

import quasar.Predef._

import org.scalacheck.{Arbitrary, Gen}
import quasar.api.MessageFormat.{JsonContentType, Csv}
import JsonPrecision.{Precise,Readable}
import JsonFormat.{LineDelimited,SingleArray}
import scalaz.scalacheck.ScalaCheckBinding._

import scalaz._, Scalaz._

object MessageFormatGen {
  implicit val arbCSV: Arbitrary[Csv] = Arbitrary(
    (Arbitrary.arbitrary[Char]   |@|
     Arbitrary.arbitrary[String] |@|
     Arbitrary.arbitrary[Char]   |@|
     Arbitrary.arbitrary[Char])(Csv.apply(_,_,_,_,None)))
  implicit val arbJsonContentType: Arbitrary[JsonContentType] = Arbitrary(
    Gen.oneOf(
      JsonContentType(Readable, LineDelimited),
      JsonContentType(Readable,SingleArray),
      JsonContentType(Precise,LineDelimited),
      JsonContentType(Precise,SingleArray)))
  implicit val arbMessageFormat: Arbitrary[MessageFormat] =
    Arbitrary(Gen.oneOf(arbCSV.arbitrary, arbJsonContentType.arbitrary))
}
