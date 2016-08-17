/*
 *  ____    ____    _____    ____    ___     ____
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.common

import blueeyes._
import blueeyes.json.serialization.DefaultSerialization._
import scalaz._, Scalaz._
import quasar.precog._, JsonTestSupport._
import Gen.frequency

class MetadataSpec extends quasar.Qspec with MetadataGenerators {
  val sampleSize = 100

  "simple metadata" should {
    "surivive round trip serialization" in prop { in: Metadata =>
      in.serialize.validated[Metadata] must beLike {
        case Success(out) => in mustEqual out
      }
    }

    "merge with like metadata" in prop { (sample1: List[Metadata], sample2: List[Metadata]) =>
      val prepared = sample1 zip sample2 map {
        case (e1, e2) => (e1, e2, e1 merge e2)
      }

      forall(prepared) {
        case (BooleanValueStats(c1, t1), BooleanValueStats(c2, t2), Some(BooleanValueStats(c3, t3))) => {
          c3 must_== c1 + c2
          t3 must_== t1 + t2
        }
        case (LongValueStats(c1, mn1, mx1), LongValueStats(c2, mn2, mx2), Some(LongValueStats(c3, mn3, mx3))) => {
          c3 must_== c1 + c2
          mn3 must_== (mn1 min mn2)
          mx3 must_== (mx1 max mx2)
        }
        case (DoubleValueStats(c1, mn1, mx1), DoubleValueStats(c2, mn2, mx2), Some(DoubleValueStats(c3, mn3, mx3))) => {
          c3 must_== c1 + c2
          mn3 must_== (mn1 min mn2)
          mx3 must_== (mx1 max mx2)
        }
        case (BigDecimalValueStats(c1, mn1, mx1), BigDecimalValueStats(c2, mn2, mx2), Some(BigDecimalValueStats(c3, mn3, mx3))) => {
          c3 must_== c1 + c2
          mn3 must_== (mn1 min mn2)
          mx3 must_== (mx1 max mx2)
        }
        case (StringValueStats(c1, mn1, mx1), StringValueStats(c2, mn2, mx2), Some(StringValueStats(c3, mn3, mx3))) => {
          c3 must_== c1 + c2
          mn3 must_== Ord[String].min(mn1, mn2)
          mx3 must_== Ord[String].max(mx1, mx2)
        }
        case (e1, e2, r) => r must beNone
      }
    }
  }

  "metadata maps" should {
    "survive round trip serialization" in prop { in: Map[MetadataType, Metadata] =>
      in.map(_._2).toList.serialize.validated[List[Metadata]] must beLike {
        case Success(out) => in must_== Map[MetadataType, Metadata](out.map{ m => (m.metadataType, m) }: _*)
      }
    }

    "merge as expected" in prop { (sample1: List[Map[MetadataType, Metadata]], sample2: List[Map[MetadataType, Metadata]]) =>
      val prepared = sample1 zip sample2 map {
        case (s1, s2) => (s1, s2, s1 |+| s2)
      }

      forall(prepared) {
        case (s1, s2, r) => {
          val keys = s1.keys ++ s2.keys

          forall(keys) { k =>
            (s1.get(k), s2.get(k)) must beLike {
              case (Some(a), Some(b)) => r(k) must_== a.merge(b).get
              case (Some(a), _)       => r(k) must_== a
              case (_, Some(b))       => r(k) must_== b
            }
          }
        }
      }
    }
  }
}

trait MetadataGenerators  {
  implicit val arbMetadata: Arbitrary[Metadata] = Arbitrary(genMetadata)
  implicit val arbMetadataMap: Arbitrary[Map[MetadataType, Metadata]] = Arbitrary(genMetadataMap)

  val metadataGenerators = List[Gen[Metadata]](genBooleanMetadata, genLongMetadata, genDoubleMetadata, genBigDecimalMetadata, genStringMetadata)

  private def upTo1K: Gen[Long]                        = choose(0L, 1000L)
  def genMetadataList: Gen[List[Metadata]]             = genMetadata * (0 upTo 10)
  def genMetadataMap: Gen[Map[MetadataType, Metadata]] = genMetadataList map { l => Map( l.map( m => (m.metadataType, m) ): _* ) }
  def genMetadata: Gen[Metadata]                       = frequency( metadataGenerators.map { (1, _) }: _* )
  def genBooleanMetadata: Gen[BooleanValueStats]       = for(count <- upTo1K; trueCount <- choose(0L, count)) yield BooleanValueStats(count, trueCount)
  def genLongMetadata: Gen[LongValueStats]             = for(count <- upTo1K; a <- genLong; b <- genLong) yield LongValueStats(count, a min b,a max b)
  def genDoubleMetadata: Gen[DoubleValueStats]         = for(count <- upTo1K; a <- genDouble; b <- genDouble) yield DoubleValueStats(count, a min b,a max b)
  def genBigDecimalMetadata: Gen[BigDecimalValueStats] = for(count <- upTo1K; a <- genBigDecimal; b <- genBigDecimal) yield BigDecimalValueStats(count, a min b, a max b)
  def genStringMetadata: Gen[StringValueStats]         = for(count <- upTo1K; a <- genString; b <- genString) yield StringValueStats(count, Ord[String].min(a,b), Ord[String].max(a,b))
}
