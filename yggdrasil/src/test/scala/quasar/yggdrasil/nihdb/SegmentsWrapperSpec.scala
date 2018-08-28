/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.yggdrasil
package nihdb

import quasar.contrib.cats.effect._
import quasar.niflheim.NIHDB
import quasar.precog.common._
import quasar.precog.util.IOUtils
import quasar.yggdrasil.table._

import cats.effect.IO

import org.scalacheck._
import org.specs2.ScalaCheck
import org.specs2.mutable._

import scalaz.std.list._
import scalaz.syntax.traverse._

import shims._

import scala.concurrent.ExecutionContext.Implicits.global

import java.nio.file.Files

object SegmentsWrapperSpec extends Specification with ScalaCheck with NIHDBAkkaSetup {
  import ArbitrarySlice._

  "direct columnmar table persistence" should {
    val paths = Vector(
      CPath("0") -> CLong,
      CPath("1") -> CBoolean,
      CPath("2") -> CString,
      CPath("3") -> CDouble,
      CPath("4") -> CNum,
      CPath("5") -> CEmptyObject,
      CPath("6") -> CEmptyArray,
      CPath("7") -> CNum,
      CPath("8") -> COffsetDateTime,
      CPath("9") -> COffsetDate,
      CPath("10") -> COffsetTime,
      CPath("11") -> CLocalDateTime,
      CPath("12") -> CLocalDate,
      CPath("13") -> CLocalTime,
      CPath("14") -> CInterval)

    val pd = paths.toList map {
      case (cpath, ctype) =>
        ColumnRef(cpath, ctype)
    }

    implicit def arbSlice = Arbitrary(for {
      badSize <- Arbitrary.arbitrary[Int]
      size = scala.math.abs(badSize % 100).toInt
      slice <- genSlice(pd, size)
    } yield slice)

    "persist arbitrary tables" in prop { slices: List[Slice] =>
      val dir = Files.createTempDirectory("SegmentsWrapperSpec").toFile
      try {
        val ioa = for {
          nihdbV <- IO {
            NIHDB.create(masterChef, dir, CookThreshold, Timeout, TxLogScheduler).unsafePerformIO
          }

          _ = nihdbV.toEither must beRight
          nihdb = nihdbV.toEither.right.get

          _ <- slices.zipWithIndex traverse {
            case (slice, index) =>
              val segments = SegmentsWrapper.sliceToSegments(index.toLong, slice)

              IO.fromFutureShift(
                IO(nihdb.insertSegmentsVerified(index.toLong, slice.size, segments)))
          }

          proj <- NIHDBProjection.wrap(nihdb)
          stream <- proj.getBlockStream(None).toStream
        } yield {
          val slices2 = stream.toList.map(_.deref("value"))

          slices.flatMap(_.toRValues) mustEqual slices2.flatMap(_.toRValues)
        }

        ioa.unsafeRunSync
      } finally {
        IOUtils.recursiveDelete(dir).unsafePerformIO
      }
    }.set(
      minTestsOk = Runtime.getRuntime.availableProcessors * 2,
      workers = Runtime.getRuntime.availableProcessors)    // these take a long time
  }
}
