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
package ingest

import blueeyes._, json._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._
import scalaz._
import quasar.precog.TestSupport._
import ArbitraryEventMessage._

class IngestSpec extends quasar.QuasarSpecification {
  implicit val arbEvent   = Arbitrary(genRandomIngest)
  implicit val arbArchive = Arbitrary(genRandomArchive)

  "serialization of an event" should {
    "read back the data that was written" in prop { in: Ingest =>
      in.serialize.validated[Ingest] must beLike {
        case Success(out) => out must_== in
      }
    }
  }

  "Event serialization" should {
    "Handle V1 format" in {
      (JObject("apiKey" -> JString("1234"),
               "path"    -> JString("/test/"),
               "data"    -> JObject("test" -> JNum(1)),
               "metadata" -> JArray())).validated[Ingest] must beLike {
        case Success(_) => ok
        case Failure(Thrown(ex)) =>
          throw ex
      }
    }
  }

  "Archive serialization" should {
    "Handle V1 format" in {
      JObject("apiKey" -> JString("1234"),
              "path"   -> JString("/test/")).validated[Archive] must beLike {
        case Success(_) => ok
      }
    }
  }

  "EventId" should {
    implicit val idRange: Arbitrary[Int] = Arbitrary(Gen.chooseNum[Int](0, Int.MaxValue))

    "support round-trip encap/decap of producer/sequence ids" in prop { (prod: Int, seq: Int) =>
      val uid = EventId(prod, seq).uid

      EventId.producerId(uid) mustEqual prod
      EventId.sequenceId(uid) mustEqual seq
    }
  }

  "serialization of an archive" should {
    "read back the data that was written" in prop { in: Archive =>
      in.serialize.validated[Archive] must beLike {
        case Success(out) => in must_== out
      }
    }

    "read new archives" in {
      val Success(JArray(input)) = JParser.parseFromString("""[
{"apiKey":"test1","path":"/foo1/test/js/delete/"},
{"apiKey":"test2","path":"/foo2/blargh/"},
{"apiKey":"test2","path":"/foo2/blargh/"},
{"apiKey":"test2","path":"/foo2/testing/"},
{"apiKey":"test2","path":"/foo2/testing/"}
]""")

      val results = input.map(_.validated[Archive]).collect {
        case Success(result) => result
      }

      results.size mustEqual 5
      results.map(_.apiKey).toSet mustEqual Set("test1", "test2")
    }

    "read archives with reversed fields" in {
      val Success(JArray(input)) = JParser.parseFromString("""[
{"path":"test1","apiKey":"/foo1/test/js/delete/"},
{"path":"test2","apiKey":"/foo2/blargh/"},
{"path":"test2","apiKey":"/foo2/blargh/"},
{"path":"test2","apiKey":"/foo2/testing/"},
{"path":"test2","apiKey":"/foo2/testing/"}
]""")

      val results = input.map(_.validated[Archive]).collect {
        case Success(result) => result
      }

      results.size mustEqual 5
      results.map(_.apiKey).toSet mustEqual Set("test1", "test2")
    }
  }
}
