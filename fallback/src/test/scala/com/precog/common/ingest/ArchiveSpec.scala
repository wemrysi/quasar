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

import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._
import scalaz._
import quasar.precog.TestSupport._

class ArchiveSpec extends quasar.QuasarSpecification with ArbitraryEventMessage {
  implicit val arbArchive = Arbitrary(genRandomArchive)
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
