/*
 * Copyright 2020 Precog Data
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

package quasar.contrib.fs2

import slamdata.Predef._

import fs2.Stream

import org.specs2.mutable.Specification

class ExpectSpec extends Specification {

  "stream size expectation" should {
    "accept an empty stream" in {
      Stream.empty.through(expect.size(0L)(Stream.emit(_))).compile.toList must beEmpty
    }

    "reject an empty stream when expecting more" in {
      Stream.empty.through(expect.size(1L)(Stream.emit(_))).compile.toList mustEqual List(0L)
    }

    "accept a stream of five things across two chunks" in {
      val s = Stream(1, 2) ++ Stream(3, 4, 5)
      s.through(expect.size(5L)(size => Stream.emit(size.toInt))).compile.toList mustEqual List(1, 2, 3, 4, 5)
    }

    "reject a stream of five things across two chunks when expecting more" in {
      val s = Stream(1, 2) ++ Stream(3, 4, 5)
      s.through(expect.size(7L)(size => Stream.emit(size.toInt))).compile.toList mustEqual List(1, 2, 3, 4, 5, 5)
    }

    "reject a stream of five things across two chunks when expecting less" in {
      val s = Stream(1, 2) ++ Stream(3, 4, 5)
      s.through(expect.size(4L)(size => Stream.emit(size.toInt))).compile.toList mustEqual List(1, 2, 3, 4, 5, 5)
    }
  }
}
