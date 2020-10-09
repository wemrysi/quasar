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

import quasar.concurrent._

import java.io.File
import java.nio.file.Files

import cats.effect.{Blocker, IO}
import cats.effect.testing.specs2.CatsIO

import fs2.{Chunk, Stream}

import org.specs2.mutable.Specification

object CompressionSpec extends Specification with CatsIO {

  val compress = Blocker.cached[IO]("fs2-compression-spec")

  "ZIP decompression" should {

    "decompressed zipped json" in {
      val file = new File("foundation/src/test/resources/data.json.zip")
      val bytes = Files.readAllBytes(file.toPath())
      val byteStream: Stream[IO, Byte] = Stream.chunk(Chunk.array(bytes))

      val unzipped =
        Stream.resource(compress).flatMap(blocker =>
          byteStream.through(compression.unzip[IO](blocker, 2048)))

      val output =
        unzipped
          .compile
          .toList
          .map(s => new String(s.toArray, "UTF-8"))

      output.map(_ mustEqual "{\"foo\":1}\n")
    }

    "decompressed zipped json with chunk size smaller than number of bytes" in {
      val file = new File("foundation/src/test/resources/data.json.zip")
      val bytes = Files.readAllBytes(file.toPath())
      val byteStream: Stream[IO, Byte] = Stream.chunk(Chunk.array(bytes))

      val unzipped =
        Stream.resource(compress).flatMap(blocker =>
          byteStream.through(compression.unzip[IO](blocker, 8)))

      val output =
        unzipped
          .compile
          .toList
          .map(s => new String(s.toArray, "UTF-8"))

      output.map(_ mustEqual "{\"foo\":1}\n")
    }

    "error when bytes are not zipped" in {
      val file = new File("foundation/src/test/resources/data.json")
      val bytes = Files.readAllBytes(file.toPath())
      val byteStream: Stream[IO, Byte] = Stream.chunk(Chunk.array(bytes))

      val unzipped =
        Stream.resource(compress).flatMap(blocker =>
          byteStream.through(compression.unzip[IO](blocker, 2048)))

      val output =
        unzipped
          .compile
          .toList
          .map(s => new String(s.toArray, "UTF-8"))

      output.attempt.unsafeRunSync() must beLeft
    }

    "error when bytes are not zipped and there are fewer than 4 bytes" in {
      val byteStream: Stream[IO, Byte] = Stream.eval(IO(42: Byte))

      val unzipped =
        Stream.resource(compress).flatMap(blocker =>
          byteStream.through(compression.unzip[IO](blocker, 2048)))

      val output =
        unzipped
          .compile
          .toList
          .map(s => new String(s.toArray, "UTF-8"))

      output.attempt.unsafeRunSync() must beLeft
    }
  }
}
