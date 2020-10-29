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

package quasar.impl.logging

import java.nio.charset.StandardCharsets

import org.specs2.mutable.Specification

import cats.effect.concurrent.Ref
import cats.effect.testing.specs2.CatsIO
import cats.effect.IO
import fs2.{Chunk, Stream}

import scala.{Stream => _, _}
import scala.Predef._

class LoggingUtilsSpec extends Specification with CatsIO {
  val stream: Stream[IO, Byte] = Stream.emits("abcdefg".getBytes(StandardCharsets.UTF_8)).chunkN(2).flatMap(Stream.chunk)
  "logFirstNChunks" >> {
    def update(ref: Ref[IO, String])(bytes: Chunk[Byte]): IO[Unit] =
      ref.getAndUpdate(_ ++ new String(bytes.toArray, StandardCharsets.UTF_8)).void

    "log zero chunks" >> {
      for {
        ref <- Ref.of[IO, String]("")
        lst <- LoggingUtils.logFirstNChunks(stream, 0, update(ref)).compile.toList
        res <- ref.get
      } yield (res must_=== "") && (lst must_=== "abcdefg".getBytes(StandardCharsets.UTF_8).toList)
    }

    "log one chunk" >> {
      for {
        ref <- Ref.of[IO, String]("")
        lst <- LoggingUtils.logFirstNChunks(stream, 1, update(ref)).compile.toList
        res <- ref.get
      } yield (res must_=== "ab") && (lst must_=== "abcdefg".getBytes(StandardCharsets.UTF_8).toList)
    }

    "log two chunks" >> {
      for {
        ref <- Ref.of[IO, String]("")
        lst <- LoggingUtils.logFirstNChunks(stream, 2, update(ref)).compile.toList
        res <- ref.get
      } yield (res must_=== "abcd") && (lst must_=== "abcdefg".getBytes(StandardCharsets.UTF_8).toList)
    }

    "log three chunks" >> {
      for {
        ref <- Ref.of[IO, String]("")
        lst <- LoggingUtils.logFirstNChunks(stream, 3, update(ref)).compile.toList
        res <- ref.get
      } yield (res must_=== "abcdef") && (lst must_=== "abcdefg".getBytes(StandardCharsets.UTF_8).toList)
    }
  }

  "logFirstN" >> {
    def update(ref: Ref[IO, String])(s: String): IO[Unit] = ref.getAndUpdate(_ ++ s).void

    "log zero chunks" >> {
      for {
        ref <- Ref.of[IO, String]("")
        lst <- LoggingUtils.logFirstN(stream, 0, update(ref)).compile.toList
        res <- ref.get
      } yield (res must_=== "") && (lst must_=== "abcdefg".getBytes(StandardCharsets.UTF_8).toList)
    }

    "log one chunk" >> {
      for {
        ref <- Ref.of[IO, String]("")
        lst <- LoggingUtils.logFirstN(stream, 1, update(ref)).compile.toList
        res <- ref.get
      } yield (res must_=== "ab") && (lst must_=== "abcdefg".getBytes(StandardCharsets.UTF_8).toList)
    }

    "log two chunks" >> {
      for {
        ref <- Ref.of[IO, String]("")
        lst <- LoggingUtils.logFirstN(stream, 2, update(ref)).compile.toList
        res <- ref.get
      } yield (res must_=== "abcd") && (lst must_=== "abcdefg".getBytes(StandardCharsets.UTF_8).toList)
    }

    "log three chunks" >> {
      for {
        ref <- Ref.of[IO, String]("")
        lst <- LoggingUtils.logFirstN(stream, 3, update(ref)).compile.toList
        res <- ref.get
      } yield (res must_=== "abcdef") && (lst must_=== "abcdefg".getBytes(StandardCharsets.UTF_8).toList)
    }
  }

  "logFirstNDrain" >> {
    def update(ref: Ref[IO, String])(s: String): IO[Unit] = ref.getAndUpdate(_ ++ s).void
    def updateObs(ref: Ref[IO, Array[Byte]])(b: Byte): IO[Unit] = ref.getAndUpdate(_ :+ b).void

    "log three chunks" >> {
      for {
        ref <- Ref.of[IO, String]("")
        _ <- LoggingUtils.logFirstNDrain(stream, 3, update(ref))
        res <- ref.get
      } yield res must_=== "abcdef"
    }

    "eval all chunks" >> {
      for {
        ref <- Ref.of[IO, String]("")
        obs <- Ref.of[IO, Array[Byte]](Array.empty)
        _ <- LoggingUtils.logFirstNDrain(stream.evalTap(updateObs(obs)), 3, update(ref))
        seen <- obs.get
      } yield seen must_=== "abcdefg".getBytes(StandardCharsets.UTF_8)
    }
  }
}
