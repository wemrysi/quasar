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

package quasar.impl.storage

import slamdata.Predef._

import cats.effect.IO
import scalaz.std.string._

import java.nio.file.{Files, Path}
import scala.concurrent.ExecutionContext, ExecutionContext.Implicits.global
import scala.util.Random

import shims._

final class MapDBIndexedStoreSpec extends IndexedStoreSpec[IO, String, String] {
  val mkTempDir: IO[Path] = IO.delay(Files.createTempDirectory("mapdb-indexed-store-spec-"))

  val emptyStore = for {
    i <- IO(Random.nextInt(10000))
    tmpDir <- mkTempDir
    store <- MapDBIndexedStore[IO](tmpDir.resolve(i.toString))
  } yield store

  val freshIndex = IO(Random.alphanumeric.take(8).mkString)

  val valueA = "A ::: B"

  val valueB = "B ::: C"

  step(MapDBIndexedStore.shutdownAll[IO].unsafeRunSync)
}
