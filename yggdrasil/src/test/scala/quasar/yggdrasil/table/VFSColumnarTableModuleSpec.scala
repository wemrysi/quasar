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
package table

import quasar.precog.common.CLong

import cats.effect.IO

import org.specs2.mutable.Specification

import scalaz.StreamT

import shims._

import java.util.concurrent.atomic.AtomicInteger

object VFSColumnarTableModuleSpec
    extends Specification
    with VFSColumnarTableModule
    with nihdb.NIHDBAkkaSetup {

  implicit val ExecutionContext = scala.concurrent.ExecutionContext.global

  "table tempfile caching" should {
    "evaluate effects exactly once (sequencing lazily)" in {
      val ctr = new AtomicInteger(0)
      val cget = IO(ctr.get())

      val sliceM = IO {
        ctr.incrementAndGet()
        Slice.fromRValues(Stream(CLong(42))) :: StreamT.empty[IO, Slice]
      }

      val table = Table(StreamT.wrapEffect(sliceM), UnknownSize)

      val eff = for {
        cachedD <- cacheTable(table, false)
        cached = cachedD.unsafeValue

        tests = for {
          i <- cget
          _ <- IO(i mustEqual 0)

          _ <- cached.slices.foreach(_ => IO.pure(()))

          i <- cget
          _ <- IO(i mustEqual 1)

          _ <- cached.slices.foreach(_ => IO.pure(()))

          i <- cget
          _ <- IO(i mustEqual 1)
        } yield ()

        e <- tests.attempt
        _ <- cachedD.dispose
        _ <- e.fold(IO.raiseError(_), IO.pure(_))
      } yield ok

      eff.unsafeRunSync
    }

    "evaluate effects exactly once (sequencing eagerly)" in {
      val ctr = new AtomicInteger(0)
      val cget = IO(ctr.get())

      val sliceM = IO {
        ctr.incrementAndGet()
        Slice.fromRValues(Stream(CLong(42))) :: StreamT.empty[IO, Slice]
      }

      val table = Table(StreamT.wrapEffect(sliceM), UnknownSize)

      val eff = for {
        cachedD <- cacheTable(table, true)
        cached = cachedD.unsafeValue

        tests = for {
          i <- cget
          _ <- IO(i mustEqual 1)

          _ <- cached.slices.foreach(_ => IO.pure(()))

          i <- cget
          _ <- IO(i mustEqual 1)

          _ <- cached.slices.foreach(_ => IO.pure(()))

          i <- cget
          _ <- IO(i mustEqual 1)
        } yield ()

        e <- tests.attempt
        _ <- cachedD.dispose
        _ <- e.fold(IO.raiseError(_), IO.pure(_))
      } yield ok

      eff.unsafeRunSync
    }
  }

  def vfs = ???
  def StorageTimeout = Timeout

  sealed trait TableCompanion extends VFSColumnarTableCompanion
  object Table extends TableCompanion
}
