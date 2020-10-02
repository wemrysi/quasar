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

import java.util.zip.ZipInputStream

import cats.data.OptionT
import cats.effect.{ConcurrentEffect, ContextShift}
import cats.effect.Blocker

import fs2.{Stream, Pipe}
import fs2.io

object compression {

   def unzip[F[_]](blocker: Blocker, chunkSize: Int)(
       implicit F: ConcurrentEffect[F],
       cs: ContextShift[F])
       : Pipe[F, Byte, Byte] =
     unzipEach[F](blocker, chunkSize).andThen(_.flatMap(_._2))

  ////

  /* unzipEach comes from https://gist.github.com/nmehitabel/a7c976ef8f0a41dfef88e981b9141075#file-fs2zip-scala-L18
   *
   * Danger Will Robinson! Do not use without flattening inner stream!
   * https://github.com/precog/quasar/pull/4690#discussion_r499025426
   */
  private def unzipEach[F[_]](
      blocker: Blocker,
      chunkSize: Int)(
      implicit F: ConcurrentEffect[F],
      cs: ContextShift[F])
      : Pipe[F, Byte, (String, Stream[F, Byte])] = {

    def unzipEntry(zis: ZipInputStream): OptionT[F, (String, Stream[F, Byte])] = {
      val next = OptionT(blocker.delay(Option(zis.getNextEntry())))

      next map { entry =>
        val str = io.readInputStream[F](F.pure(zis), chunkSize, blocker, closeAfterUse = false)
        (entry.getName, str.onFinalize(blocker.delay(zis.closeEntry())))
      }
    }

    def unzipEntries(zis: ZipInputStream): Stream[F, (String, Stream[F, Byte])] =
      Stream.unfoldEval(zis) { zis0 =>
        unzipEntry(zis0).map((_, zis0)).value
      }

    value: Stream[F, Byte] =>
      value.through(io.toInputStream) flatMap { is: InputStream =>
        val zis: F[ZipInputStream] =
          blocker.delay(new ZipInputStream(is))

        val zres: Stream[F, ZipInputStream] =
          Stream.bracket(zis)(zis => blocker.delay(zis.close()))

        zres.flatMap(unzipEntries)
      }
   }
}
