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

import java.util.zip.{ZipException, ZipInputStream}

import cats.ApplicativeError
import cats.data.OptionT
import cats.effect.{ConcurrentEffect, ContextShift}
import cats.effect.Blocker

import fs2.{Stream, Pipe, Pull}
import fs2.io

object compression {

  def unzip[F[_]](blocker: Blocker, chunkSize: Int)(
      implicit F: ConcurrentEffect[F],
      cs: ContextShift[F])
      : Pipe[F, Byte, Byte] =
    isZip(_).through(
      unzipEach[F](blocker, chunkSize).andThen(_.flatMap(_._2)))

  ////

   /* https://pkware.cachefly.net/webdocs/APPNOTE/APPNOTE-6.3.9.TXT
    *
    * A ZIP file must begin with a "local file header" signature or
    * the "end of central directory record" signature.
    */
  private def isZip[F[_]: ApplicativeError[?[_], Throwable]](
      bytes: Stream[F, Byte])
      : Stream[F, Byte] = {
    val back: Pull[F, Byte, Unit] = bytes.pull.unconsN(4) flatMap {
      case Some((chunk, rest)) =>
        val list: List[Byte] = chunk.toList
        if (list == List[Byte](80, 75, 3, 4) || list == List[Byte](80, 75, 5, 6))
          Pull.output[F, Byte](chunk) >> rest.pull.echo
        else
          Pull.raiseError[F](new ZipException("Not in zip format"))

      case None =>
        Pull.raiseError[F](new ZipException("Not in zip format"))
    }

    back.stream
  }

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
