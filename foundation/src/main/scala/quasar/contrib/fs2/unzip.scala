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
import cats.effect.{Sync, ConcurrentEffect, ContextShift}
import cats.effect.Blocker

import fs2.{Stream, Pipe}
import fs2.io

object compression {
  // unzipP comes from https://gist.github.com/nmehitabel/a7c976ef8f0a41dfef88e981b9141075#file-fs2zip-scala-L18
  private def unzipP[F[_]](
      bec: Blocker,
      chunkSize: Int)(
      implicit F: ConcurrentEffect[F], 
      cs: ContextShift[F])
      : Pipe[F, Byte, (String, Stream[F, Byte])] = {

    def entry(zis: ZipInputStream): OptionT[F, (String, Stream[F, Byte])] =
      OptionT(Sync[F].delay(Option(zis.getNextEntry()))).map { ze =>
        (ze.getName, io.readInputStream[F](F.pure(zis), chunkSize, bec, closeAfterUse = false))
      }

    def unzipEntries(zis: ZipInputStream): Stream[F, (String, Stream[F, Byte])] =
      Stream.unfoldEval(zis) { zis0 =>
        entry(zis0).map((_, zis0)).value
      }

    value: Stream[F, Byte] =>
      value.through(io.toInputStream).flatMap { is: InputStream =>
        val zis: F[ZipInputStream]          = bec.delay(new ZipInputStream(is))
        val zres: Stream[F, ZipInputStream] = Stream.bracket(zis)(zis => bec.delay(zis.close()))
        zres.flatMap { z =>
          unzipEntries(z)
        }
      }
   }

   def unzip[F[_]](bec: Blocker, chunkSize: Int)(
       implicit F: ConcurrentEffect[F], 
        cs: ContextShift[F])
        : Pipe[F, Byte, Byte] = 
      unzipP[F](bec, chunkSize).andThen(_.flatMap(_._2))
}