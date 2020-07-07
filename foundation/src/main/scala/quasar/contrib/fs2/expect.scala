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

import fs2.{Pipe, Pull, Stream}

object expect {

  def size[F[_], A](count: Long)(orElse: Long => Stream[F, A]): Pipe[F, A, A] = { in =>
    def loop(in: Stream[F, A], acc: Long): Pull[F, A, Unit] =
      in.pull.uncons flatMap {
        case Some((chunk, tail)) =>
          Pull.output(chunk) >> loop(tail, acc + chunk.size)

        case None =>
          if (acc != count)
            orElse(acc).pull.echo
          else
            Pull.done
      }

    loop(in, 0L).stream
  }
}
