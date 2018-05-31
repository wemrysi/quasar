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

package quasar.contrib.fs2

import slamdata.Predef.{Boolean, None, Option, Some, Unit}

import java.util.Iterator
import java.util.stream.{Stream => JStream}

import fs2.{async, Chunk, Stream}
import fs2.interop.scalaz._
import fs2.util.{Async, Attempt, Suspendable}
import scalaz.{Functor, StreamT}
import scalaz.stream.Process
import scalaz.std.option._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._

object convert {

  // java.util.stream.Stream

  def fromJavaStream[F[_], A](js: F[JStream[A]])(implicit F: Suspendable[F]): Stream[F, A] = {
    def getNext(i: Iterator[A]): F[Option[(A, Iterator[A])]] =
      F.delay(i.hasNext).ifM(
        F.delay(Some((i.next(), i))),
        F.pure(None))

    Stream.bracket(js)(
      s => Stream.unfoldEval(s.iterator)(getNext),
      s => F.delay(s.close()))
  }


  // scalaz.StreamT

  def fromChunkedStreamT[F[_]: Functor, A](chunks: StreamT[F, Chunk[A]]): Stream[F, A] =
    Stream.unfoldChunkEval(chunks)(_.step.map(_(
      yieldd = (c, st) => Some((c, st))
    , skip = st => Some((Chunk.empty, st))
    , done = None)))

  def fromStreamT[F[_]: Functor, A](st: StreamT[F, A]): Stream[F, A] =
    fromChunkedStreamT(st.map(a => Chunk.singleton(a): Chunk[A]))

  // NB: This will potentially leak resources if the resulting `StreamT` is
  //     not fully consumed!
  def toStreamT[F[_]: Async, A](s: Stream[F, A]): StreamT[F, A] =
    StreamT.wrapEffect(chunkQ(s) map {
      case (q, _) =>
        for {
          c <- StreamT.unfoldM(q)(_.dequeue1.map(_.flatMap(_.toOption).strengthR(q)))
          a <- StreamT.unfoldM(c)(_.uncons.point[F])
        } yield a
    })


  // scalaz.Process

  def toProcess[F[_]: Async, A](s: Stream[F, A]): Process[F, A] =
    Process.bracket(chunkQ(s))(t => Process.eval_(t._2)) {
      case (q, _) =>
        Process.await(q.dequeue1)(_.cata(
          a => a.fold(Process.fail, c => Process.emitAll(c.toVector)),
          Process.halt))
    }

  ////

  private def chunkQ[F[_], A](s: Stream[F, A])(implicit F: Async[F])
      : F[(async.mutable.Queue[F, Option[Attempt[Chunk[A]]]], F[Unit])] =
    for {
      q <- async.boundedQueue[F, Option[Attempt[Chunk[A]]]](1)
      i <- async.signalOf[F, Boolean](false)

      enqueue =
        s.chunks.attempt.noneTerminate
          .to(q.enqueue)
          .interruptWhen(i)
          .onError(_ => Stream.eval(q.enqueue1(None)))

      _ <- F.start(enqueue.run)
    } yield (q, i.set(true))
}
