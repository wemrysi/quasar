/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.marklogic.xcc

import quasar.Predef._
import quasar.Data
import quasar.fp._
import quasar.fp.numeric.Positive
import quasar.fp.free.lift
import quasar.fs.DataCursor

import com.marklogic.xcc.{ResultItem, ResultSequence}
import com.marklogic.xcc.types.XdmItem
import com.marklogic.xcc.exceptions.StreamingResultException
import scalaz._, Scalaz._
import scalaz.stream.Process
import scalaz.concurrent.Task

final class ChunkedResultSequence[S[_]](val chunkSize: Positive, rs: ResultSequence) {
  import XccError.streamingError

  def close(implicit S: Task :<: S): Free[S, Executed] =
    lift(Task.delay(rs.close()).as(Executed.executed)).into[S]

  def nextChunk(implicit S0: Task :<: S, S1: XccFailure :<: S): Free[S, Vector[XdmItem]] = {
    val chunk = Process.unfoldEval(())(_ => next.map(_ strengthR (())))
                  .take(chunkSize.get.toInt)
                  .runLog

    XccFailure.Ops[S].unattempt(lift(chunk.run).into[S])
  }

  ////

  private def next: EitherT[Task, XccError, Option[XdmItem]] =
    nextItem.liftM[EitherT[?[_], XccError, ?]] >>= (_ traverse cachedItem)

  private def cachedItem(ritem: ResultItem): EitherT[Task, XccError, XdmItem] =
    EitherT(Task.delay {
      ritem.cache()
      ritem.getItem.right[XccError]
    } handle {
      case ex: StreamingResultException => streamingError(ritem, ex).left
    })

  private def nextItem: Task[Option[ResultItem]] =
    Task.delay(if (rs.hasNext) Some(rs.next) else None)
}

object ChunkedResultSequence {
  implicit def dataCursor[S[_]](implicit S0: Task :<: S, S1: XccFailure :<: S): DataCursor[Free[S, ?], ChunkedResultSequence[S]] =
    new DataCursor[Free[S, ?], ChunkedResultSequence[S]] {
      def close(crs: ChunkedResultSequence[S]) =
        crs.close.void

      def nextChunk(crs: ChunkedResultSequence[S]) =
        crs.nextChunk.map(_.foldLeft(Vector[Data]())((ds, x) =>
          ds :+ xdmitem.toData(x)))
    }
}
