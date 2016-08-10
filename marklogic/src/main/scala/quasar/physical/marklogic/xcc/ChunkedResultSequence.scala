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
import quasar.fp.numeric.Positive
import quasar.fs.DataCursor

import com.marklogic.xcc.ResultSequence
import com.marklogic.xcc.types.XdmItem
import scalaz.syntax.functor._
import scalaz.std.option._
import scalaz.stream.Process
import scalaz.concurrent.Task

final class ChunkedResultSequence(val chunkSize: Positive, rs: ResultSequence) {
  val close: Task[Executed] =
    Task.delay(rs.close()).as(Executed.executed)

  val nextChunk: Task[Vector[XdmItem]] =
    Process.unfoldEval(())(_ => next.map(_ strengthR (())))
      .take(chunkSize.get.toInt)
      .runLog

  // TODO: What kind of errors can happen when cache()-ing an item?
  private val next: Task[Option[XdmItem]] =
    Task delay {
      if (rs.hasNext) {
        val ritem = rs.next
        ritem.cache()
        Some(ritem.getItem)
      } else None
    }
}

object ChunkedResultSequence {
  implicit val dataCursor: DataCursor[Task, ChunkedResultSequence] =
    new DataCursor[Task, ChunkedResultSequence] {
      def close(crs: ChunkedResultSequence) =
        crs.close.void

      // TODO: Better to just handle this in the stream?
      def nextChunk(crs: ChunkedResultSequence) =
        crs.nextChunk.map(_.foldLeft(Vector[Data]())((ds, x) =>
          xdmitem.toData(x).fold(ds)(ds :+ _)))
    }
}
