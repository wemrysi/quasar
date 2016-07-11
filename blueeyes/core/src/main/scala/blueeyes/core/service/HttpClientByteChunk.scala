package blueeyes
package core.service

import blueeyes.core.data._
import blueeyes.core.http.{ HttpResponse, HttpRequest }
import blueeyes.util.metrics.DataSize

trait HttpClientByteChunk extends HttpClient[ByteChunk] { self =>
  implicit val executor: ExecutionContext

  def aggregate(chunkSize: DataSize) = new HttpClient[ByteChunk] {
    def isDefinedAt(request: HttpRequest[ByteChunk]) = self.isDefinedAt(request)

    def apply(request: HttpRequest[ByteChunk]) = {
      self(request) map { response =>
        response.copy(content = response.content.map(ByteChunk.aggregate(_, chunkSize.bytes.size.toInt)))
      }
    }
  }
}
