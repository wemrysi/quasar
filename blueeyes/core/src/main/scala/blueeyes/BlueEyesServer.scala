package blueeyes

import blueeyes.core.data.ByteChunk
import blueeyes.core.service._
import blueeyes.core.service.engines.netty.NettyEngine
import org.streum.configrity.Configuration
import org.slf4s.Logging

/** Convenience trait for building a server. This server uses reflection to mix
  * in any services defined as fields.
  * <pre>
  * trait EmailServices extends BlueEyesServiceBuilder {
  *   val emailService = service("email", "1.32") {
  *     request {
  *       ...
  *     }
  *   }
  * }
  * object EmailServer extends BlueEyesServer with EmailServices
  * </pre>
  */
trait BlueEyesServer extends NettyEngine with ReflectiveServiceList[ByteChunk] with Logging with HttpServerMain { self =>
  class HttpServer(rootConfig: Configuration, ctx: ExecutionContext) extends NettyHttpServer(rootConfig, services, ctx) {
    implicit val executionContext = ctx
  }

  def server(rootConfig: Configuration, ctx: ExecutionContext) = new HttpServer(rootConfig, ctx)
}
