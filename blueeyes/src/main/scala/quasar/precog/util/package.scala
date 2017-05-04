package quasar.precog

import quasar.blueeyes._
import java.util.Comparator
import scala.collection.JavaConverters._
import java.util.concurrent.atomic.AtomicInteger
import scalaz._, Scalaz._

package object util {
  /**
    * Opaque symbolic identifier (like Int, but better!).
    */
  final class Identifier extends AnyRef

  // Shared Int could easily overflow: Unshare? Extend to a Long? Different approach?
  object IdGen extends IdGen
  class IdGen {
    private[this] val currentId = new AtomicInteger(0)
    def nextInt(): Int = currentId.getAndIncrement()
  }

}
