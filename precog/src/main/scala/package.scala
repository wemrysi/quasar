package quasar

import scalaz._
import scala.collection.mutable

/** For some reason extending ScodecImplicits makes sbt recompile
 *  everything under the sun even if we never touch it.
 */
package object precog /*extends ScodecImplicits*/ {
}
