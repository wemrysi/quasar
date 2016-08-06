package quasar

import scalaz._
import scala.collection.mutable
import java.nio.file._

/** For some reason extending ScodecImplicits makes sbt recompile
 *  everything under the sun even if we never touch it.
 */
package object precog /*extends ScodecImplicits*/ {
  val Try          = scala.util.Try
  type Try[+A]     = scala.util.Try[A]
  type jPath       = java.nio.file.Path
  type =?>[-A, +B] = scala.PartialFunction[A, B]
  type CTag[A]     = scala.reflect.ClassTag[A]
  type jClass      = java.lang.Class[_]

  def ctag[A](implicit z: CTag[A]): CTag[A] = z
  def jclass[A: CTag]: jClass               = ctag[A].runtimeClass.asInstanceOf[jClass]

  def jPath(path: String): jPath = Paths get path

  implicit class jPathOps(private val p: jPath) {
    def slurpBytes(): Array[Byte] = Files readAllBytes p
  }
}
