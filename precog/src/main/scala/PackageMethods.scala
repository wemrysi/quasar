package quasar
package precog

import java.nio.file._
import java.math.MathContext.UNLIMITED
import scala.collection.{ mutable => scm, immutable => sci }

trait PackageMethods {
  self: PackageAliases =>

  def scmSet[A](): scmSet[A]                                    = scm.HashSet[A]()
  def sciQueue[A](): sciQueue[A]                                = sci.Queue[A]()
  def sciTreeMap[K: Ordering, V](xs: (K, V)*): sciTreeMap[K, V] = sci.TreeMap[K, V](xs: _*)

  def warn[A](msg: String)(value: => A): A = {
    java.lang.System.err.println(msg)
    value
  }
  def ctag[A](implicit z: CTag[A]): CTag[A] = z
  def jclass[A: CTag]: jClass               = ctag[A].runtimeClass.asInstanceOf[jClass]

  def jPath(path: String): jPath = Paths get path

  implicit class jPathOps(private val p: jPath) {
    def slurpBytes(): Array[Byte] = Files readAllBytes p
  }


  def decimal(d: java.math.BigDecimal): BigDecimal         = new BigDecimal(d, UNLIMITED)
  def decimal(d: String): BigDecimal                       = BigDecimal(d, UNLIMITED)
  def decimal(d: Int): BigDecimal                          = decimal(d.toLong)
  def decimal(d: Long): BigDecimal                         = BigDecimal.decimal(d, UNLIMITED)
  def decimal(d: Double): BigDecimal                       = BigDecimal.decimal(d, UNLIMITED)
  def decimal(d: Float): BigDecimal                        = BigDecimal.decimal(d, UNLIMITED)
  def decimal(unscaledVal: BigInt, scale: Int): BigDecimal = BigDecimal(unscaledVal, scale, UNLIMITED)
}
