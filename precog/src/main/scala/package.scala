package quasar

import java.nio.file._
import java.math.MathContext.UNLIMITED

package object precog {
  val Try          = scala.util.Try
  type Try[+A]     = scala.util.Try[A]
  val ScalaFailure = scala.util.Failure

  type jPath       = java.nio.file.Path
  type =?>[-A, +B] = scala.PartialFunction[A, B]
  type CTag[A]     = scala.reflect.ClassTag[A]
  type jClass      = java.lang.Class[_]

  type jConcurrentMap[K, V] = java.util.concurrent.ConcurrentMap[K, V]

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
