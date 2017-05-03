package quasar.mimir

import scala.util.Random._

import quasar.blueeyes._
import quasar.precog.common._
import quasar.precog.util.IOUtils
import spire.implicits._

trait ClusteringTestSupport {

  case class GeneratedPointSet(points: Array[Array[Double]], centers: Array[Array[Double]])

  def genPoints(n: Int, dimension: Int, k: Int): GeneratedPointSet = {

    def genPoint(x: => Double): Array[Double] = Array.fill(dimension)(x)

    val s = math.pow(2 * k, 1.0 / dimension)
    val centers = (1 to k).map({ _ => genPoint(nextDouble * s) }).toArray
    val points = (1 to n).map({ _ =>
      val c = nextInt(k)
      genPoint(nextGaussian) + centers(c)
    }).toArray

    GeneratedPointSet(points, centers)
  }

  def pointToJson(p: Array[Double]): RValue = {
    RArray(p.toSeq map (CNum(_)): _*)
  }

  def pointsToJson(points: Array[Array[Double]]): List[RValue] = points.toList map (pointToJson(_))

  def writePointsToDataset[A](points: Array[Array[Double]])(f: String => A): A = {
    writeRValuesToDataset(pointsToJson(points))(f)
  }

  def writeRValuesToDataset[A](jvals: List[RValue])(f: String => A): A = {
    val lines = jvals map { _.toJValue.renderCompact }
    val tmpFile = java.io.File.createTempFile("values", ".json")
    IOUtils.writeSeqToFile(lines, tmpFile).unsafePerformIO
    val pointsString0 = "filesystem" + tmpFile.toString
    val pointsString = pointsString0.take(pointsString0.length - 5)
    val result = f(pointsString)
    tmpFile.delete()
    result
  }

}
