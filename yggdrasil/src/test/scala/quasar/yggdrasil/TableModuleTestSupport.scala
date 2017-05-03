package quasar.yggdrasil

import quasar.blueeyes._, json._
import scalaz._, Scalaz._
import quasar.precog.TestSupport._

trait TestLib[M[+_]] extends TableModule[M] {
  def lookupF1(namespace: List[String], name: String): F1
  def lookupF2(namespace: List[String], name: String): F2
  def lookupScanner(namespace: List[String], name: String): Scanner
}

trait TableModuleTestSupport[M[+_]] extends TableModule[M] with TestLib[M] {
  implicit def M: Monad[M] with Comonad[M]

  def fromJson(data: Stream[JValue], maxBlockSize: Option[Int] = None): Table
  def toJson(dataset: Table): M[Stream[JValue]] = dataset.toJson.map(_.toStream)

  def fromSample(sampleData: SampleData, maxBlockSize: Option[Int] = None): Table = fromJson(sampleData.data, maxBlockSize)
}

trait TableModuleSpec[M[+_]] extends SpecificationLike with ScalaCheck {
  import SampleData._

  implicit def M: Monad[M] with Comonad[M]

  def checkMappings(testSupport: TableModuleTestSupport[M]) = {
    implicit val gen = sample(schema)
    prop { (sample: SampleData) =>
      val dataset = testSupport.fromSample(sample)
      testSupport.toJson(dataset).copoint.toSet must_== sample.data.toSet
    }
  }
}
