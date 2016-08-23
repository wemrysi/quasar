package ygg.tests

import scalaz._, Scalaz._
import ygg.table._
import ygg.json._

abstract class TableQspec         extends quasar.Qspec with TableModuleTestSupport
abstract class ColumnarTableQspec extends TableQspec with ColumnarTableModuleTestSupport

trait TableModuleTestSupport extends TableModule {
  def lookupF1(namespace: List[String], name: String): F1
  def lookupF2(namespace: List[String], name: String): F2
  def lookupScanner(namespace: List[String], name: String): Scanner
  def fromJson(data: Seq[JValue], maxBlockSize: Option[Int]): Table

  def toJson(dataset: Table): Need[Stream[JValue]]                         = dataset.toJson.map(_.toStream)

  def fromJson(data: Seq[JValue]): Table                    = fromJson(data, None)
  def fromJson(data: Seq[JValue], maxBlockSize: Int): Table = fromJson(data, Some(maxBlockSize))

  def fromSample(sampleData: SampleData): Table                            = fromJson(sampleData.data, None)
  def fromSample(sampleData: SampleData, maxBlockSize: Option[Int]): Table = fromJson(sampleData.data, maxBlockSize)
}

trait TableModuleSpec extends quasar.QuasarSpecification {
  import SampleData._

  def checkMappings(testSupport: TableModuleTestSupport) = {
    implicit val gen = sample(schema)
    prop { (sample: SampleData) =>
      val dataset = testSupport.fromSample(sample)
      testSupport.toJson(dataset).copoint.toSet must_== sample.data.toSet
    }
  }
}
