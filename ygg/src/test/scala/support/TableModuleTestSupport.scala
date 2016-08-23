package ygg.tests

import scalaz._, Scalaz._
import ygg._, common._, data._, json._, table._

abstract class TableQspec         extends quasar.Qspec with TableModuleTestSupport
abstract class ColumnarTableQspec extends TableQspec with ColumnarTableModuleTestSupport

trait TableModuleTestSupport extends TableModule {
  def lookupF1(namespace: List[String], name: String): F1 = {
    val lib = Map[String, CF1](
      "negate"         -> cf.math.Negate,
      "coerceToDouble" -> cf.CoerceToDouble,
      "true" -> CF1("testing::true") { _ =>
        Some(Column.const(true))
      }
    )

    lib(name)
  }

  def lookupF2(namespace: List[String], name: String): F2 = {
    val lib = Map[String, CF2](
      "add" -> cf.math.Add,
      "mod" -> cf.math.Mod,
      "eq"  -> cf.std.Eq
    )
    lib(name)
  }

  def lookupScanner(namespace: List[String], name: String): Scanner = {
    val lib = Map[String, Scanner](
      "sum" -> new Scanner {
        type A = BigDecimal
        val init = BigDecimal(0)
        def scan(a: BigDecimal, cols: Map[ColumnRef, Column], range: Range): (A, Map[ColumnRef, Column]) = {
          val identityPath = cols collect { case c @ (ColumnRef.id(_), _) => c }
          val prioritized = identityPath.values filter {
            case (_: LongColumn | _: DoubleColumn | _: NumColumn) => true
            case _                                                => false
          }

          val mask = Bits.filteredRange(range.start, range.end) { i =>
            prioritized exists { _ isDefinedAt i }
          }

          val (a2, arr) = mask.toList.foldLeft((a, new Array[BigDecimal](range.end))) {
            case ((acc, arr), i) => {
              val col = prioritized find { _ isDefinedAt i }

              val acc2 = col map {
                case lc: LongColumn   => acc + lc(i)
                case dc: DoubleColumn => acc + dc(i)
                case nc: NumColumn    => acc + nc(i)
                case _                => abort("unreachable")
              }

              acc2 foreach { arr(i) = _ }

              (acc2 getOrElse acc, arr)
            }
          }

          (a2, Map(ColumnRef.id(CNum) -> ArrayNumColumn(mask, arr)))
        }
      }
    )

    lib(name)
  }

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
