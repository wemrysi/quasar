package ygg.tests

import ygg.cf
import ygg.common._
import ygg.table._
import scalaz._
import scalaz.syntax.std.boolean._
import ygg.json._
import ygg.data._

trait ColumnarTableModuleTestSupport extends ColumnarTableModule with TableModuleTestSupport {
  def newGroupId: GroupId

  private def makeSlice(sampleData: Stream[JValue], sliceSize: Int): (Slice, Stream[JValue]) = {
    @tailrec def buildColArrays(from: Stream[JValue], into: Map[ColumnRef, ArrayColumn[_]], sliceIndex: Int): (Map[ColumnRef, ArrayColumn[_]], Int) = {
      from match {
        case jv #:: xs =>
          val refs = Slice.withIdsAndValues(jv, into, sliceIndex, sliceSize)
          buildColArrays(xs, refs, sliceIndex + 1)
        case _ =>
          (into, sliceIndex)
      }
    }

    val (prefix, suffix) = sampleData.splitAt(sliceSize)
    val slice            = Slice(buildColArrays(prefix.toStream, Map.empty[ColumnRef, ArrayColumn[_]], 0))

    (slice, suffix)
  }

  // production-path code uses fromRValues, but all the tests use fromJson
  // this will need to be changed when our tests support non-json such as CDate and CPeriod
  def fromJson0(values: Stream[JValue], sliceSize: Int): Table = {
    Table(
      StreamT.unfoldM(values) { events =>
        Need {
          (!events.isEmpty) option {
            makeSlice(events.toStream, sliceSize)
          }
        }
      },
      ExactSize(values.length)
    )
  }

  def fromJson(values: Seq[JValue], maxSliceSize: Option[Int]): Table =
    fromJson0(values.toStream, maxSliceSize getOrElse yggConfig.maxSliceSize)

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
}
