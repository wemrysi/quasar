package ygg.tests

import scalaz._, Scalaz._
import ygg._, common._, data._, json._, table._
import TableModule._

abstract class TableQspec         extends quasar.Qspec with TableModuleTestSupport
abstract class ColumnarTableQspec extends TableQspec with ColumnarTableModuleTestSupport {
  import trans._

  class Table(slices: StreamT[Need, Slice], size: TableSize) extends ColumnarTable(slices, size) {
    import trans._

    def load(apiKey: APIKey, jtpe: JType)                                                                                           = ???
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean)                                                     = Need(this)
    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean): Need[Seq[Table]] = ???
  }

  trait TableCompanion extends ColumnarTableCompanion {
    def apply(slices: StreamT[Need, Slice], size: TableSize) = new Table(slices, size)

    def singleton(slice: Slice) = new Table(slice :: StreamT.empty[Need, Slice], ExactSize(1))

    def align(sourceLeft: Table, alignOnL: TransSpec1, sourceRight: Table, alignOnR: TransSpec1): Need[Table -> Table] =
      abort("not implemented here")
  }

  object Table extends TableCompanion

  def testConcat = {
    val data1: Stream[JValue] = Stream.fill(25)(json"""{ "a": 1, "b": "x", "c": null }""")
    val data2: Stream[JValue] = Stream.fill(35)(json"""[4, "foo", null, true]""")

    val table1   = fromSample(SampleData(data1), Some(10))
    val table2   = fromSample(SampleData(data2), Some(10))
    val results  = toJson(table1.concat(table2))
    val expected = data1 ++ data2

    results.copoint must_== expected
  }

  def streamToString(stream: StreamT[Need, CharBuffer]): String = {
    def loop(stream: StreamT[Need, CharBuffer], sb: StringBuilder): Need[String] =
      stream.uncons.flatMap {
        case None =>
          Need(sb.toString)
        case Some((cb, tail)) =>
          sb.append(cb)
          loop(tail, sb)
      }
    loop(stream, new StringBuilder).copoint
  }


  def testRenderCsv(json: String, maxSliceSize: Option[Int] = None): String = {
    val es    = JParser.parseManyFromString(json).valueOr(throw _)
    val table = fromJson(es.toStream, maxSliceSize)
    streamToString(table.renderCsv())
  }

  def testRenderJson(xs: JValue*) = {
    val seq = xs.toVector
    def minimizeItem(t: (String, JValue)) = minimize(t._2).map((t._1, _))

    def minimize(value: JValue): Option[JValue] = value match {
      case JUndefined       => None
      case JObject(fields)  => Some(JObject(fields.flatMap(minimizeItem)))
      case JArray(Seq())    => Some(jarray())
      case JArray(elements) => elements flatMap minimize match { case Seq() => None ; case xs => Some(JArray(xs)) }
      case v                => Some(v)
    }

    val table     = fromJson(seq.toStream)
    val expected  = JArray(seq.toVector)
    val arrayM    = table.renderJson("[", ",", "]").foldLeft("")(_ + _.toString).map(JParser.parseUnsafe)
    val minimized = minimize(expected) getOrElse jarray()

    arrayM.copoint mustEqual minimized
  }

  def sanitize(s: String): String = s.toArray.map(c => if (c < ' ') ' ' else c).mkString("")
  def undef: JValue = JUndefined

  def renderLotsToCsv(lots: Int, maxSliceSize: Option[Int] = None) = {
    val event    = "{\"x\":123,\"y\":\"foobar\",\"z\":{\"xx\":1.0,\"yy\":2.0}}"
    val events   = event * lots
    val csv      = testRenderCsv(events, maxSliceSize)
    val expected = ".x,.y,.z.xx,.z.yy\r\n" + ("123,foobar,1,2\r\n" * lots)
    csv must_=== expected
  }
}

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
