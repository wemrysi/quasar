package ygg.tests

import blueeyes._
import scalaz._, Scalaz._
import ygg.table._
import ygg.json._

trait PartitionMergeSpec extends ColumnarTableQspec {
  import trans._

  def testPartitionMerge = {
    val JArray(elements) = JParser.parseUnsafe("""[
      { "key": [0], "value": { "a": "0a" } },
      { "key": [1], "value": { "a": "1a" } },
      { "key": [1], "value": { "a": "1b" } },
      { "key": [1], "value": { "a": "1c" } },
      { "key": [2], "value": { "a": "2a" } },
      { "key": [3], "value": { "a": "3a" } },
      { "key": [3], "value": { "a": "3b" } },
      { "key": [4], "value": { "a": "4a" } }
    ]""")

    val tbl = fromJson(elements.toStream)

    val JArray(expected) = JParser.parseUnsafe("""[
      "0a",
      "1a;1b;1c",
      "2a",
      "3a;3b",
      "4a"
    ]""")

    val result: Need[Table] = tbl.partitionMerge(DerefObjectStatic(Leaf(Source), CPathField("key"))) { table =>
      val reducer = new Reducer[String] {
        def reduce(schema: CSchema, range: Range): String = {
          schema.columns(JTextT).head match {
            case col: StrColumn => range map col mkString ";"
            case _              => abort("Not a StrColumn")
          }
        }
      }

      val derefed = table.transform(DerefObjectStatic(DerefObjectStatic(Leaf(Source), CPathField("value")), CPathField("a")))

      derefed.reduce(reducer).map(s => Table.constString(Set(s)))
    }

    result.flatMap(_.toJson).copoint must_== expected.toStream
  }

}
