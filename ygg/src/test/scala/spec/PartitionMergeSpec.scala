package ygg.tests

import scalaz._, Scalaz._
import ygg.common._
import ygg.table._
import ygg.json._

trait PartitionMergeSpec extends ColumnarTableQspec {
  val trans: TransSpecClasses
  import trans._

  private object reducer extends Reducer[String] {
    def reduce(schema: CSchema, range: Range): String = {
      schema.columns(JTextT).head match {
        case col: StrColumn => range map col mkString ";"
        case _              => abort("Not a StrColumn")
      }
    }
  }

  def testPartitionMerge = {
    val data = jsonMany"""
      { "key": [0], "value": { "a": "0a" } }
      { "key": [1], "value": { "a": "1a" } }
      { "key": [1], "value": { "a": "1b" } }
      { "key": [1], "value": { "a": "1c" } }
      { "key": [2], "value": { "a": "2a" } }
      { "key": [3], "value": { "a": "3a" } }
      { "key": [3], "value": { "a": "3b" } }
      { "key": [4], "value": { "a": "4a" } }
    """
    val expected = jsonMany"""
      "0a"
      "1a;1b;1c"
      "2a"
      "3a;3b"
      "4a"
    """
    val tbl    = fromJson(data)
    val result = tbl.partitionMerge(root.key)(table => table transform root.value.a  reduce reducer map (s => Table constString Set(s)))

    result.flatMap(_.toJson).copoint must_=== expected.toStream
  }
}
