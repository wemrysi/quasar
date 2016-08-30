package ygg.tests

import ygg._, common._, json._, table._
import trans._
import CPath._

class ConcatSpec extends ColumnarTableQspec {
  "in concat" >> {
    "concat two tables" in testConcat
    "transform a CPathTree into a TransSpec" in testTreeToTransSpec
  }

  private def testConcat = {
    val data1: Stream[JValue] = Stream.fill(25)(json"""{ "a": 1, "b": "x", "c": null }""")
    val data2: Stream[JValue] = Stream.fill(35)(json"""[4, "foo", null, true]""")

    val table1   = fromSample(SampleData(data1), Some(10))
    val table2   = fromSample(SampleData(data2), Some(10))
    val results  = toJson(table1.concat(table2))
    val expected = data1 ++ data2

    results.value must_=== expected
  }

  private def testTreeToTransSpec = {
    val tree: CPathTree[Int] = RootNode(
      Seq(
        FieldNode(
          CPathField("bar"),
          Seq(
            IndexNode(CPathIndex(0), Seq(LeafNode(4))),
            IndexNode(CPathIndex(1), Seq(FieldNode(CPathField("baz"), Seq(LeafNode(6))))),
            IndexNode(CPathIndex(2), Seq(LeafNode(2))))),
        FieldNode(CPathField("foo"), Seq(LeafNode(0)))))

    val result = TransSpec.concatChildren(tree, Leaf(Source))

    val expected = InnerObjectConcat(
      WrapObject(
        InnerArrayConcat(
          InnerArrayConcat(
            WrapArray(root(4)),
            WrapArray(WrapObject(root(6), "baz"))
          ),
          WrapArray(root(2))
        ),
        "bar"
      ),
      WrapObject(root(0), "foo")
    )

    result mustEqual expected
  }
}
