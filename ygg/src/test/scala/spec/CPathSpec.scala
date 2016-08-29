package ygg.tests

import ygg._, common._, table._

class CPathSpec extends quasar.Qspec {
  import CPath._

  "makeTree" should {
    "return correct tree given a sequence of CPath" in {
      val cpaths = Vec[CPath](
        "foo",
        CPath("bar", 0),
        CPath("bar", 1, "baz"),
        CPath("bar", 1, "ack"),
        CPath("bar", 2)
      )

      val values: Vec[Int] = Vec(4, 6, 7, 2, 0)
      val result           = makeTree(cpaths, values)

      val expected: CPathTree[Int] = {
        RootNode(
          Seq(
            FieldNode(
              CPathField("bar"),
              Seq(
                IndexNode(CPathIndex(0), Vec(LeafNode(4))),
                IndexNode(CPathIndex(1), Vec(FieldNode(CPathField("ack"), Seq(LeafNode(6))), FieldNode(CPathField("baz"), Seq(LeafNode(7))))),
                IndexNode(CPathIndex(2), Vec(LeafNode(2))))),
            FieldNode(CPathField("foo"), Vec(LeafNode(0)))))
      }

      result mustEqual expected
    }
  }
}
