package ygg.tests

import ygg.table._

class TransSpecModuleSpec extends quasar.Qspec with TransSpecModule {
  import trans._
  import CPath._

  implicit def liftF1(f1: CF1): F1Like                              = ???
  implicit def liftF2(f2: CF2): F2Like                              = ???
  def lookupF1(namespace: List[String],name: String): CF1           = ???
  def lookupF2(namespace: List[String],name: String): ygg.table.CF2 = ???

  "concatChildren" should {
    "transform a CPathTree into a TransSpec" in {
      val tree: CPathTree[Int] = RootNode(
        Seq(
          FieldNode(
            CPathField("bar"),
            Seq(
              IndexNode(CPathIndex(0), Seq(LeafNode(4))),
              IndexNode(CPathIndex(1), Seq(FieldNode(CPathField("baz"), Seq(LeafNode(6))))),
              IndexNode(CPathIndex(2), Seq(LeafNode(2))))),
          FieldNode(CPathField("foo"), Seq(LeafNode(0)))))

      val result = TransSpec.concatChildren(tree)

      val expected = InnerObjectConcat(
        WrapObject(
          InnerArrayConcat(
            InnerArrayConcat(
              WrapArray(DerefArrayStatic(Leaf(Source), CPathIndex(4))),
              WrapArray(WrapObject(DerefArrayStatic(Leaf(Source), CPathIndex(6)), "baz"))),
            WrapArray(DerefArrayStatic(Leaf(Source), CPathIndex(2)))),
          "bar"),
        WrapObject(DerefArrayStatic(Leaf(Source), CPathIndex(0)), "foo"))

      result mustEqual expected
    }
  }
}
