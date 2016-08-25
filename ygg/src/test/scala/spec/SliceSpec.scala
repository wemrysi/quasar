package ygg.tests

import ygg.common._
import ygg.table._
import TableTestSupport._

class SliceSpec extends quasar.Qspec {
  private def extractCValues(colGroups: List[List[Column]], row: Int): List[CValue] = {
    colGroups map { _ find (_.isDefinedAt(row)) map (_.cValue(row)) getOrElse CUndefined }
  }

  private def columnsByCPath(slice: Slice): Map[CPath, List[Column]] = {
    val byCPath = slice.columns.groupBy(_._1.selector)
    byCPath.mapValues(_.map(_._2).toList)
  }

  private def sortableCValues(slice: Slice, cpaths: Vector[CPath]): List[List[CValue] -> List[CValue]] = {
    val byCPath = columnsByCPath(slice)
    (0 until slice.size).map({ row =>
      (extractCValues(cpaths.map(byCPath).toList, row), extractCValues(byCPath.values.toList, row))
    })(collection.breakOut)
  }

  private def toCValues(slice: Slice) = sortableCValues(slice, Vector.empty) map (_._2)

  private def fakeConcat(slices: List[Slice]) = {
    slices.foldLeft(List.empty[List[CValue]]) { (acc, slice) =>
      acc ++ toCValues(slice)
    }
  }

  private def stripUndefineds(cvals: List[CValue]): Set[CValue] =
    (cvals filter (_ != CUndefined)).toSet

  private def concatProjDesc = Seq(
    ColumnRef(CPath("0"), CLong),
    ColumnRef(CPath("1"), CBoolean),
    ColumnRef(CPath("2"), CString),
    ColumnRef(CPath("3"), CDouble),
    ColumnRef(CPath("4"), CNum),
    ColumnRef(CPath("5"), CEmptyObject),
    ColumnRef(CPath("6"), CEmptyArray),
    ColumnRef(CPath("7"), CNum)
  )

  "concat" should {
    "concat arbitrary slices together" in {
      implicit def arbSlice = Arbitrary(genSlice(concatProjDesc, 23))

      prop { slices: List[Slice] =>
        val slice = Slice.concat(slices)
        toCValues(slice) must_== fakeConcat(slices)
      }
    }

    "concat small singleton together" in {
      implicit def arbSlice = Arbitrary(genSlice(concatProjDesc, 1))

      prop { slices: List[Slice] =>
        val slice = Slice.concat(slices)
        toCValues(slice) must_== fakeConcat(slices)
      }
    }

    "concat empty slices correctly" in {
      implicit def arbSlice = Arbitrary(genSlice(concatProjDesc, 23))

      prop { fullSlices: List[Slice] =>
        val slices = fullSlices map (s => if (randomBool) s else Slice.empty)
        val slice  = Slice.concat(slices)

        toCValues(slice) must_== fakeConcat(slices)
      }
    }

    "concat heterogeneous slices" in {
      val pds               = List.fill(25)(concatProjDesc filter (_ => randomBool))
      val g1 :: g2 :: gs    = pds.map(genSlice(_, 17))
      implicit val arbSlice = Arbitrary(Gen.oneOf(g1, g2, gs: _*))

      prop { slices: List[Slice] =>
        val slice = Slice.concat(slices)
        // This is terrible, but there isn't an immediately easy way to test
        // without duplicating concat.
        toCValues(slice).map(stripUndefineds) must_== fakeConcat(slices).map(stripUndefineds)
      }
    }
  }
}
