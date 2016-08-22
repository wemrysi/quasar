package ygg

import ygg.common._
import ygg.data._
import ygg.table._

package object cf {
  /**
    * Right-biased column union
    */
  val UnionRight = CF2P("builtin::ct::unionRight") {
    case (c1: BoolColumn, c2: BoolColumn) =>
      new UnionColumn(c1, c2) with BoolColumn {
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else abort("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: LongColumn, c2: LongColumn) =>
      new UnionColumn(c1, c2) with LongColumn {
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else abort("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: DoubleColumn, c2: DoubleColumn) =>
      new UnionColumn(c1, c2) with DoubleColumn {
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else abort("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: NumColumn, c2: NumColumn) =>
      new UnionColumn(c1, c2) with NumColumn {
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else abort("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: StrColumn, c2: StrColumn) =>
      new UnionColumn(c1, c2) with StrColumn {
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else abort("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: DateColumn, c2: DateColumn) =>
      new UnionColumn(c1, c2) with DateColumn {
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else abort("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: PeriodColumn, c2: PeriodColumn) =>
      new UnionColumn(c1, c2) with PeriodColumn {
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else abort("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: HomogeneousArrayColumn[a], _c2: HomogeneousArrayColumn[_]) if c1.tpe == _c2.tpe =>
      val c2 = _c2.asInstanceOf[HomogeneousArrayColumn[a]]
      new UnionColumn(c1, c2) with HomogeneousArrayColumn[a] {
        val tpe = c1.tpe
        def apply(row: Int) = {
          if (c2.isDefinedAt(row)) c2(row) else if (c1.isDefinedAt(row)) c1(row) else abort("Attempt to retrieve undefined value for row: " + row)
        }
      }

    case (c1: EmptyArrayColumn, c2: EmptyArrayColumn)   => new UnionColumn(c1, c2) with EmptyArrayColumn
    case (c1: EmptyObjectColumn, c2: EmptyObjectColumn) => new UnionColumn(c1, c2) with EmptyObjectColumn
    case (c1: NullColumn, c2: NullColumn)               => new UnionColumn(c1, c2) with NullColumn
  }

  val isSatisfied = CF1P("builtin::ct::isSatisfied") {
    case c: BoolColumn =>
      new BoolColumn {
        def isDefinedAt(row: Int) = c.isDefinedAt(row) && c(row)
        def apply(row: Int)       = isDefinedAt(row)
      }
  }

  val Empty = CF1P("builtin::ct::empty") {
    case c: BoolColumn   => new EmptyColumn[BoolColumn] with BoolColumn
    case c: LongColumn   => new EmptyColumn[LongColumn] with LongColumn
    case c: DoubleColumn => new EmptyColumn[DoubleColumn] with DoubleColumn
    case c: NumColumn    => new EmptyColumn[NumColumn] with NumColumn
    case c: StrColumn    => new EmptyColumn[StrColumn] with StrColumn
    case c: DateColumn   => new EmptyColumn[DateColumn] with DateColumn
    case c: PeriodColumn => new EmptyColumn[PeriodColumn] with PeriodColumn
    case c: HomogeneousArrayColumn[a] =>
      new EmptyColumn[HomogeneousArrayColumn[a]] with HomogeneousArrayColumn[a] {
        val tpe = c.tpe
      }
    case c: EmptyArrayColumn  => new EmptyColumn[EmptyArrayColumn] with EmptyArrayColumn
    case c: EmptyObjectColumn => new EmptyColumn[EmptyObjectColumn] with EmptyObjectColumn
    case c: NullColumn        => new EmptyColumn[NullColumn] with NullColumn
  }

  //it would be nice to generalize these to `CoerceTo[A]`
  def CoerceToDouble = CF1P("builtin:ct:coerceToDouble") {
    case (c: DoubleColumn) => c

    case (c: LongColumn) =>
      new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = c(row).toDouble
      }

    case (c: NumColumn) =>
      new Map1Column(c) with DoubleColumn {
        def apply(row: Int) = c(row).toDouble
      }
  }

  def Sparsen(idx: Array[Int], toSize: Int) = CF1P("builtin::ct::sparsen") {
    case c: BoolColumn   => new SparsenColumn(c, idx, toSize) with BoolColumn   { def apply(row: Int) = c(remap(row)) }
    case c: LongColumn   => new SparsenColumn(c, idx, toSize) with LongColumn   { def apply(row: Int) = c(remap(row)) }
    case c: DoubleColumn => new SparsenColumn(c, idx, toSize) with DoubleColumn { def apply(row: Int) = c(remap(row)) }
    case c: NumColumn    => new SparsenColumn(c, idx, toSize) with NumColumn    { def apply(row: Int) = c(remap(row)) }
    case c: StrColumn    => new SparsenColumn(c, idx, toSize) with StrColumn    { def apply(row: Int) = c(remap(row)) }
    case c: DateColumn   => new SparsenColumn(c, idx, toSize) with DateColumn   { def apply(row: Int) = c(remap(row)) }
    case c: PeriodColumn => new SparsenColumn(c, idx, toSize) with PeriodColumn { def apply(row: Int) = c(remap(row)) }
    case c: HomogeneousArrayColumn[a] =>
      new SparsenColumn(c, idx, toSize) with HomogeneousArrayColumn[a] {
        val tpe             = c.tpe
        def apply(row: Int) = c(remap(row))
      }

    case c: EmptyArrayColumn  => new SparsenColumn(c, idx, toSize) with EmptyArrayColumn
    case c: EmptyObjectColumn => new SparsenColumn(c, idx, toSize) with EmptyObjectColumn
    case c: NullColumn        => new SparsenColumn(c, idx, toSize) with NullColumn
  }


  def Remap(f: Int => Int) = CF1P("builtin::ct::remap") {
    case c: BoolColumn   => new RemapColumn(c, f) with BoolColumn   { def apply(row: Int) = c(f(row)) }
    case c: LongColumn   => new RemapColumn(c, f) with LongColumn   { def apply(row: Int) = c(f(row)) }
    case c: DoubleColumn => new RemapColumn(c, f) with DoubleColumn { def apply(row: Int) = c(f(row)) }
    case c: NumColumn    => new RemapColumn(c, f) with NumColumn    { def apply(row: Int) = c(f(row)) }
    case c: StrColumn    => new RemapColumn(c, f) with StrColumn    { def apply(row: Int) = c(f(row)) }
    case c: DateColumn   => new RemapColumn(c, f) with DateColumn   { def apply(row: Int) = c(f(row)) }
    case c: PeriodColumn => new RemapColumn(c, f) with PeriodColumn { def apply(row: Int) = c(f(row)) }
    case c: HomogeneousArrayColumn[a] =>
      new RemapColumn(c, f) with HomogeneousArrayColumn[a] {
        val tpe             = c.tpe
        def apply(row: Int) = c(f(row))
      }
    case c: EmptyArrayColumn  => new RemapColumn(c, f) with EmptyArrayColumn
    case c: EmptyObjectColumn => new RemapColumn(c, f) with EmptyObjectColumn
    case c: NullColumn        => new RemapColumn(c, f) with NullColumn
  }

  def RemapFilter(filter: Int => Boolean, offset: Int) = CF1P("builtin::ct::remapFilter") {
    case c: BoolColumn   => new RemapFilterColumn(c, filter, offset) with BoolColumn   { def apply(row: Int) = c(row + offset) }
    case c: LongColumn   => new RemapFilterColumn(c, filter, offset) with LongColumn   { def apply(row: Int) = c(row + offset) }
    case c: DoubleColumn => new RemapFilterColumn(c, filter, offset) with DoubleColumn { def apply(row: Int) = c(row + offset) }
    case c: NumColumn    => new RemapFilterColumn(c, filter, offset) with NumColumn    { def apply(row: Int) = c(row + offset) }
    case c: StrColumn    => new RemapFilterColumn(c, filter, offset) with StrColumn    { def apply(row: Int) = c(row + offset) }
    case c: DateColumn   => new RemapFilterColumn(c, filter, offset) with DateColumn   { def apply(row: Int) = c(row + offset) }
    case c: PeriodColumn => new RemapFilterColumn(c, filter, offset) with PeriodColumn { def apply(row: Int) = c(row + offset) }
    case c: HomogeneousArrayColumn[a] =>
      new RemapFilterColumn(c, filter, offset) with HomogeneousArrayColumn[a] {
        val tpe             = c.tpe
        def apply(row: Int) = c(row + offset)
      }
    case c: EmptyArrayColumn  => new RemapFilterColumn(c, filter, offset) with EmptyArrayColumn
    case c: EmptyObjectColumn => new RemapFilterColumn(c, filter, offset) with EmptyObjectColumn
    case c: NullColumn        => new RemapFilterColumn(c, filter, offset) with NullColumn
  }

  def RemapIndices(indices: ArrayIntList) = CF1P("builtin::ct::remapIndices") {
    case c: BoolColumn   => new RemapIndicesColumn(c, indices) with BoolColumn   { def apply(row: Int) = c(indices.get(row)) }
    case c: LongColumn   => new RemapIndicesColumn(c, indices) with LongColumn   { def apply(row: Int) = c(indices.get(row)) }
    case c: DoubleColumn => new RemapIndicesColumn(c, indices) with DoubleColumn { def apply(row: Int) = c(indices.get(row)) }
    case c: NumColumn    => new RemapIndicesColumn(c, indices) with NumColumn    { def apply(row: Int) = c(indices.get(row)) }
    case c: StrColumn    => new RemapIndicesColumn(c, indices) with StrColumn    { def apply(row: Int) = c(indices.get(row)) }
    case c: DateColumn   => new RemapIndicesColumn(c, indices) with DateColumn   { def apply(row: Int) = c(indices.get(row)) }
    case c: PeriodColumn => new RemapIndicesColumn(c, indices) with PeriodColumn { def apply(row: Int) = c(indices.get(row)) }
    case c: HomogeneousArrayColumn[a] =>
      new RemapIndicesColumn(c, indices) with HomogeneousArrayColumn[a] {
        val tpe             = c.tpe
        def apply(row: Int) = c(indices.get(row))
      }
    case c: EmptyArrayColumn  => new RemapIndicesColumn(c, indices) with EmptyArrayColumn
    case c: EmptyObjectColumn => new RemapIndicesColumn(c, indices) with EmptyObjectColumn
    case c: NullColumn        => new RemapIndicesColumn(c, indices) with NullColumn
  }

  def filter(from: Int, to: Int, definedAt: BitSet) = CF1P("builtin::ct::filter") {
    case c: BoolColumn   => new BitsetColumn(definedAt & c.definedAt(from, to)) with BoolColumn   { def apply(row: Int) = c(row) }
    case c: LongColumn   => new BitsetColumn(definedAt & c.definedAt(from, to)) with LongColumn   { def apply(row: Int) = c(row) }
    case c: DoubleColumn => new BitsetColumn(definedAt & c.definedAt(from, to)) with DoubleColumn { def apply(row: Int) = c(row) }
    case c: NumColumn    => new BitsetColumn(definedAt & c.definedAt(from, to)) with NumColumn    { def apply(row: Int) = c(row) }
    case c: StrColumn    => new BitsetColumn(definedAt & c.definedAt(from, to)) with StrColumn    { def apply(row: Int) = c(row) }
    case c: DateColumn   => new BitsetColumn(definedAt & c.definedAt(from, to)) with DateColumn   { def apply(row: Int) = c(row) }
    case c: PeriodColumn => new BitsetColumn(definedAt & c.definedAt(from, to)) with PeriodColumn { def apply(row: Int) = c(row) }
    case c: HomogeneousArrayColumn[a] =>
      new BitsetColumn(definedAt & c.definedAt(from, to)) with HomogeneousArrayColumn[a] {
        val tpe             = c.tpe
        def apply(row: Int) = c(row)
      }
    case c: EmptyArrayColumn  => new BitsetColumn(definedAt & c.definedAt(from, to)) with EmptyArrayColumn
    case c: EmptyObjectColumn => new BitsetColumn(definedAt & c.definedAt(from, to)) with EmptyObjectColumn
    case c: NullColumn        => new BitsetColumn(definedAt & c.definedAt(from, to)) with NullColumn
  }

  def MaskedUnion(leftMask: BitSet) = CF2P("builtin::ct::maskedUnion") {
    case (left: BoolColumn, right: BoolColumn) =>
      new UnionColumn(left, right) with BoolColumn {
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
      }

    case (left: LongColumn, right: LongColumn) =>
      new UnionColumn(left, right) with LongColumn {
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
      }

    case (left: DoubleColumn, right: DoubleColumn) =>
      new UnionColumn(left, right) with DoubleColumn {
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
      }

    case (left: NumColumn, right: NumColumn) =>
      new UnionColumn(left, right) with NumColumn {
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
      }

    case (left: StrColumn, right: StrColumn) =>
      new UnionColumn(left, right) with StrColumn {
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
      }

    case (left: DateColumn, right: DateColumn) =>
      new UnionColumn(left, right) with DateColumn {
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
      }

    case (left: PeriodColumn, right: PeriodColumn) =>
      new UnionColumn(left, right) with PeriodColumn {
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row)
      }

    case (left: HomogeneousArrayColumn[a], right: HomogeneousArrayColumn[b]) if left.tpe == right.tpe =>
      new UnionColumn(left, right) with HomogeneousArrayColumn[a] {
        val tpe             = left.tpe
        def apply(row: Int) = if (leftMask.get(row)) left(row) else right(row).asInstanceOf[Array[a]]
      }

    case (left: EmptyArrayColumn, right: EmptyArrayColumn)   => new UnionColumn(left, right) with EmptyArrayColumn
    case (left: EmptyObjectColumn, right: EmptyObjectColumn) => new UnionColumn(left, right) with EmptyObjectColumn
    case (left: NullColumn, right: NullColumn)               => new UnionColumn(left, right) with NullColumn
  }
}
