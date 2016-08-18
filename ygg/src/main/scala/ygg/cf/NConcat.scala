package ygg.cf

import blueeyes._
import quasar.ygg._, table._

case object NConcat {
  // Closest thing we can get to casting an array. This is completely unsafe.
  private def copyCastArray[A: CTag](as: Array[_]): Array[A] = {
    val bs = new Array[A](as.length)
    System.arraycopy(as, 0, bs, 0, as.length)
    bs
  }

  def apply(cols: List[Int -> Column]) = {
    val sortedCols             = cols.sortBy(_._1)
    val offsets: Array[Int]    = sortedCols.map(_._1)(collection.breakOut)
    val columns: Array[Column] = sortedCols.map(_._2)(collection.breakOut)

    cols match {
      case (_, _: BoolColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[BoolColumn]) =>
        val boolColumns = copyCastArray[BoolColumn](columns)
        Some(new NConcatColumn(offsets, boolColumns) with BoolColumn {
          def apply(row: Int) = {
            val i = indexOf(row)
            boolColumns(i)(row - offsets(i))
          }
        })

      case (_, _: LongColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[LongColumn]) =>
        val longColumns = copyCastArray[LongColumn](columns)
        Some(new NConcatColumn(offsets, longColumns) with LongColumn {
          def apply(row: Int) = {
            val i = indexOf(row)
            longColumns(i)(row - offsets(i))
          }
        })

      case (_, _: DoubleColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[DoubleColumn]) =>
        val doubleColumns = copyCastArray[DoubleColumn](columns)
        Some(new NConcatColumn(offsets, doubleColumns) with DoubleColumn {
          def apply(row: Int) = {
            val i = indexOf(row)
            doubleColumns(i)(row - offsets(i))
          }
        })

      case (_, _: NumColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[NumColumn]) =>
        val numColumns = copyCastArray[NumColumn](columns)
        Some(new NConcatColumn(offsets, numColumns) with NumColumn {
          def apply(row: Int) = {
            val i = indexOf(row)
            numColumns(i)(row - offsets(i))
          }
        })

      case (_, _: StrColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[StrColumn]) =>
        val strColumns = copyCastArray[StrColumn](columns)
        Some(new NConcatColumn(offsets, strColumns) with StrColumn {
          def apply(row: Int) = {
            val i = indexOf(row)
            strColumns(i)(row - offsets(i))
          }
        })

      case (_, _: DateColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[DateColumn]) =>
        val dateColumns = copyCastArray[DateColumn](columns)
        Some(new NConcatColumn(offsets, dateColumns) with DateColumn {
          def apply(row: Int) = {
            val i = indexOf(row)
            dateColumns(i)(row - offsets(i))
          }
        })

      case (_, _: PeriodColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[PeriodColumn]) =>
        val periodColumns = copyCastArray[PeriodColumn](columns)
        Some(new NConcatColumn(offsets, periodColumns) with PeriodColumn {
          def apply(row: Int) = {
            val i = indexOf(row)
            periodColumns(i)(row - offsets(i))
          }
        })

      case (_, _: EmptyArrayColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[EmptyArrayColumn]) =>
        val emptyArrayColumns = copyCastArray[EmptyArrayColumn](columns)
        Some(new NConcatColumn(offsets, emptyArrayColumns) with EmptyArrayColumn)

      case (_, _: EmptyObjectColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[EmptyObjectColumn]) =>
        val emptyObjectColumns = copyCastArray[EmptyObjectColumn](columns)
        Some(new NConcatColumn(offsets, emptyObjectColumns) with EmptyObjectColumn)

      case (_, _: NullColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[NullColumn]) =>
        val nullColumns = copyCastArray[NullColumn](columns)
        Some(new NConcatColumn(offsets, nullColumns) with NullColumn)

      case _ => None
    }
  }
}
