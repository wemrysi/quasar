package quasar.ygg

import blueeyes._
import com.precog.common._
import quasar.ygg.table._

object JDBM {
  type Bytes             = Array[Byte]
  type BtoBEntry         = jMapEntry[Bytes, Bytes]
  type BtoBIterator      = Iterator[BtoBEntry]
  type BtoBMap           = java.util.SortedMap[Bytes, Bytes]
  type BtoBConcurrentMap = jConcurrentMap[Bytes, Bytes]

  final case class JSlice(firstKey: Bytes, lastKey: Bytes, rows: Int)
}

object JDBMSlice {
  def columnFor(prefix: CPath, sliceSize: Int)(ref: ColumnRef): ColumnRef -> ArrayColumn[_] = ((
     ref.copy(selector = prefix \ ref.selector),
     ref.ctype match {
       case CString              => ArrayStrColumn.empty(sliceSize)
       case CBoolean             => ArrayBoolColumn.empty()
       case CLong                => ArrayLongColumn.empty(sliceSize)
       case CDouble              => ArrayDoubleColumn.empty(sliceSize)
       case CNum                 => ArrayNumColumn.empty(sliceSize)
       case CDate                => ArrayDateColumn.empty(sliceSize)
       case CPeriod              => ArrayPeriodColumn.empty(sliceSize)
       case CNull                => MutableNullColumn.empty()
       case CEmptyObject         => MutableEmptyObjectColumn.empty()
       case CEmptyArray          => MutableEmptyArrayColumn.empty()
       case CArrayType(elemType) => ArrayHomogeneousArrayColumn.empty(sliceSize)(elemType)
       case CUndefined           => abort("CUndefined cannot be serialized")
     }
  ))
}
