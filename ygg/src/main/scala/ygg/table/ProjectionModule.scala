package ygg.table

import scalaz._, Scalaz._

case class BlockProjectionData[Key](minKey: Key, maxKey: Key, data: Slice)

trait ProjectionLike {
  type Key
  def structure: Need[Set[ColumnRef]]
  def length: Long

  /**
    * Get a block of data beginning with the first record with a key greater than
    * the specified key. If id.isEmpty, return a block starting with the minimum
    * key. Each resulting block should contain only the columns specified in the
    * column set; if the set of columns is empty, return all columns.
    */
  def getBlockAfter(id: Option[Key], columns: Option[Set[ColumnRef]]): Need[Option[BlockProjectionData[Key]]]

  def getBlockStream(columns: Option[Set[ColumnRef]]): StreamT[Need, Slice] = {
    StreamT.unfoldM[Need, Slice, Option[Key]](None) { key =>
      getBlockAfter(key, columns) map {
        _ map { case BlockProjectionData(_, maxKey, block) => (block, Some(maxKey)) }
      }
    }
  }
}
