package ygg.table

import blueeyes._
import org.mapdb._

final case class SortingKeyComparator(rowFormat: RowFormat, ascending: Boolean) extends Serializer[Array[Byte]] {
  def deserialize(input: DataInput2, available: Int): Array[Byte] = warn(s"deserialize($input, $available)")(???)
  def serialize(out: DataOutput2, value: Array[Byte]): Unit       = warn(s"serialize($out, $value)")(???)

  def compare(a: Array[Byte], b: Array[Byte]): Int =
    rowFormat.compare(a, b) |> (n => if (ascending) n else -n)
}
