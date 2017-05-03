package quasar.yggdrasil
package jdbm3

import quasar.blueeyes._
import java.util.Comparator

final case class SortingKeyComparator(rowFormat: RowFormat, ascending: Boolean) extends Comparator[Array[Byte]] {
  def compare(a: Array[Byte], b: Array[Byte]): Int =
    rowFormat.compare(a, b) |> (n => if (ascending) n else -n)
}
