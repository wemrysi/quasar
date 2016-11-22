/** Some signatures from precog.
 *  - conflation of concerns
 *  - primitive types
 */

def cogroup(leftKey: TransSpec1, rightKey: TransSpec1, that: Table)(leftResultTrans: TransSpec1, rightResultTrans: TransSpec1, bothResultTrans: TransSpec2): Table

case class Cell private[MergeEngine] (index: Int, maxKey: KeyType, slice0: Slice)(succf: KeyType => M[Option[BlockData]], remap: Array[Int], var position: Int)
