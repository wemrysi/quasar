package quasar
package physical
package mongodb

import quasar.fs._

import org.bson.Document
import com.mongodb.async.AsyncBatchCursor
import scalaz._

object readfile extends (ReadFile ~> ReadMongo) {
  import ReadFile._

  def apply[A](rf: ReadFile[A]) = rf match {
  }

  ////

  val seqL: Lens[Long, ReadState] = Lens.firstLens
  val cursorsL: Lens[Map[ReadHandle, BsonCursor]] = Lens.secondLens
}
