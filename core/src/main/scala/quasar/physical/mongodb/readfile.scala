package quasar
package physical
package mongodb

import quasar.Predef._
import quasar.fs._

import scala.collection.JavaConverters._

import org.bson.Document
import com.mongodb.async.AsyncBatchCursor
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object readfile extends (ReadFile ~> ReadMongo) {
  import ReadFile._, ReadError._, MongoDb._

  def apply[A](rf: ReadFile[A]) = rf match {
    case Open(file, offset, limit) =>
      Collection.fromFile(file).fold(
        err  => PathError(err).left.point[ReadMongo],
        coll => for {
          it   <- find(coll).liftM[ReadStateT]
          skpd =  it skip offset.run.toInt
          ltd  =  limit cata (n => skpd.limit(n.run.toInt), skpd)
          // TODO: Does MongoDB error if the database doesn't exist?
          cur  <- async(ltd.batchCursor).liftM[ReadStateT]
          h    <- seqL.modo(_ + 1).map(ReadHandle(_)).lift[MongoDb]
          _    <- cursorL(h).assign(Some(cur)).lift[MongoDb]
        } yield h.right)

    case Read(h) =>
      lookupCursor(h)
        .flatMapF(nextChunk)
        .toRight(UnknownHandle(h))
        .run

    case Close(h) =>
      OptionT[ReadMongo, BsonCursor](cursorL(h).assigno(None).lift[MongoDb])
        .flatMapF(c => liftTask(Task.delay(c.close())).liftM[ReadStateT])
        .run.void
  }

  ////

  private val seqL: ReadState @> Long =
    Lens.firstLens

  private val cursorsL: ReadState @> Map[ReadHandle, BsonCursor] =
    Lens.secondLens

  private def cursorL(h: ReadHandle): ReadState @> Option[BsonCursor] =
    Lens.mapVLens(h) compose cursorsL

  private def nextChunk(c: BsonCursor): ReadMongo[Vector[Data]] = {
    val withoutId: Document => Document =
      d => (d: Id[Document]) map (_ remove "_id") as d

    val toData: Document => Data =
      (BsonCodec.toData _) compose (Bson.fromRepr _) compose withoutId

    // NB: `null` is used as a sentinel value to indicate input is exhausted,
    //     because Java.
    async(c.next)
      .map(r => Option(r).map(_.asScala.toVector).orZero.map(toData))
      .liftM[ReadStateT]
  }

  private def lookupCursor(h: ReadHandle): OptionT[ReadMongo, BsonCursor] =
    OptionT[ReadMongo, BsonCursor](cursorL(h).st.lift[MongoDb])
}
