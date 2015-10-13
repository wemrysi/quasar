package quasar
package physical
package mongodb

import quasar.Predef._
import quasar.fp.TaskRef
import quasar.fs._

import scala.collection.JavaConverters._

import org.bson.Document
import com.mongodb.async.AsyncBatchCursor
import com.mongodb.async.client.MongoClient
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object readfile {
  import ReadFile._, ReadError._, MongoDb._

  type OpenHandlesT[F[_], A] = WriterT[F, ISet[ReadHandle], A]
  type OpenHandles[       A] = OpenHandlesT[Task, A]

  /** Interpret the [[ReadFile]] algebra using MongoDB */
  val interpret: ReadFile ~> ReadMongo = new (ReadFile ~> ReadMongo) {
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
  }

  /** Run [[ReadMongo]] using the given [[MongoClient]], discarding the resulting
    * state and automatically closing any remaining open [[ReadHandle]]s. Any handles
    * closed in such a manner are reported in the result.
    */
  def run(client: MongoClient): ReadMongo ~> OpenHandles =
    new (ReadMongo ~> OpenHandles) {
      def apply[A](rm: ReadMongo[A]) =
        for {
          r <- rm.run((0, Map.empty)).run(client).liftM[OpenHandlesT]
          ((_, cursors), a) = r
          _ <- cursors.toList.traverseU { case (h, c) =>
                 WriterT.put(Task.delay(c.close()))(ISet singleton h)
               }
        } yield a
    }

  /** Transform [[OpenHandles]] to [[Task]] by discarding the written handles. */
  val ignoreOpenHandles: OpenHandles ~> Task =
    new (OpenHandles ~> Task) {
      def apply[A](oh: OpenHandles[A]) = oh.value
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
