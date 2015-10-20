package quasar
package physical
package mongodb
package fs

import quasar.Predef._
import quasar.fp._
import quasar.fs._

import org.bson.Document
import com.mongodb.async.client.MongoClient
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object writefile {
  import WriteFile._, FileSystemError._, MongoDb._

  type WriteState           = (Long, Map[WriteHandle, Collection])
  type WriteStateT[F[_], A] = StateT[F, WriteState, A]
  type WriteMongo[A]        = WriteStateT[MongoDb, A]

  /** Interpret the [[WriteFile]] algebra using MongoDB. */
  val interpret: WriteFile ~> WriteMongo = new (WriteFile ~> WriteMongo) {
    def apply[A](wf: WriteFile[A]) = wf match {
      case Open(file) =>
        Collection.fromFile(file) fold (
          err  => PathError(err).left.point[WriteMongo],
          coll => seqL.modo(_ + 1).map(WriteHandle(_))
                    .flatMap(h => collectionL(h).assign(Some(coll)).as(h.right[FileSystemError]))
                    .lift[MongoDb])

      case Write(h, data) =>
        val (errs, docs) = data foldMap { d =>
          dataToDocument(d).fold(
            e => (Vector(e), Vector()),
            d => (Vector(), Vector(d)))
        }

        lookupCollection(h) flatMap (_.cata(
          c => insertAny(docs, c)
                 .filter(_ < docs.size)
                 .map(n => PartialWrite(docs.size - n))
                 .run.map(errs ++ _)
                 .liftM[WriteStateT],
          (errs :+ UnknownWriteHandle(h)).point[WriteMongo]))

      case Close(h) =>
        collectionL(h).mods_(κ(None)).lift[MongoDb]
    }
  }

  /** Run [[WriteMongo]] using the given [[MongoClient]], discarding the
    * resulting state.
    */
  def run(client: MongoClient): WriteMongo ~> Task =
    new (WriteMongo ~> Task) {
      def apply[A](wm: WriteMongo[A]) =
        wm.eval((0, Map.empty)).run(client)
    }

  ////

  private val seqL: WriteState @> Long =
    Lens.firstLens

  private val collectionsL: WriteState @> Map[WriteHandle, Collection] =
    Lens.secondLens

  private def collectionL(h: WriteHandle): WriteState @> Option[Collection] =
    Lens.mapVLens(h) compose collectionsL

  private def lookupCollection(h: WriteHandle): WriteMongo[Option[Collection]] =
    collectionL(h).st.lift[MongoDb]

  private def dataToDocument(d: Data): FileSystemError \/ Document =
    BsonCodec.fromData(d)
      .leftMap(err => WriteFailed(d, err.toString))
      .flatMap {
        case doc @ Bson.Doc(_) => doc.repr.right
        case otherwise         => WriteFailed(d, "MongoDB is only able to store documents").left
      }
}
