package quasar
package physical
package mongodb

import quasar.Predef._
import quasar.fp._

import scala.collection.JavaConverters._
import java.util.LinkedList

import org.bson.Document
import com.mongodb.{MongoCredential, MongoCommandException}
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.client.model._
import com.mongodb.async._
import com.mongodb.async.client._

import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream._

final class MongoDb[A] private (protected val r: ReaderT[Task, MongoClient, A]) {
  def map[B](f: A => B): MongoDb[B] =
    new MongoDb(r map f)

  def flatMap[B](f: A => MongoDb[B]): MongoDb[B] =
    new MongoDb(r flatMap (a => f(a).r))

  def attempt: MongoDb[Throwable \/ A] =
    new MongoDb(r mapK (_.attempt))

  def run(c: MongoClient): Task[A] = r.run(c)
}

object MongoDb {

  /** All discoverable collections on the server. */
  def collections: Process[MongoDb, Collection] =
    for {
      name <- databaseNames
      db   <- database(name).liftM[Process]
      coll <- iterableToProcess(db.listCollectionNames)
    } yield Collection(name, coll)

  /** Names of all discoverable databases on the server. */
  def databaseNames: Process[MongoDb, String] =
    client.liftM[Process]
      .flatMap(c => iterableToProcess(c.listDatabaseNames))
      .onFailure {
        case t: MongoCommandException =>
          credentials.liftM[Process]
            .flatMap(ys => Process.emitAll(ys.map(_.getSource).distinct))

        case t =>
          Process.fail(t)
      }

  def dropCollection(c: Collection): MongoDb[Unit] =
    collection(c) flatMap (mc => async(mc.drop).void)

  def dropDatabase(named: String): MongoDb[Unit] =
    database(named) flatMap (d => async(d.drop).void)

  /** Attempts to insert as many of the given documents into the collection as
    * possible. The number of documents inserted is returned, if possible, and
    * may be smaller than the original amount if any documents failed to insert.
    */
  def insertAny[F[_]: Foldable](docs: F[Document], coll: Collection): OptionT[MongoDb, Int] = {
    val docList = Foldable[F].foldLeft(docs, new LinkedList[WriteModel[Document]]()) { (l, d) =>
      l.point[Id] map (_ add new InsertOneModel(d)) as l
    }

    val writeOpts = (new BulkWriteOptions()).ordered(false)

    OptionT(
      collection(coll)
       .flatMap(c => async[BulkWriteResult](c.bulkWrite(docList, writeOpts, _)))
       .map(r => r.wasAcknowledged option r.getInsertedCount))
  }

  def fail[A](t: Throwable): MongoDb[A] =
    new MongoDb(Kleisli(_ => Task.fail(t)))

  val liftTask: Task ~> MongoDb =
    new (Task ~> MongoDb) {
      def apply[A](t: Task[A]) = new MongoDb(Kleisli(_ => t))
    }

  private[mongodb] def find(c: Collection): MongoDb[FindIterable[Document]] =
    collection(c) map (_.find)

  private[mongodb] def async[A](f: SingleResultCallback[A] => Unit): MongoDb[A] =
    liftTask(Task.async(cb => f(new DisjunctionCallback(cb))))

  implicit val mongoDbInstance: Monad[MongoDb] with Catchable[MongoDb] =
    new Monad[MongoDb] with Catchable[MongoDb] {
      override def map[A, B](fa: MongoDb[A])(f: A => B) = fa map f
      def point[A](a: => A) = new MongoDb(Kleisli(_ => Task.now(a)))
      def bind[A, B](fa: MongoDb[A])(f: A => MongoDb[B]) = fa flatMap f
      def fail[A](t: Throwable) = fail(t)
      def attempt[A](fa: MongoDb[A]) = fa.attempt
    }

  ////

  private def apply[A](f: MongoClient => A): MongoDb[A] =
    lift(c => Task.delay(f(c)))

  private def lift[A](f: MongoClient => Task[A]): MongoDb[A] =
    new MongoDb(Kleisli(f))

  private def client: MongoDb[MongoClient] =
    MongoDb(ι)

  // TODO: Make a basic credential type in scala and expose this method.
  private val credentials: MongoDb[List[MongoCredential]] =
    MongoDb(_.getSettings.getCredentialList.asScala.toList)

  private def collection(c: Collection): MongoDb[MongoCollection[Document]] =
    database(c.databaseName) map (_ getCollection c.collectionName)

  private def database(named: String): MongoDb[MongoDatabase] =
    MongoDb(_ getDatabase named)

  private def iterableToProcess[A](it: MongoIterable[A]): Process[MongoDb, A] = {
    def go(c: AsyncBatchCursor[A]): Process[MongoDb, A] =
      Process.eval(async(c.next))
        .flatMap(r => Option(r).cata(
          as => Process.emitAll(as.asScala.toVector) ++ go(c),
          Process.halt))

    Process.eval(async(it.batchCursor)) flatMap (cur =>
      go(cur) onComplete Process.eval_(MongoDb(_ => cur.close())))
  }

  private final class DisjunctionCallback[A](f: Throwable \/ A => Unit)
    extends SingleResultCallback[A] {

    def onResult(result: A, error: Throwable): Unit =
      f(Option(error) <\/ result)
  }
}
