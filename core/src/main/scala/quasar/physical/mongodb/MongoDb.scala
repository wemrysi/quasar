package quasar
package physical
package mongodb

import quasar.Predef._

import scala.collection.JavaConverters._

import org.bson.Document
import com.mongodb.{MongoCredential, MongoCommandException}
import com.mongodb.async._
import com.mongodb.async.client._

import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream._

final class MongoDb[A] private (protected val r: ReaderT[Task, MongoClient, A]) {
  def map[B](f: A => B): MongoDb[B] =
    new MongoDb(r map f)

  def mapK[B](f: Task[A] => Task[B]): MongoDb[B] =
    new MongoDb(r mapK f)

  def flatMap[B](f: A => MongoDb[B]): MongoDb[B] =
    new MongoDb(r flatMap (a => f(a).r))

  def flatMapK[B](f: A => Task[B]): MongoDb[B] =
    new MongoDb(r flatMapK f)

  def attempt: MongoDb[Throwable \/ A] =
    mapK(_.attempt)

  def run(c: MongoClient): Task[A] = r.run(c)
}

object MongoDb {

  /** All discoverable collections on the server. */
  def collections: MongoDb[Process[Task, Collection]] =
    (databaseNames |@| getDatabase)((xs, f) => xs flatMap (n =>
      iterableToProcess(f(n).listCollectionNames) map (Collection(n, _))))

  /** Names of all discoverable databases on the server. */
  def databaseNames: MongoDb[Process[Task, String]] = {
    val names = MongoDb(c => iterableToProcess(c.listDatabaseNames))

    (names |@| credentials.mapK(Task.now))((xs, cs) => xs onFailure {
      case t: MongoCommandException =>
        cs.liftM[Process]
          .flatMap(ys => Process.emitAll(ys.map(_.getSource).distinct))

      case t =>
        Process.fail(t)
    })
  }

  def dropCollection(c: Collection): MongoDb[Unit] =
    collection(c) flatMapK (mc => mongoCbToTask(mc.drop).void)

  def dropDatabase(named: String): MongoDb[Unit] =
    database(named) flatMapK (d => mongoCbToTask(d.drop).void)

  def fail[A](t: Throwable): MongoDb[A] =
    new MongoDb(Kleisli(_ => Task.fail(t)))

  val liftTask: Task ~> MongoDb =
    new (Task ~> MongoDb) {
      def apply[A](t: Task[A]) = new MongoDb(Kleisli(_ => t))
    }

  private[mongodb] def find(c: Collection): MongoDb[FindIterable[Document]] =
    collection(c) map (_.find)

  private[mongodb] def mongoCbToTask[A](f: SingleResultCallback[A] => Unit): Task[A] =
    Task.async(cb => f(new DisjunctionCallback(cb)))

  implicit val mongoDbInstance: Monad[MongoDb] with Catchable[MongoDb] =
    new Monad[MongoDb] with Catchable[MongoDb] {
      override def map[A, B](fa: MongoDb[A])(f: A => B) = fa map f
      def point[A](a: => A) = new MongoDb(Kleisli(_ => Task.now(a)))
      def bind[A, B](fa: MongoDb[A])(f: A => MongoDb[B]) = fa flatMap f
      def fail[A](t: Throwable) = fail(t)
      def attempt[A](fa: MongoDb[A]) = fa.attempt
    }

  ////

  // TODO: Make a basic credential type in scala and expose this method.
  private def credentials: MongoDb[List[MongoCredential]] =
    MongoDb(_.getSettings.getCredentialList.asScala.toList)

  private def collection(c: Collection): MongoDb[MongoCollection[Document]] =
    database(c.databaseName) map (_ getCollection c.collectionName)

  private def database(named: String): MongoDb[MongoDatabase] =
    named.point[MongoDb] <*> getDatabase

  private def getDatabase: MongoDb[String => MongoDatabase] =
    MongoDb(c => name => c.getDatabase(name))

  private def apply[A](f: MongoClient => A): MongoDb[A] =
    new MongoDb(Kleisli(c => Task.delay(f(c))))

  private def iterableToProcess[A](it: MongoIterable[A]): Process[Task, A] = {
    def go(c: AsyncBatchCursor[A]): Process[Task, A] =
      Process.eval(mongoCbToTask(c.next)) flatMap { r =>
        Option(r).cata(as => Process.emitAll(as.asScala.toVector) ++ go(c), Process.halt)
      }

    Process.eval(mongoCbToTask(it.batchCursor)) flatMap { cur =>
      go(cur) onComplete Process.eval_(Task.delay(cur.close()))
    }
  }

  private final class DisjunctionCallback[A](f: Throwable \/ A => Unit)
    extends SingleResultCallback[A] {

    def onResult(result: A, error: Throwable): Unit =
      f(Option(error) <\/ result)
  }
}
