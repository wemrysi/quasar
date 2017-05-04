package quasar.precog.common.ingest

import org.scalacheck.Gen
import quasar.precog.TestSupport._

object EventIdSpecs extends Specification with ScalaCheck {
  implicit val idRange = Gen.chooseNum[Int](0, Int.MaxValue)

  "EventId" should {
    "support round-trip encap/decap of producer/sequence ids" in prop { (prod: Int, seq: Int) =>
      val uid = EventId(prod, seq).uid

      EventId.producerId(uid) mustEqual prod
      EventId.sequenceId(uid) mustEqual seq
    }
  }
}
