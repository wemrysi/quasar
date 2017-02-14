/*
 * Copyright 2014–2016 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.physical.sparkcore.fs.hdfs.parquet

import quasar.Predef._
import quasar.Qspec

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.parquet.schema.MessageTypeParser
import org.apache.parquet.schema.MessageType
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.example.data.Group
import scalaz._, Scalaz._

class SerializableGroupReadSupport extends GroupReadSupport with Serializable

class ProjectableGroupReadSupport(projection: MessageType)
    extends GroupReadSupport
    with Serializable {

  private val projectionStr: String = projection.toString

  override def init(configuration: Configuration,
                    keyValueMetaData: java.util.Map[String, String],
                    fileSchema: MessageType): ReadContext =
    new ReadContext(MessageTypeParser.parseMessageType(projectionStr))
}

class ParquetRDDSpec extends Qspec {

  val config =
    new SparkConf().setMaster("local[*]").setAppName("ParquetRDDSpec")

  val path = "sparkcore/src/test/resources/example-data.parquet"

  import ParquetRDD._

  "ParquetRDDE" should {
    "should read parquet files by using sc.parquet" in {
      withinSpark(sc => {
        // when
        val rdd: RDD[Group] =
          sc.parquet(path, new SerializableGroupReadSupport())
        // then
        val login =
          rdd.filter(_.getLong("id", 0) == 20).map(_.getString("login", 0)).first()
        login must_= "login20"
      })
    }

    "should be returned from sc.parquet with schema projection" in {
      withinSpark(sc => {
        // given
        val projection = MessageTypeParser.parseMessageType(
          "message User {\n" +
            "   required int32 age;\n" +
            "}")
        //when
        val rdd: RDD[Group] =
          sc.parquet(path, new ProjectableGroupReadSupport(projection))
        // then
        val result: String =
          rdd.filter(_.getInteger("age", 0) == 30).map(_.toString).first()
        result must_= "age: 30\n"
      })
    }
  }


  def withinSpark[T](runnable: SparkContext => T): T = {
    val sc = new SparkContext(config)
    val result = runnable(sc)
    sc.stop()
    result
  }

}
