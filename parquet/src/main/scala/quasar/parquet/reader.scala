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

package quasar.parquet

import quasar.Predef._
import quasar.Data

import java.util.{Map => JMap}
import java.time._
import scala.collection.JavaConverters._
import scala.collection.mutable.{ListMap => MListMap, MutableList => MList}

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.io.api._
import org.apache.parquet.schema.{OriginalType, MessageType, GroupType}
import scalaz._
import scalaz.concurrent.Task

object ReadSupportProvider extends ReadSupportProvider {
  // NB: ReadSupport[Data] is still mutable. This is just a step towards
  //     encapsulation behind ParquetRDD.
  val rs: Task[ReadSupport[Data]] = Task.delay(new DataReadSupport)
}

sealed abstract class ReadSupportProvider {

  def rs: Task[ReadSupport[Data]]

  /**
    * Read support is an entry point in conversion between stream of data from
    * parquet file and final type to which data is being transformed to (in our
    * case it's Data).
    * Method prepareForRead is the right place to do the projection of columns if
    * we are only interested in subset of columns being read.
    * It returns a RecordMaterializer that continues protocol conversation for the whole
    * file
   */
  protected class DataReadSupport extends ReadSupport[Data] with Serializable {

    override def prepareForRead(conf: Configuration,
      metaData: JMap[String, String],
      schema: MessageType,
      context: ReadContext
    ): RecordMaterializer[Data] =
      new DataRecordMaterializer(schema)

    override def init(context: InitContext): ReadContext =
      new ReadContext(context.getFileSchema())

  }

  /**
    * Is being instantiated for every row group in the parquet file. Requires returning
    * a GroupConverter that will continue the protocol conversation.
    */
  private class DataRecordMaterializer(schema: MessageType) extends RecordMaterializer[Data] {

    val rootConverter = new DataGroupConverter(schema, None)

    override def getCurrentRecord(): Data = rootConverter.getCurrentRecord()

    override def getRootConverter(): GroupConverter = rootConverter
  }

  private trait ConverterLike {

    def schema: GroupType

    def save: (String, Data) => Unit

    val converters: List[Converter] =
      schema.getFields().asScala.map(field => field.getOriginalType() match {
        case OriginalType.UTF8 => new DataStringConverter(field.getName(), save)
        case OriginalType.DATE => new DataDateConverter(field.getName(), save)
        case OriginalType.TIME_MILLIS => new DataTimeConverter(field.getName(), save)
        case OriginalType.TIMESTAMP_MILLIS => new DataTimestampConverter(field.getName(), save)
        case OriginalType.LIST =>
          new DataListConverter(field.asGroupType(), field.getName(), this)
        case a if !field.isPrimitive() =>
          new DataGroupConverter(field.asGroupType(), Some((field.getName(), this)))
        case _ => new DataPrimitiveConverter(field.getName(), save)
      }).toList
  }

  /**
    * Converts the group row into data. Protocol is following:
    * 1. call start()
    * 2. fetch converter for each of the columns
    * 3. call end()
    */
  private class DataGroupConverter(
    val schema: GroupType,
    parent: Option[(String, ConverterLike)]
  ) extends GroupConverter with ConverterLike {

    /**
      * I'm using mutable data structures and var for which I should
      * commit https://en.wikipedia.org/wiki/Seppuku#/media/File:Seppuku-2.jpg
      * however for the time being I don't have idea how to adhere to
      * parquet API in pure FP way :( -  #needhelp
      */
    val values: MListMap[String, Data] = MListMap()
    var record: Data = Data.Null

    def save: (String, Data) => Unit = (name: String, data: Data) => {
      values += ((name, data))
      ()
    }

    /**
      * Must return SAME object for given column (based on schema)
      */
    override def getConverter(fieldIndex: Int): Converter = converters.apply(fieldIndex)

    /**
      * Called for every row, at the start of processing
      */
    override def start(): Unit = {}

    /**
      * Called for every row, at the end of processing
      */
    override def end(): Unit = {
      parent.fold {
        record = Data.Obj(values.toSeq:_*)
        ()
      } { case (name, p) =>
          p.save(name, Data.Obj(values.toSeq:_*))
      }
      values.clear()
    }

    def getCurrentRecord(): Data = record

  }

  private class DataListConverter(
    val schema: GroupType,
    name: String,
    parent: ConverterLike
  ) extends GroupConverter with ConverterLike {

    val values: MList[Data] = MList()

    def save: (String, Data) => Unit = (name: String, data: Data) => {
      values += data
      ()
    }

    override def getConverter(fieldIndex: Int): Converter = converters.apply(fieldIndex)

    override def start(): Unit = {}

    override def end(): Unit = {
      parent.save(name, Data.Arr(values.toList))
      values.clear()
    }
  }


  private class DataStringConverter(name: String, save: (String, Data) => Unit)
      extends PrimitiveConverter {

    override def addBinary(v: Binary): Unit =
      save(name, Data.Str(new String(v.getBytes())) : Data)
  }

  private class DataDateConverter(name: String, save: (String, Data) => Unit)
      extends PrimitiveConverter {
    override def addInt(v: Int): Unit =
      save(name, Data.Date(LocalDate.of(1970,1,1).plusDays(v.toLong)) : Data)
  }

  private class DataTimestampConverter(name: String, save: (String, Data) => Unit)
      extends PrimitiveConverter {
    override def addLong(v: Long): Unit =
      save(name, Data.Timestamp(Instant.ofEpochMilli(v)) : Data)
  }

  private class DataTimeConverter(name: String, save: (String, Data) => Unit)
      extends PrimitiveConverter {
    override def addInt(v: Int): Unit =
      save(name, Data.Time(LocalTime.ofNanoOfDay((v * 100).toLong)) : Data)
  }


  private class DataPrimitiveConverter(name: String, save: (String, Data) => Unit)
      extends PrimitiveConverter {

    override def addBoolean(v: Boolean): Unit =
      save(name, Data.Bool(v) : Data)

    override def addLong(v: Long): Unit =
      save(name, Data.Int(v) : Data)

    override def addInt(v: Int): Unit =
      save(name, Data.Int(v) : Data)

    override def addDouble(v: Double): Unit =
      save(name, Data.Dec(v) : Data)

    override def addFloat(v: scala.Float): Unit =
      save(name, Data.Dec(v) : Data)

    override def addBinary(v: Binary): Unit =
      save(name, Data.Binary(ImmutableArray.fromArray(v.getBytes())) : Data)
  }
}
