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
import scala.collection.JavaConverters._
import scala.collection.mutable.{ListMap => MListMap}

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.io.api._
import org.apache.parquet.schema.MessageType
import scalaz._

/** 
  * Read support is an entry point in conversion between stream of data from
  * parquet file and final type to which data is being transformed to (in our 
  * case it's Data). 
  * Method prepareForRead is the right place to do the projection of columns if
  * we are only interested in subset of coulmns being read.
  * It returns a RecordMaterializer that continues protocol conversation for the whole
  * file
 */
class DataReadSupport extends ReadSupport[Data] {

  override def prepareForRead(conf: Configuration,
    metaData: JMap[String, String],
    schema: MessageType,
    context: ReadContext
  ) = new DataRecordMaterializer(schema)

  override def init(context: InitContext): ReadContext =
    new ReadContext(context.getFileSchema())

}

/** 
  * Is being instantiated for every row group in the parquet file. Requires returning
  * a GroupConverter that will continue the protocol conversation.
  */
class DataRecordMaterializer(schema: MessageType) extends RecordMaterializer[Data] {

  val rootConverter = new DataGroupConverter(schema)

  override def getCurrentRecord(): Data = rootConverter.getCurrentRecord()

  override def getRootConverter(): GroupConverter = rootConverter
}

/** 
  * Converts the group row into data. Protocol is following:
  * 1. call start()
  * 2. fetch converter for each of the columns
  * 3. call end()
  */
class DataGroupConverter(schema: MessageType) extends GroupConverter {

  /**
    * I'm using mutable data structures and var for which I should
    * commit https://en.wikipedia.org/wiki/Seppuku#/media/File:Seppuku-2.jpg
    * however for the time being I don't have idea how to adhere to
    * parquet API in pure FP way :( -  #needhelp
    */
  val values: MListMap[String, Data] = MListMap()
  var record: Data = Data.Null

  val converters: List[DataPrimitiveConverter] =
    schema.getFields().asScala.map { field =>
      new DataPrimitiveConverter(field.getName(), values)
    }.toList

  /**
    * Must return SAME object for given column (based on schema)
    */
  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

  /**
    * Called for every row, at the start of processing
    */
  override def start(): Unit = {}

  /**
    * Called for every row, at the end of processing
    */
  override def end(): Unit = {
    record = Data.Obj(values.toSeq:_*)
    values.clear()
  }

  def getCurrentRecord(): Data = record
}

class DataPrimitiveConverter(name: String, values: MListMap[String, Data])
    extends PrimitiveConverter {


  override def addBoolean(v: Boolean): Unit = {
    values += ((name, Data.Bool(v) : Data))
    ()
  }

  override def addLong(v: Long): Unit = {
    values += ((name, Data.Int(v) : Data))
    ()
  }

  override def addInt(v: Int): Unit = {
    values += ((name, Data.Int(v) : Data))
    ()
  }

  override def addDouble(v: Double): Unit = {
    values += ((name, Data.Dec(v) : Data))
    ()
  }

  override def addFloat(v: scala.Float): Unit = {
    values += ((name, Data.Dec(v) : Data))
    ()
  }

  override def addBinary(v: Binary): Unit = {
    values += ((name, Data.Binary(ImmutableArray.fromArray(v.getBytes())) : Data))
    ()
  }

}


