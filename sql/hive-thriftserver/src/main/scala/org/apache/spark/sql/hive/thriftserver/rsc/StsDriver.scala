/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.rsc

import java.nio.ByteBuffer

import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import java.util.{Arrays, ArrayList => JArrayList, List => JList}

import scala.collection.JavaConverters._

import io.netty.channel.ChannelHandlerContext
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{DataFrame, SparkSession, Row => SparkRow}
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.io.ObjectOutputStream
import java.io.ObjectOutput
import java.security.PrivilegedExceptionAction
import java.sql.Date
import java.sql.Timestamp

import io.netty.channel.ChannelHandlerContext

import org.apache.spark.serializer.JavaSerializer
import org.apache.hive.service.cli._

import org.apache.spark.sql.types._
import org.apache.spark.util.{Utils => SparkUtils}
import org.apache.spark.sql.hive.HiveUtils
import org.apache.hadoop.hive.metastore.api.FieldSchema
import scala.collection.mutable.{ArrayBuffer, Map => SMap}
import java.security.PrivilegedExceptionAction


class StsDriver(conf: SparkConf, livyConf: RSCConf)
  extends RSCDriver(conf, livyConf)
  with Logging {

  private var ss: SparkSession = _
  private var output: DataFrame = _
  private var iter: Iterator[SparkRow] = _
  private var iterHeader: Iterator[SparkRow] = _
  private var dataTypes: Array[DataType] = _
  private var statementId: String = _


  override protected def initializeContext(): Unit = {

    info("tlitest starting Spark Session")
    ss = SparkSession
      .builder()
      .config("hive.metastore.uris", "thrift://127.0.0.1:9083")
      .enableHiveSupport()
      .getOrCreate()
  }

  override protected def shutdownContext(): Unit = {
    if (ss != null) {
      try {
        ss.sparkContext.stop()
        ss = null
      } finally {
        super.shutdownContext()
      }
    }
  }

  def handle(ctx: ChannelHandlerContext, msg: BaseProtocol.SqlQueryRequest): Unit = {
    info("received the request: " + msg)

    if (ss == null)
      {
        info("SparkSession not initialized. Initializing now.")
        ss = SparkSession
          .builder()
          .config("hive.metastore.uris", "thrift://127.0.0.1:9083")
          .enableHiveSupport()
          .getOrCreate()
      }

    output = ss.sql(msg.code)

    info("Query df result: " + output)

    var qe = output.queryExecution;
    val resultStr = qe.hiveResultString();
    for (str <- resultStr) {
      info("Query result string: " + str)
    }

    iter = output.collect().iterator

    val (itra, itrb) = iter.duplicate
    iterHeader = itra
    iter = itrb
    dataTypes = output.queryExecution.analyzed.output.map(_.dataType).toArray
  }

  def getResultSetSchema: TableSchema = resultSchema

  private lazy val resultSchema: TableSchema = {
    if (output == null || output.queryExecution.analyzed.output.size == 0) {
      new TableSchema(Arrays.asList(new FieldSchema("Result", "string", "")))
    } else {
      info(s"Result Schema: ${output.queryExecution.analyzed.output}")
      val schema = output.queryExecution.analyzed.output.map { attr =>
        new FieldSchema(attr.name, attr.dataType.catalogString, "")
      }
      new TableSchema(schema.asJava)
    }
  }

  def getSchema(): TableSchema ={
    if (output == null || output.queryExecution.analyzed.output.size == 0) {
      new TableSchema(Arrays.asList(new FieldSchema("Result", "string", "")))
    } else {
      info(s"Result Schema: ${output.queryExecution.analyzed.output}")
      val schema = output.queryExecution.analyzed.output.map { attr =>
        new FieldSchema(attr.name, attr.dataType.catalogString, "")
      }
      new TableSchema(schema.asJava)
    }
  }


  def addNonNullColumnValue(from: SparkRow, to: ArrayBuffer[Any], ordinal: Int) {
    dataTypes(ordinal) match {
      case StringType =>
        to += from.getString(ordinal)
      case IntegerType =>
        to += from.getInt(ordinal)
      case BooleanType =>
        to += from.getBoolean(ordinal)
      case DoubleType =>
        to += from.getDouble(ordinal)
      case FloatType =>
        to += from.getFloat(ordinal)
      case DecimalType() =>
        to += from.getDecimal(ordinal)
      case LongType =>
        to += from.getLong(ordinal)
      case ByteType =>
        to += from.getByte(ordinal)
      case ShortType =>
        to += from.getShort(ordinal)
      case DateType =>
        to += from.getAs[Date](ordinal)
      case TimestampType =>
        to += from.getAs[Timestamp](ordinal)
      case BinaryType =>
        to += from.getAs[Array[Byte]](ordinal)
      case _: ArrayType | _: StructType | _: MapType =>
        val hiveString = HiveUtils.toHiveString((from.get(ordinal), dataTypes(ordinal)))
        to += hiveString
    }
  }

  def handle(ctx: ChannelHandlerContext, msg: BaseProtocol.FetchQueryResultRequest): RowBasedSet = {

    info("Trying to fetch results")

    val resultRowSet: RowBasedSet = new RowBasedSet(getSchema())

    if (iter.hasNext) {
      // maxRowsL here typically maps to java.sql.Statement.getFetchSize, which is an int
      val maxRows = 1000
      var curRow = 0
      while (curRow < maxRows && iter.hasNext) {
        val sparkRow = iter.next()
        val row = ArrayBuffer[Any]()
        var curCol = 0
        while (curCol < sparkRow.length) {
          if (sparkRow.isNullAt(curCol)) {
            row += null
          } else {
            addNonNullColumnValue(sparkRow, row, curCol)
          }
          curCol += 1
        }
        resultRowSet.addRow(row.toArray.asInstanceOf[Array[Object]])
        curRow += 1
      }
    }

    resultRowSet
  }
}
