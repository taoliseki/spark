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

package org.apache.spark.sql.hive.thriftserver

import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import java.security.PrivilegedExceptionAction
import java.sql.{Date, Timestamp}
import java.util.{Arrays, UUID, Map => JMap}
import java.util.concurrent.RejectedExecutionException

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, Map => SMap}
import scala.util.control.NonFatal

import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.shims.Utils
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.ExecuteStatementOperation
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext, Row => SparkRow}
import org.apache.spark.sql.execution.command.SetCommand
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.thriftserver.rsc.RSCClient
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.{Utils => SparkUtils}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.hive.thriftserver.rsc.Serializer

private[hive] class SparkExecuteStatementOperation(
    parentSession: HiveSession,
    statement: String,
    confOverlay: JMap[String, String],
    runInBackground: Boolean = true)
    (rscClient: RSCClient, sessionToActivePool: SMap[SessionHandle, String])
  extends ExecuteStatementOperation(parentSession, statement, confOverlay, runInBackground)
  with Logging {

  private var result: DataFrame = _
  private var iter: Iterator[SparkRow] = _
  private var iterHeader: Iterator[SparkRow] = _
  private var dataTypes: Array[DataType] = _
  private var statementId: String = _

  private lazy val resultSchema: TableSchema = {
    if (result == null || result.schema.isEmpty) {
      new TableSchema(Arrays.asList(new FieldSchema("Result", "string", "")))
    } else {
      logInfo(s"Result Schema: ${result.schema}")
      SparkExecuteStatementOperation.getTableSchema(result.schema)
    }
  }
  
  def close(): Unit = {
    // RDDs will be cleaned automatically upon garbage collection.
    logDebug(s"CLOSING $statementId")
    cleanup(OperationState.CLOSED)
  }

  def getNextRowSet(order: FetchOrientation, maxRowsL: Long): RowSet = {
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)

    val res = rscClient.fetchResult()
    logInfo("Number of columns from the execution result: " + res.numColumns())

    val iter = res.iterator();

    while(iter.hasNext())
    {
      logInfo("tlitest query res row: " + iter.next())
    }

    res
  }

  def getResultSetSchema: TableSchema = resultSchema


  override def runInternal(): Unit = {
    setState(OperationState.PENDING)
    setHasResultSet(true) // avoid no resultset for async run

    if (!runInBackground) {
      execute()
    } else {
      val sparkServiceUGI = Utils.getUGI()

      // Runnable impl to call runInternal asynchronously,
      // from a different thread
      val backgroundOperation = new Runnable() {

        override def run(): Unit = {
          val doAsAction = new PrivilegedExceptionAction[Unit]() {
            override def run(): Unit = {
              try {
                execute()
              } catch {
                case e: HiveSQLException =>
                  setOperationException(e)
                  log.error("Error running hive query: ", e)
              }
            }
          }

          try {
            sparkServiceUGI.doAs(doAsAction)
          } catch {
            case e: Exception =>
              setOperationException(new HiveSQLException(e))
              logError("Error running hive query as user : " +
                sparkServiceUGI.getShortUserName(), e)
          }
        }
      }
      try {
        // This submit blocks if no background threads are available to run this operation
        val backgroundHandle =
          parentSession.getSessionManager().submitBackgroundOperation(backgroundOperation)
        setBackgroundHandle(backgroundHandle)
      } catch {
        case rejected: RejectedExecutionException =>
          setState(OperationState.ERROR)
          throw new HiveSQLException("The background threadpool cannot accept" +
            " new task for execution, please retry the operation", rejected)
        case NonFatal(e) =>
          logError(s"Error executing query in background", e)
          setState(OperationState.ERROR)
          throw e
      }
    }
  }

  private def execute(): Unit = {
    statementId = UUID.randomUUID().toString
    logInfo(s"Running query '$statement' with $statementId")
    setState(OperationState.RUNNING)

    try {
      rscClient.executeSql(statement)

//      result.queryExecution.logical match {
//        case SetCommand(Some((SQLConf.THRIFTSERVER_POOL.key, Some(value)))) =>
//          sessionToActivePool(parentSession.getSessionHandle) = value
//          logInfo(s"Setting spark.scheduler.pool=$value for future statements in this session.")
//        case _ =>
//      }
      //HiveThriftServer2.listener.onStatementParsed(statementId, result.queryExecution.toString())
    } catch {
      case e: HiveSQLException =>

        logError(e.getMessage());

        if (getStatus().getState() == OperationState.CANCELED) {
          logInfo("tli canceled")
          return
        } else {
          setState(OperationState.ERROR)
          throw e
        }
      // Actually do need to catch Throwable as some failures don't inherit from Exception and
      // HiveServer will silently swallow them.
      case e: Throwable =>

        /*logError("tlitest message: " + e.getMessage());
        logError("tlitest cause: " + e.getCause());

        for(stack <- e.getStackTrace())
        {
          logError("tlitest stacktrace: " + stack);
        }*/

        val currentState = getStatus().getState()
        logError(s"Error executing query, currentState $currentState, ", e)
        setState(OperationState.ERROR)
        HiveThriftServer2.listener.onStatementError(
          statementId, e.getMessage, SparkUtils.exceptionString(e))
        throw new HiveSQLException(e.toString)
    }
    setState(OperationState.FINISHED)
    HiveThriftServer2.listener.onStatementFinish(statementId)
  }

  override def cancel(): Unit = {
    logInfo(s"Cancel '$statement' with $statementId")
    cleanup(OperationState.CANCELED)
  }

  private def cleanup(state: OperationState) {
    setState(state)
    if (runInBackground) {
      val backgroundHandle = getBackgroundHandle()
      if (backgroundHandle != null) {
        backgroundHandle.cancel(true)
      }
    }
  }
}

object SparkExecuteStatementOperation {
  def getTableSchema(structType: StructType): TableSchema = {
    val schema = structType.map { field =>
      val attrTypeString = if (field.dataType == NullType) "void" else field.dataType.catalogString
      new FieldSchema(field.name, attrTypeString, "")
    }
    new TableSchema(schema.asJava)
  }
}
