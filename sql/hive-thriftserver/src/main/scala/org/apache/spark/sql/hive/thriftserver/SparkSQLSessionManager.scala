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

import java.net.URI
import java.util.concurrent.Executors

import scala.collection.mutable.Map

import org.apache.commons.logging.Log
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.service.cli.SessionHandle
import org.apache.hive.service.cli.session.SessionManager
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.hive.service.server.HiveServer2

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.{HiveSessionState, HiveUtils}
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._
import org.apache.spark.sql.hive.thriftserver.rsc.{RSCClientBuilder, RSCClient, RSCConf}
import org.apache.spark.sql.hive.thriftserver.rsc.RSCConf
import org.apache.spark.sql.hive.thriftserver.server.SparkSQLOperationManager

private[hive] class SparkSQLSessionManager(hiveServer: HiveServer2, sqlContext: SQLContext)
  extends SessionManager(hiveServer)
  with ReflectedCompositeService {

  private lazy val sparkSqlOperationManager = new SparkSQLOperationManager()
  private var isImpersonationEnabled: Boolean = false;

  override def init(hiveConf: HiveConf) {
    setSuperField(this, "hiveConf", hiveConf)

    // Create operation log root directory, if operation logging is enabled
    if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED)) {
      invoke(classOf[SessionManager], this, "initOperationLogRootDir")
    }

    val backgroundPoolSize = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_THREADS)
    setSuperField(this, "backgroundOperationPool", Executors.newFixedThreadPool(backgroundPoolSize))
    getAncestorField[Log](this, 3, "LOG").info(
      s"HiveServer2: Async execution pool size $backgroundPoolSize")

    setSuperField(this, "operationManager", sparkSqlOperationManager)
    addService(sparkSqlOperationManager)

    initCompositeService(hiveConf)
  }

  override def openSession(
      protocol: TProtocolVersion,
      username: String,
      passwd: String,
      ipAddress: String,
      sessionConf: java.util.Map[String, String],
      withImpersonation: Boolean,
      delegationToken: String): SessionHandle = {
    val sessionHandle =
      super.openSession(protocol, username, passwd, ipAddress, sessionConf, withImpersonation,
          delegationToken)
    val session = super.getSession(sessionHandle)
    HiveThriftServer2.listener.onSessionCreated(
      session.getIpAddress, sessionHandle.getSessionId.toString, session.getUsername)

    isImpersonationEnabled = withImpersonation

    if (withImpersonation)
    {
      val builder = new RSCClientBuilder()
        .setConf(RSCConf.Entry.DRIVER_CLASS.key(), "org.apache.spark.sql.hive.thriftserver.rsc.StsDriver")
        .setConf(RSCConf.Entry.PROXY_USER.key(), username)
        .setURI(new URI("rsc:/"))
      val client = builder.build().asInstanceOf[RSCClient]
      sparkSqlOperationManager.sessionToRscClinet += sessionHandle -> client
    } else {
      val sessionState = sqlContext.sessionState.asInstanceOf[HiveSessionState]
      val ctx = if (sessionState.hiveThriftServerSingleSession) {
        sqlContext
      } else {
        sqlContext.newSession()
      }
      ctx.setConf("spark.sql.hive.version", HiveUtils.hiveExecutionVersion)
      sparkSqlOperationManager.sessionToContexts += sessionHandle -> ctx
    }

    sessionHandle
  }

  override def closeSession(sessionHandle: SessionHandle) {
    HiveThriftServer2.listener.onSessionClosed(sessionHandle.getSessionId.toString)
    super.closeSession(sessionHandle)
    sparkSqlOperationManager.sessionToActivePool -= sessionHandle
    if (isImpersonationEnabled) {
      var map: Map[SessionHandle, RSCClient] = sparkSqlOperationManager.sessionToRscClinet
      map(sessionHandle).stop(true)
      sparkSqlOperationManager.sessionToRscClinet.remove(sessionHandle)
    } else {
      sparkSqlOperationManager.sessionToContexts.remove(sessionHandle)
    }
  }
}
