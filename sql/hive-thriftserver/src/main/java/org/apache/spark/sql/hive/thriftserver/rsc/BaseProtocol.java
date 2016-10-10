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

package org.apache.spark.sql.hive.thriftserver.rsc;

public abstract class BaseProtocol extends RpcDispatcher {

  protected static class Error {

    public final String cause;

    public Error(Throwable cause) {
      if (cause == null) {
        this.cause = "";
      } else {
        this.cause = Utils.stackTraceAsString(cause);
      }
    }

    public Error() {
      this(null);
    }

  }

  public static class RemoteDriverAddress {

    public final String host;
    public final int port;

    public RemoteDriverAddress(String host, int port) {
      this.host = host;
      this.port = port;
    }

    public RemoteDriverAddress() {
      this(null, -1);
    }

  }

  public static class SqlQueryRequest {

    public final String code;
    public final String id;

    public SqlQueryRequest(String code, String id) {
      this.code = code;
      this.id = id;
    }

    public SqlQueryRequest() {
      this(null, null);
    }
  }

  public static class FetchQueryResultRequest {

    public final String id;

    public FetchQueryResultRequest(String id) {
      this.id = id;
    }

    public FetchQueryResultRequest() {
      this(null);
    }

  }

  public static class InitializationError {

    public final String stackTrace;

    public InitializationError(String stackTrace) {
      this.stackTrace = stackTrace;
    }

    public InitializationError() {
      this(null);
    }

  }

}
