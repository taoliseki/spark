/*
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.rsc;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.objenesis.strategy.StdInstantiatorStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.network.util.JavaUtils;

import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowBasedSet;
import org.apache.hive.service.cli.thrift.TColumn;

import java.util.Arrays;
/**
 * Utility class to serialize user data using Kryo.
 */
public class Serializer {

  // Kryo docs say 0-8 are taken. Strange things happen if you don't set an ID when registering
  // classes.
  private static final int REG_ID_BASE = 16;

  private static ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
    @Override
    protected Kryo initialValue() {
      Kryo kryo = new Kryo();
      ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy())
              .setFallbackInstantiatorStrategy(
                      new StdInstantiatorStrategy());
      kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
      kryo.register(Rpc.MessageHeader.class);
      kryo.register(Rpc.NullMessage.class);
      kryo.register(Rpc.SaslMessage.class);
      kryo.register(RowBasedSet.class);

      return kryo;
    }
  };

  private static final Logger LOG = LoggerFactory.getLogger(Serializer.class);

  public Serializer(final Class<?>... klasses) {
  }

  public Object deserialize(ByteBuffer data) {
    byte[] b = new byte[data.remaining()];
    data.get(b);
    LOG.info("Received data length: " + b.length);
    Input kryoIn = new Input(b);
      return kryos.get().readClassAndObject(kryoIn);

  }



  public ByteBuffer serialize(Object data) {
    ByteBufferOutputStream out = new ByteBufferOutputStream();
    Output kryoOut = new Output(out);
    kryos.get().writeClassAndObject(kryoOut, data);
    kryoOut.flush();
    return out.getBuffer();
  }

  /*public Object deserializeFromString(String data) {
    LOG.info("tlitest deserializing data: " + data);
    ByteBuffer buffer = JavaUtils.stringToBytes(data);
    return deserialize(buffer);
  }

  public String serializeToString(Object data) {
    ByteBuffer buffer = serialize(data);
    String res = JavaUtils.bytesToString(buffer);
    LOG.info("tlitest serialized string: " + res);
    return res;
  }*/

  private static class ByteBufferOutputStream extends ByteArrayOutputStream {

    public ByteBuffer getBuffer() {
      ByteBuffer result = ByteBuffer.wrap(buf, 0, count);
      buf = null;
      reset();
      return result;
    }

  }

}
