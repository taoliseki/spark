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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver code for the Spark client library.
 */
@Sharable
public abstract class RSCDriver extends BaseProtocol {

  private static final Logger LOG = LoggerFactory.getLogger(RSCDriver.class);

  private final Object jcLock;
  private final Object shutdownLock;
  private final ExecutorService executor;
  private final File localTmpDir;
  // Used to queue up requests while the SparkContext is being created.
  //private final List<JobWrapper<?>> jobQueue;
  // Keeps track of connected clients.
  private final Collection<Rpc> clients;
  //final Map<String, JobWrapper<?>> activeJobs;

  private RpcServer server;
  //private volatile JobContextImpl jc;
  private volatile boolean running;

  protected final SparkConf conf;
  protected final RSCConf livyConf;

  private final AtomicReference<ScheduledFuture<?>> idleTimeout;

  public RSCDriver(SparkConf conf, RSCConf livyConf) throws Exception {
    Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwx------");
    this.localTmpDir = Files.createTempDirectory("rsc-tmp",
      PosixFilePermissions.asFileAttribute(perms)).toFile();
    this.executor = Executors.newCachedThreadPool();
    this.clients = new ConcurrentLinkedDeque<>();

    this.conf = conf;
    this.livyConf = livyConf;
    this.jcLock = new Object();
    this.shutdownLock = new Object();

    this.idleTimeout = new AtomicReference<>();
  }

  private synchronized void shutdown() {
    if (!running) {
      return;
    }

    running = false;

    try {
      shutdownContext();
    } catch (Exception e) {
      LOG.warn("Error during shutdown.", e);
    }
    try {
      shutdownServer();
    } catch (Exception e) {
      LOG.warn("Error during shutdown.", e);
    }

    synchronized (shutdownLock) {
      shutdownLock.notifyAll();
    }
    synchronized (jcLock) {
      jcLock.notifyAll();
    }
  }

  private void initializeServer() throws Exception {
    String clientId = livyConf.get(RSCConf.Entry.CLIENT_ID);
    Utils.checkArgument(clientId != null, "No client ID provided.");
    String secret = livyConf.get(RSCConf.Entry.CLIENT_SECRET);
    Utils.checkArgument(secret != null, "No secret provided.");

    String launcherAddress = livyConf.get(RSCConf.Entry.LAUNCHER_ADDRESS);
    Utils.checkArgument(launcherAddress != null, "Missing launcher address.");
    int launcherPort = livyConf.getInt(RSCConf.Entry.LAUNCHER_PORT);
    Utils.checkArgument(launcherPort > 0, "Missing launcher port.");

    LOG.info("Connecting to: {}:{}", launcherAddress, launcherPort);

    // We need to unset this configuration since it doesn't really apply for the driver side.
    // If the driver runs on a multi-homed machine, this can lead to issues where the Livy
    // server cannot connect to the auto-detected address, but since the driver can run anywhere
    // on the cluster, it would be tricky to solve that problem in a generic way.
    livyConf.set(RSCConf.Entry.RPC_SERVER_ADDRESS, null);

    // Bring up the RpcServer an register the secret provided by the Livy server as a client.
    LOG.info("Starting RPC server...");
    this.server = new RpcServer(livyConf);
    server.registerClient(clientId, secret, new RpcServer.ClientCallback() {
      @Override
      public RpcDispatcher onNewClient(Rpc client) {
        registerClient(client);
        return RSCDriver.this;
      }
    });

    // The RPC library takes care of timing out this.
    Rpc callbackRpc = Rpc.createClient(livyConf, server.getEventLoopGroup(),
      launcherAddress, launcherPort, clientId, secret, this).get();
    try {
      callbackRpc.call(new RemoteDriverAddress(server.getAddress(), server.getPort())).get(
        livyConf.getTimeAsMs(RSCConf.Entry.RPC_CLIENT_HANDSHAKE_TIMEOUT), TimeUnit.MILLISECONDS);
    } catch (TimeoutException te) {
      LOG.warn("Timed out sending address to Livy server, shutting down.");
      throw te;
    } finally {
      callbackRpc.close();
    }

    // At this point we install the idle timeout handler, in case the Livy server fails to connect
    // back.
    setupIdleTimeout();
  }

  private void registerClient(final Rpc client) {
    clients.add(client);
    stopIdleTimeout();

    Utils.addListener(client.getChannel().closeFuture(), new FutureListener<Void>() {
      @Override
      public void onSuccess(Void unused) {
        clients.remove(client);
        setupIdleTimeout();
      }
    });
    LOG.debug("Registered new connection from {}.", client.getChannel());
  }

  private void setupIdleTimeout() {
    if (clients.size() > 0) {
      return;
    }

    Runnable timeoutTask = new Runnable() {
      @Override
      public void run() {
        LOG.warn("Shutting down RSC due to idle timeout ({}).", livyConf.get(RSCConf.Entry.SERVER_IDLE_TIMEOUT));
        shutdown();
      }
    };
    ScheduledFuture<?> timeout = server.getEventLoopGroup().schedule(timeoutTask,
      livyConf.getTimeAsMs(RSCConf.Entry.SERVER_IDLE_TIMEOUT), TimeUnit.MILLISECONDS);

    // If there's already an idle task registered, then cancel the new one.
    if (!this.idleTimeout.compareAndSet(null, timeout)) {
      LOG.debug("Timeout task already registered.");
      timeout.cancel(false);
    }

    // If a new client connected while the idle task was being set up, then stop the task.
    if (clients.size() > 0) {
      stopIdleTimeout();
    }
  }

  private void stopIdleTimeout() {
    ScheduledFuture<?> idleTimeout = this.idleTimeout.getAndSet(null);
    if (idleTimeout != null) {
      LOG.debug("Cancelling idle timeout since new client connected.");
      idleTimeout.cancel(false);
    }
  }

  /**
   * Initializes the SparkContext used by this driver. This implementation creates a
   * context with the provided configuration. Subclasses can override this behavior,
   * and returning a null context is allowed. In that case, the context exposed by
   * JobContext will be null.
   */
  protected abstract void initializeContext();

  /**
   * Called to shut down the driver; any initialization done by initializeContext() should
   * be undone here. This is guaranteed to be called only once.
   */
  protected void shutdownContext() {
    executor.shutdownNow();
    try {
      FileUtils.deleteDirectory(localTmpDir);
    } catch (IOException e) {
      LOG.warn("Failed to delete local tmp dir: " + localTmpDir, e);
    }
  }

  private void shutdownServer() {
    if (server != null) {
      server.close();
    }
    for (Rpc client: clients) {
      client.close();
    }
  }

  private void broadcast(Object msg) {
    for (Rpc client : clients) {
      try {
        client.call(msg);
      } catch (Exception e) {
        LOG.warn("Failed to send message to client " + client, e);
      }
    }
  }

  void run() throws Exception {
    this.running = true;

    // Set up a class loader that can be modified, so that we can add jars uploaded
    // by the client to the driver's class path.
    ClassLoader driverClassLoader = new MutableClassLoader(
      Thread.currentThread().getContextClassLoader());
    Thread.currentThread().setContextClassLoader(driverClassLoader);

    try {
      initializeServer();

      initializeContext();

      synchronized (shutdownLock) {
        try {
          while (running) {
            shutdownLock.wait();
          }
        } catch (InterruptedException ie) {
          // Nothing to do.
        }
      }
    } finally {
      shutdown();
    }
  }

  /*public void submit(JobWrapper<?> job) {
    if (jc != null) {
      job.submit(executor);
      return;
    }
    synchronized (jcLock) {
      if (jc != null) {
        job.submit(executor);
      } else {
        LOG.info("SparkContext not yet up, queueing job request.");
        jobQueue.add(job);
      }
    }
  }

  JobContextImpl jobContext() {
    return jc;
  }

  <T> void jobFinished(String jobId, T result, Throwable error) {
    LOG.debug("Send job({}) result to Client.", jobId);
    broadcast(new JobResult<T>(jobId, result, error));
  }

  void jobStarted(String jobId) {
    broadcast(new JobStarted(jobId));
  }

  public void handle(ChannelHandlerContext ctx, BaseProtocol.CancelJob msg) {
    JobWrapper<?> job = activeJobs.get(msg.id);
    if (job == null || !job.cancel()) {
      LOG.info("Requested to cancel an already finished job.");
    }
  }


  public void handle(ChannelHandlerContext ctx, BaseProtocol.JobRequest<?> msg) {
    LOG.info("Received job request {}", msg.id);
    JobWrapper<?> wrapper = new JobWrapper<>(this, msg.id, msg.job);
    activeJobs.put(msg.id, wrapper);
    submit(wrapper);
  }


  @SuppressWarnings("unchecked")
  public Object handle(ChannelHandlerContext ctx, BaseProtocol.SyncJobRequest msg) throws Exception {
    waitForJobContext();
    return msg.job.call(jc);
  }

  private void waitForJobContext() throws InterruptedException {
    synchronized (jcLock) {
      while (jc == null) {
        jcLock.wait();
        if (!running) {
          throw new IllegalStateException("Remote context is shutting down.");
        }
      }
    }
  }

  protected void addFile(String path) {
    jc.sc().addFile(path);
  }

  protected void addJarOrPyFile(String path) throws Exception {
    File localCopyDir = new File(jc.getLocalTmpDir(), "__livy__");
    File localCopy = copyFileToLocal(localCopyDir, path, jc.sc().sc());
    addLocalFileToClassLoader(localCopy);
    jc.sc().addJar(path);
  }

  public void addLocalFileToClassLoader(File localCopy) throws MalformedURLException {
    MutableClassLoader cl = (MutableClassLoader) Thread.currentThread().getContextClassLoader();
    cl.addURL(localCopy.toURI().toURL());
  }

  public File copyFileToLocal(
      File localCopyDir,
      String filePath,
      SparkContext sc) throws Exception {
    synchronized (jc) {
      if (!localCopyDir.isDirectory() && !localCopyDir.mkdir()) {
        throw new IOException("Failed to create directory to add pyFile");
      }
    }
    URI uri = new URI(filePath);
    String name = uri.getFragment() != null ? uri.getFragment() : uri.getPath();
    name = new File(name).getName();
    File localCopy = new File(localCopyDir, name);

    if (localCopy.exists()) {
      throw new IOException(String.format("A file with name %s has " +
              "already been uploaded.", name));
    }
    Configuration conf = sc.hadoopConfiguration();
    FileSystem fs = FileSystem.get(uri, conf);
    fs.copyToLocalFile(new Path(uri), new Path(localCopy.toURI()));
    return localCopy;
  }*/
}
