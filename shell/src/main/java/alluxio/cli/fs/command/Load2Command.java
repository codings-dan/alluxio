/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.cli.fs.command;

import static alluxio.util.FormatUtils.getSizeFromBytes;

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.cli.fs.FileSystemShellUtils;
import alluxio.client.block.BlockStoreClient;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.BlockLocationPolicy;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.InStreamOptions;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.CacheBlockInfo;
import alluxio.grpc.CachesRequest;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.CloseableResource;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.util.CommonUtils;
import alluxio.util.FileSystemOptions;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Loads a file or directory in Alluxio space, making it resident in Alluxio.
 */
@ThreadSafe
@PublicApi
public final class Load2Command extends AbstractFileSystemCommand {
  private static final Option LOCAL_OPTION =
      Option.builder()
          .longOpt("local")
          .required(false)
          .hasArg(false)
          .desc("load the file to local worker.")
          .build();
  private static final Option BATCH_SIZE_OPTION =
      Option.builder()
          .longOpt("batch-size")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .type(Number.class)
          .argName("batch-size")
          .desc("Number of files per request")
          .build();
  private static final Option MAX_ACTIVES =
      Option.builder()
          .longOpt("actives")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .type(Number.class)
          .argName("actives")
          .desc("Max actives")
          .build();
  private static final Option THREADS =
      Option.builder()
          .longOpt("threads")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .type(Number.class)
          .argName("threads")
          .desc("threads num")
          .build();
  private static final Option QUIET =
      Option.builder("quiet")
          .longOpt("quiet")
          .required(false)
          .hasArg(false)
          .desc("Do not print detail task info")
          .build();

  private class TaskInfo {
    private final AtomicLong mCount = new AtomicLong();
    private final AtomicLong mLength = new AtomicLong();

    public void add(long count, long length) {
      mCount.addAndGet(count);
      mLength.addAndGet(length);
    }

    public long getCount() {
      return mCount.get();
    }

    public long getLength() {
      return mLength.get();
    }
  }

  // Whether to output detailed information
  boolean mQuiet = false;
  int mBatchSize;
  // The maximum number of simultaneous tasks allowed by the cluster
  int mMaxActives;
  // Number of tasks currently in progress
  AtomicInteger mActives;

  List<Future<?>> mTaskFutures = new LinkedList<>();
  ExecutorService mExecutor;
  Map<WorkerNetAddress, LinkedBlockingQueue<CacheBlockInfo>> mWorkerCacheRequestsMap =
      new HashMap<>();
  Map<WorkerNetAddress, AtomicInteger> mWorksCommitTasksCountMap = new HashMap<>();
  Map<WorkerNetAddress, TaskInfo> mWorksFailedTasksInfoMap = new HashMap<>();
  Map<WorkerNetAddress, TaskInfo> mWorksCompletedTasksInfoMap = new HashMap<>();

  /**
   * Constructs a new instance to load a file or directory in Alluxio space.
   *
   * @param fsContext the filesystem of Alluxio
   */
  public Load2Command(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "load2";
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(LOCAL_OPTION)
        .addOption(BATCH_SIZE_OPTION)
        .addOption(THREADS)
        .addOption(MAX_ACTIVES)
        .addOption(QUIET);
  }

  @Override
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    load(plainPath, cl.hasOption(LOCAL_OPTION.getLongOpt()));
  }

  private void init(CommandLine cl) throws IOException {
    List<BlockWorkerInfo> cachedWorkers = mFsContext.getCachedWorkers();
    if (cachedWorkers.isEmpty()) {
      throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
    }
    mBatchSize = FileSystemShellUtils.getIntArg(cl, BATCH_SIZE_OPTION, 50);
    mMaxActives = FileSystemShellUtils.getIntArg(cl, MAX_ACTIVES, 1000 * cachedWorkers.size());
    mQuiet = cl.hasOption(QUIET.getLongOpt());

    for (BlockWorkerInfo blockWorkerInfo: cachedWorkers) {
      mWorkerCacheRequestsMap.put(blockWorkerInfo.getNetAddress(), new LinkedBlockingQueue<>());
      mWorksCommitTasksCountMap.put(blockWorkerInfo.getNetAddress(), new AtomicInteger());
      mWorksCompletedTasksInfoMap.put(blockWorkerInfo.getNetAddress(), new TaskInfo());
      mWorksFailedTasksInfoMap.put(blockWorkerInfo.getNetAddress(), new TaskInfo());
    }
    int threadsNum = FileSystemShellUtils.getIntArg(cl, THREADS,
        Math.min(8 * Runtime.getRuntime().availableProcessors(), 4 * cachedWorkers.size()));
    mExecutor = Executors.newFixedThreadPool(threadsNum);

    System.out.printf("Task parameter: %s: %s, %s: %s, %s: %s, %s: %s, %s: %s %n",
        BATCH_SIZE_OPTION.getLongOpt(), mBatchSize,
        THREADS.getLongOpt(), threadsNum,
        MAX_ACTIVES.getLongOpt(), mMaxActives,
        LOCAL_OPTION.getLongOpt(), cl.hasOption(LOCAL_OPTION.getLongOpt()),
        QUIET.getLongOpt(), mQuiet);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    init(cl);
    runWildCardCmd(path, cl);
    mExecutor.shutdown();
    while (mExecutor.isTerminated()) {
      CommonUtils.sleepMs(10);
    }
    outputResult();
    return 0;
  }

  private void outputResult() {
    long completedCount = 0;
    long completedLength = 0;
    long failedLoad = 0;
    long failedLength = 0;
    for (Map.Entry<WorkerNetAddress, AtomicInteger> entry : mWorksCommitTasksCountMap.entrySet()) {
      System.out.println("Summarize:");
      System.out.print("  " + entry.getKey().getHost()
          + "\tloaded successfully: " + mWorksCompletedTasksInfoMap.get(entry.getKey()).getCount()
          + " size: "
          + getSizeFromBytes(mWorksCompletedTasksInfoMap.get(entry.getKey()).getLength())
          + "\tloaded failed: " + mWorksFailedTasksInfoMap.get(entry.getKey()).getCount()
          + " size: " + getSizeFromBytes(mWorksFailedTasksInfoMap.get(entry.getKey()).getLength()));
      System.out.println();
      completedCount += mWorksCompletedTasksInfoMap.get(entry.getKey()).getCount();
      completedLength += mWorksCompletedTasksInfoMap.get(entry.getKey()).getLength();
      failedLoad += mWorksFailedTasksInfoMap.get(entry.getKey()).getCount();
      failedLength += mWorksFailedTasksInfoMap.get(entry.getKey()).getLength();
    }
    System.out.printf("  All loaded successfully count: %s size: %s\tfailed count: %s size: %s%n",
        completedCount, getSizeFromBytes(completedLength),
        failedLoad, getSizeFromBytes(failedLength));
  }

  /**
   * Loads a file or directory in Alluxio space, makes it resident in Alluxio.
   *
   * @param filePath The {@link AlluxioURI} path to load into Alluxio
   * @param local whether to load data to local worker even when the data is already loaded remotely
   */
  private void load(AlluxioURI filePath, boolean local)
      throws AlluxioException, IOException {
    mActives = new AtomicInteger();
    ListStatusPOptions options = ListStatusPOptions.newBuilder().setRecursive(true).build();
    mFileSystem.iterateStatus(filePath, options, uriStatus -> {
      if (!uriStatus.isFolder()) {
        if (!uriStatus.isCompleted()) {
          System.out.printf("Ignored load because: %s is in incomplete status%n",
              uriStatus.getPath());
          return;
        }
        if (local) {
          try {
            if (!mFsContext.hasNodeLocalWorker()) {
              String msg =
                  "When local option is specified, there must be a local worker available%n";
              System.out.println(msg);
              throw new RuntimeException(msg);
            }
          } catch (IOException e) {
            System.out.printf("%s%n", e);
            throw new RuntimeException(e);
          }
        } else if (uriStatus.getInAlluxioPercentage() == 100 && uriStatus.getLength() != 0) {
          // The file has already been fully loaded into Alluxio.
          if (!mQuiet) {
            System.out.println(uriStatus.getPath() + " already in Alluxio fully");
          }
          return;
        }
        try {
          runLoadTask(filePath, uriStatus, local);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });

    waitForTask();
    submitLeftRequest();
    System.out.printf("load %s Successfully %n", filePath);
  }

  private void runLoadTask(AlluxioURI filePath, URIStatus status, boolean local)
      throws IOException {
    AlluxioConfiguration conf = mFsContext.getPathConf(filePath);
    OpenFilePOptions options = FileSystemOptions.openFileDefaults(conf);
    BlockLocationPolicy policy = Preconditions.checkNotNull(
        BlockLocationPolicy.Factory
            .create(conf.getClass(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY), conf),
        "UFS read location policy Required when loading files");
    WorkerNetAddress dataSource;
    List<Long> blockIds = status.getBlockIds();
    for (long blockId : blockIds) {
      if (local) {
        dataSource = mFsContext.getNodeLocalWorker();
      } else { // send request to data source
        BlockStoreClient blockStore = BlockStoreClient.create(mFsContext);
        Pair<WorkerNetAddress, BlockInStream.BlockInStreamSource> dataSourceAndType = blockStore
            .getDataSourceAndType(status.getBlockInfo(blockId), status, policy, ImmutableMap.of());
        dataSource = dataSourceAndType.getFirst();
      }
      Protocol.OpenUfsBlockOptions openUfsBlockOptions =
          new InStreamOptions(status, options, conf).getOpenUfsBlockOptions(blockId);
      cacheBlock(blockId, dataSource, status, openUfsBlockOptions);
    }
  }

  @Override
  public String getUsage() {
    return "load2 [--local]  [--batch-size <num>] "
        + " [--actives <num>] [--threads <num>] [--detail] <path> ";
  }

  @Override
  public String getDescription() {
    return "Loads a file or directory in Alluxio space, makes it resident in Alluxio.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
  }

  private void cacheBlock(long blockId, WorkerNetAddress dataSource, URIStatus status,
      Protocol.OpenUfsBlockOptions options) {
    BlockInfo info = status.getBlockInfo(blockId);
    long blockLength = info.getLength();
    CacheBlockInfo blockInfo = CacheBlockInfo.newBuilder()
        .setBlockId(blockId).setLength(blockLength)
        .setOpenUfsBlockOptions(options).build();

    submitCacheRequest(dataSource, blockInfo);
  }

  private void submitCacheRequest(WorkerNetAddress worker, CacheBlockInfo blockInfo) {
    while (mActives.intValue() > mMaxActives) {
      CommonUtils.sleepMs(5);
    }
    mActives.incrementAndGet();
    mWorkerCacheRequestsMap.get(worker).add(blockInfo);
    mWorksCommitTasksCountMap.get(worker).incrementAndGet();
    if (mWorksCommitTasksCountMap.get(worker).get() % mBatchSize == 0) {
      submitBatchRequest(worker);
    }
  }

  private void submitLeftRequest() {
    for (WorkerNetAddress worker :mWorkerCacheRequestsMap.keySet()) {
      submitBatchRequest(worker);
      waitForTask();
    }
  }

  private void submitBatchRequest(WorkerNetAddress worker) {
    Future<?> future = mExecutor.submit(new CacheTask(worker));
    mTaskFutures.add(future);
  }

  private void waitForTask() {
    for (Future<?> taskFuture :mTaskFutures) {
      while (!taskFuture.isDone()) {
        CommonUtils.sleepMs(5);
      }
    }
    mTaskFutures.clear();
  }

  public class CacheTask implements Runnable {

    private final WorkerNetAddress mWorkerNetAddress;

    public CacheTask(WorkerNetAddress workerNetAddress) {
      mWorkerNetAddress = workerNetAddress;
    }

    private boolean sendCachesRequestWithRetry(CloseableResource<BlockWorkerClient> blockWorker ,
        List<CacheBlockInfo> pool, String dataSource) {
      int reTryTimes = 3;
      RetryPolicy retryPolicy = new CountingRetry(reTryTimes);
      while (retryPolicy.attempt()) {
        CachesRequest cachesRequest =
            CachesRequest.newBuilder().addAllCacheBlockInfo(pool).setSourceHost(dataSource)
                .setSourcePort(mWorkerNetAddress.getDataPort()).build();
        try {
          blockWorker.get().caches(cachesRequest);
        } catch (Exception e) {
          System.out.printf("Failed to complete caches request for the Worker %s, "
              + "AttemptCount: %d%n", dataSource, retryPolicy.getAttemptCount());
          e.printStackTrace();
          if (retryPolicy.getAttemptCount() > reTryTimes) {
            return false;
          }
          continue;
        }
        break;
      }
      return true;
    }

    @Override
    public void run() {
      LinkedBlockingQueue<CacheBlockInfo> cacheBlockInfos =
          mWorkerCacheRequestsMap.get(mWorkerNetAddress);
      String dataSource = mWorkerNetAddress.getHost();
      // issues#11172: If the worker is in a container, use the container hostname
      // to establish the connection.
      if (!mWorkerNetAddress.getContainerHost().equals("")) {
        dataSource = mWorkerNetAddress.getContainerHost();
      }
      List<CacheBlockInfo> pool = new LinkedList<>();
      final long[] taskLength = {0};
      IntStream.range(0, mBatchSize).forEach(ignore -> {
        CacheBlockInfo blockInfo = cacheBlockInfos.poll();
        if (blockInfo != null) {
          taskLength[0] += blockInfo.getLength();
          pool.add(blockInfo);
        }
      });

      boolean successfully = false;
      try (CloseableResource<BlockWorkerClient> blockWorker = mFsContext.acquireBlockWorkerClient(
          mWorkerNetAddress)) {
        successfully = sendCachesRequestWithRetry(blockWorker, pool, dataSource);
      } catch (Exception e) {
        System.out.printf("Failed to complete cache request for the Worker %s: %n",
            mWorkerNetAddress.getHost());
        e.printStackTrace();
      } finally {
        if (!mQuiet) {
          System.out.print(mWorkerNetAddress.getHost()
              + "  loaded successfully: "
              + mWorksCompletedTasksInfoMap.get(mWorkerNetAddress).getCount()
              + " size: "
              + getSizeFromBytes(mWorksCompletedTasksInfoMap.get(mWorkerNetAddress).getLength())
              + "  loaded failed: " + mWorksFailedTasksInfoMap.get(mWorkerNetAddress).getCount()
              + " size :"
              + getSizeFromBytes(mWorksFailedTasksInfoMap.get(mWorkerNetAddress).getLength()));
          System.out.println();
        }
        if (successfully) {
          mWorksCompletedTasksInfoMap.get(mWorkerNetAddress).add(pool.size(), taskLength[0]);
        } else {
          mWorksFailedTasksInfoMap.get(mWorkerNetAddress).add(pool.size(), 0);
        }
        mActives.addAndGet(-pool.size());
        pool.clear();
      }
    }
  }
}
