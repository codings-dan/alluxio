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

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.cli.fs.FileSystemShellUtils;
import alluxio.client.block.AlluxioBlockStore;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
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

  int mBatchSize;
  boolean mLeftJobFlag = false;
  AtomicInteger mActives = new AtomicInteger();
  int mMaxActives;
  int mReTryTimes = 3;
  Map<WorkerNetAddress, LinkedBlockingQueue<CacheBlockInfo>> mWorkerCacheRequestsMap =
      new HashMap<>();

  public class CacheTask implements Runnable {

    private WorkerNetAddress mWorkerNetAddress;

    public CacheTask(WorkerNetAddress workerNetAddress) {
      mWorkerNetAddress = workerNetAddress;
    }

    @Override
    public void run() {
      LinkedBlockingQueue<CacheBlockInfo> cacheBlockInfos =
          mWorkerCacheRequestsMap.get(mWorkerNetAddress);
      String host = mWorkerNetAddress.getHost();
      // issues#11172: If the worker is in a container, use the container hostname
      // to establish the connection.
      if (!mWorkerNetAddress.getContainerHost().equals("")) {
        host = mWorkerNetAddress.getContainerHost();
      }
      List<CacheBlockInfo> pool = new LinkedList<>();
      int finished = 0;
      int failed = 0;

      try (CloseableResource<BlockWorkerClient> blockWorker = mFsContext.acquireBlockWorkerClient(
          mWorkerNetAddress)) {
        while (true) {
          if (cacheBlockInfos.size() >= mBatchSize || mLeftJobFlag) {
            RetryPolicy retryPolicy = new CountingRetry(mReTryTimes);
            cacheBlockInfos.stream().limit(mBatchSize)
                .forEach(ignore -> pool.add(cacheBlockInfos.remove()));

            while (retryPolicy.attempt()) {
              CachesRequest cachesRequest =
                  CachesRequest.newBuilder().addAllCacheBlockInfo(pool).setSourceHost(host)
                      .setSourcePort(mWorkerNetAddress.getDataPort()).build();
              try {
                blockWorker.get().caches(cachesRequest);
              } catch (Exception e) {
                System.out.printf("Failed to complete caches request for the Worker %s, "
                    + "Retry: %d%n", host, retryPolicy.getAttemptCount());
                e.printStackTrace();
              }
            }
            if (retryPolicy.getAttemptCount() == mReTryTimes) {
              failed += pool.size();
            }
            finished += pool.size();
            pool.clear();
            mActives.addAndGet(-mBatchSize);
            System.out.printf(
                "The worker %s cached num: %d failed num: %d\r", host, finished, failed);
          } else {
            CommonUtils.sleepMs(10);
          }
          if (mLeftJobFlag && cacheBlockInfos.isEmpty()) {
            return;
          }
        }
      } catch (Exception e) {
        System.out.printf("Failed to complete cache request for the Worker %s: %n", host);
        e.printStackTrace();
      }
    }
  }

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
    return new Options().addOption(LOCAL_OPTION).addOption(BATCH_SIZE_OPTION);
  }

  @Override
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    load(plainPath, cl.hasOption(LOCAL_OPTION.getLongOpt()));
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    mBatchSize = FileSystemShellUtils.getIntArg(cl, BATCH_SIZE_OPTION, 1);
    List<BlockWorkerInfo> cachedWorkers = mFsContext.getCachedWorkers();
    if (cachedWorkers.isEmpty()) {
      throw new UnavailableException(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage());
    }
    mMaxActives = cachedWorkers.size() * mBatchSize;

    for (BlockWorkerInfo blockWorkerInfo: cachedWorkers) {
      mWorkerCacheRequestsMap.put(blockWorkerInfo.getNetAddress(), new LinkedBlockingQueue<>());
      Thread thread = new Thread(new CacheTask(blockWorkerInfo.getNetAddress()));
      thread.start();
    }

    runWildCardCmd(path, cl);
    return 0;
  }

  /**
   * Loads a file or directory in Alluxio space, makes it resident in Alluxio.
   *
   * @param filePath The {@link AlluxioURI} path to load into Alluxio
   * @param local whether to load data to local worker even when the data is already loaded remotely
   */
  private void load(AlluxioURI filePath, boolean local)
      throws AlluxioException, IOException {
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
        } else if (uriStatus.getInAlluxioPercentage() == 100) {
          // The file has already been fully loaded into Alluxio.
          System.out.println(uriStatus.getPath() + " already in Alluxio fully");
          return;
        }
        try {
          runLoadTask(filePath, uriStatus, local);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });
    mLeftJobFlag = true;
  }

  private void runLoadTask(AlluxioURI filePath, URIStatus status, boolean local)
      throws IOException {
    AlluxioConfiguration conf = mFsContext.getPathConf(filePath);
    OpenFilePOptions options = FileSystemOptions.openFileDefaults(conf);
    BlockLocationPolicy policy = Preconditions.checkNotNull(
        BlockLocationPolicy.Factory
            .create(conf.get(PropertyKey.USER_UFS_BLOCK_READ_LOCATION_POLICY), conf),
        "UFS read location policy Required when loading files");
    WorkerNetAddress dataSource;
    List<Long> blockIds = status.getBlockIds();
    for (long blockId : blockIds) {
      if (local) {
        dataSource = mFsContext.getNodeLocalWorker();
      } else { // send request to data source
        AlluxioBlockStore blockStore = AlluxioBlockStore.create(mFsContext);
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
    return "load2 [--local]  [--batch-size <num>] <path> ";
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
    String host = dataSource.getHost();
    // issues#11172: If the worker is in a container, use the container hostname
    // to establish the connection.
    if (!dataSource.getContainerHost().equals("")) {
      host = dataSource.getContainerHost();
    }
    CacheBlockInfo blockInfo = CacheBlockInfo.newBuilder()
        .setBlockId(blockId).setLength(blockLength)
        .setOpenUfsBlockOptions(options).build();

    submitCacheRequest(dataSource, blockInfo);
  }

  private void submitCacheRequest(WorkerNetAddress dataSource, CacheBlockInfo blockInfo) {
    while (mActives.intValue() > mMaxActives) {
      CommonUtils.sleepMs(10);
    }
    mActives.incrementAndGet();
    mWorkerCacheRequestsMap.get(dataSource).add(blockInfo);
  }
}
