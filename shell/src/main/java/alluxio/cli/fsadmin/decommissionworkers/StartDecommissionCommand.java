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

package alluxio.cli.fsadmin.decommissionworkers;

import alluxio.cli.fsadmin.FileSystemAdminShellUtils;
import alluxio.cli.fsadmin.command.AbstractFsAdminCommand;
import alluxio.cli.fsadmin.command.Context;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.AlluxioStatusException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Start worker decommission process.
 */
public class StartDecommissionCommand extends AbstractFsAdminCommand {
  private static final Logger LOG = LoggerFactory.getLogger(StartDecommissionCommand.class);

  private static final Option HELP_OPTION =
      Option.builder("h")
          .required(false)
          .hasArg(false)
          .desc("print help information.")
          .build();
  private static final Option EXCLUDED_HOSTS_OPTION =
      Option.builder()
          .longOpt("excluded-hosts")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .argName("excluded-hosts")
          .desc("A list of excluded worker hosts separated by comma.")
          .build();
  private static final Option ADD_ONLY_OPTION =
      Option.builder("a")
          .required(false)
          .hasArg(false)
          .desc("Only add host to excluded hosts list, but do not remove host not mentioned.")
          .build();
  private final AlluxioConfiguration mAlluxioConf;

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public StartDecommissionCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    mAlluxioConf = alluxioConf;
  }

  private void readItemsFromOptionString(Set<String> localityIds,
      String argOption) {
    for (String locality : StringUtils.split(argOption, ",")) {
      locality = locality.trim();
      if (!locality.isEmpty()) {
        localityIds.add(locality);
      }
    }
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    Set<String> excludedWorkerSet = new HashSet<>();
    if (cl.hasOption(HELP_OPTION.getOpt())) {
      System.out.println(getUsage());
      System.out.println(getDescription());
      return 0;
    } else if (cl.hasOption(EXCLUDED_HOSTS_OPTION.getLongOpt())) {
      String argOption = cl.getOptionValue(EXCLUDED_HOSTS_OPTION.getLongOpt()).trim();
      readItemsFromOptionString(excludedWorkerSet, argOption);
    }
    FileSystemAdminShellUtils.checkMasterClientService(mAlluxioConf);
    try {
      mFsClient.decommissionWorkers(cl.hasOption(ADD_ONLY_OPTION.getOpt()) , excludedWorkerSet);
    } catch (AlluxioStatusException e) {
      LOG.error(e.getMessage(), e);
      return -1;
    }
    return 0;
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(HELP_OPTION)
        .addOption(EXCLUDED_HOSTS_OPTION)
        .addOption(ADD_ONLY_OPTION);
  }

  @Override
  public String getCommandName() {
    return "start";
  }

  @Override
  public String getUsage() {
    return "start [-h] [-a] [--excluded-hosts <host1>,<host2>,...,<hostN>]";
  }

  @Override
  public String getDescription() {
    return "Decommission workers defined by excluded-workers file and options. If there are "
        + "entries in excluded-workers, the hosts in it are need to be decommissioned. "
        + "Decommissioned Worker are not automatically shutdown and are not chosen for"
        + "writing new replicas.";
  }
}
