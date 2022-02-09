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

package alluxio.client.cli.fsadmin.command;

import alluxio.cli.fsadmin.command.DecommissionWorkersCommand;
import alluxio.client.cli.fsadmin.AbstractFsAdminShellTest;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for decommissionWorkers command.
 */
public final class DecommissionWorkersCommandIntegrationTest extends AbstractFsAdminShellTest {
  // TODO(bingbzheng): add more test function, later add status command and stop command test.
  @Test
  public void decommissionWorkers() {
    // Validate decommissionWorkers sub-commands are validated.
    int ret;
    mOutput.reset();
    mFsAdminShell.run("decommissionWorkers", "nonexistentCommand");
    Assert.assertTrue(mOutput.toString().trim().contains(DecommissionWorkersCommand.description()));
    mOutput.reset();
    ret = mFsAdminShell.run("decommissionWorkers", "start");
    Assert.assertEquals(0, ret);
    mFsAdminShell.run("decommissionWorkers", "start", "-h");
  }
}
