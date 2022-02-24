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

package alluxio.security.authorization.hadoop;

import alluxio.collections.Pair;
import alluxio.master.file.meta.InodeAttributes;
import alluxio.master.file.meta.InodeView;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclActions;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.AclEntryType;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.security.authorization.ExtendedACLEntries;
import alluxio.security.authorization.Mode;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * HdfsAclConverter.
 */
public class HdfsAclConverter {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsAclConverter.class);

  /**
   * Convert Hdfs AclEntry to Alluxio AclEntry.
   *
   * @param entry Hdfs AclEntry
   * @return Alluxio AclEntry
   */
  public static AclEntry toAlluxioAclEntry(org.apache.hadoop.fs.permission.AclEntry entry) {
    AclEntry.Builder builder = new AclEntry.Builder();
    builder.setType(HdfsAclConverter.toAlluxioAclEntryType(entry)).setIsDefault(
        entry.getScope() == org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT);
    if (entry.getName() != null) {
      builder.setSubject(entry.getName());
    }
    builder.setActions(HdfsAclConverter.toAlluxioPermission(entry));
    return builder.build();
  }

  private static AclActions toAlluxioPermission(org.apache.hadoop.fs.permission.AclEntry entry) {
    org.apache.hadoop.fs.permission.FsAction hdfsAction = entry.getPermission();
    AclActions actions = new AclActions();
    switch (hdfsAction) {
      case NONE: {
        actions.updateByModeBits(Mode.Bits.NONE);
        break;
      }
      case EXECUTE: {
        actions.updateByModeBits(Mode.Bits.EXECUTE);
        break;
      }
      case WRITE: {
        actions.updateByModeBits(Mode.Bits.WRITE);
        break;
      }
      case WRITE_EXECUTE: {
        actions.updateByModeBits(Mode.Bits.WRITE_EXECUTE);
        break;
      }
      case READ: {
        actions.updateByModeBits(Mode.Bits.READ);
        break;
      }
      case READ_EXECUTE: {
        actions.updateByModeBits(Mode.Bits.READ_EXECUTE);
        break;
      }
      case READ_WRITE: {
        actions.updateByModeBits(Mode.Bits.READ_WRITE);
        break;
      }
      case ALL: {
        actions.updateByModeBits(Mode.Bits.ALL);
        break;
      }
      default: {
        throw new UnsupportedOperationException(
            "Unsupported HDFS ACL permission: " + (entry.getPermission()));
      }
    }
    return actions;
  }

  private static org.apache.hadoop.fs.permission.FsAction toHdfsPermission(AclEntry entry) {
    AclActions actions = entry.getActions();
    switch (actions.toModeBits()) {
      case NONE: {
        return org.apache.hadoop.fs.permission.FsAction.NONE;
      }
      case EXECUTE: {
        return org.apache.hadoop.fs.permission.FsAction.EXECUTE;
      }
      case WRITE: {
        return org.apache.hadoop.fs.permission.FsAction.WRITE;
      }
      case WRITE_EXECUTE: {
        return org.apache.hadoop.fs.permission.FsAction.WRITE_EXECUTE;
      }
      case READ: {
        return org.apache.hadoop.fs.permission.FsAction.READ;
      }
      case READ_EXECUTE: {
        return org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE;
      }
      case READ_WRITE: {
        return org.apache.hadoop.fs.permission.FsAction.READ_WRITE;
      }
      case ALL: {
        return org.apache.hadoop.fs.permission.FsAction.ALL;
      }
      default: {
        throw new UnsupportedOperationException(
            "Unsupported Alluxio ACL permission:" + entry.getActions());
      }
    }
  }

  /**
   * Convert Alluxio AclEntry to Hdfs AclEntry.
   *
   * @param entry Alluxio AclEntry
   * @return Hdfs AclEntry
   */
  public static org.apache.hadoop.fs.permission.AclEntry toHdfsAclEntry(AclEntry entry) {
    org.apache.hadoop.fs.permission.AclEntry.Builder builder =
        new org.apache.hadoop.fs.permission.AclEntry.Builder();
    if (entry.getType() != AclEntryType.OWNING_USER
        && entry.getType() != AclEntryType.OWNING_GROUP) {
      builder.setName(entry.getSubject());
    }
    builder.setScope(entry.isDefault()
        ? org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT
        : org.apache.hadoop.fs.permission.AclEntryScope.ACCESS);
    builder.setType(HdfsAclConverter.toHdfsAclEntryType(entry));
    builder.setPermission(HdfsAclConverter.toHdfsPermission(entry));
    return builder.build();
  }

  private static
      org.apache.hadoop.fs.permission.AclEntryType toHdfsAclEntryType(AclEntry aclEntry) {
    switch (aclEntry.getType()) {
      case OWNING_USER:
      case NAMED_USER: {
        return org.apache.hadoop.fs.permission.AclEntryType.USER;
      }
      case OWNING_GROUP:
      case NAMED_GROUP: {
        return org.apache.hadoop.fs.permission.AclEntryType.GROUP;
      }
      case MASK: {
        return org.apache.hadoop.fs.permission.AclEntryType.MASK;
      }
      case OTHER: {
        return org.apache.hadoop.fs.permission.AclEntryType.OTHER;
      }
      default: {
        throw new UnsupportedOperationException("Unknown Alluxio ACL entry type: "
            + aclEntry.getType());
      }
    }
  }

  private static AclEntryType toAlluxioAclEntryType(
      org.apache.hadoop.fs.permission.AclEntry entry) {
    switch (entry.getType()) {
      case USER: {
        return entry.getName() == null || entry.getName().isEmpty()
            ? AclEntryType.OWNING_USER : AclEntryType.NAMED_USER;
      }
      case GROUP: {
        return entry.getName() == null || entry.getName().isEmpty()
            ? AclEntryType.OWNING_GROUP : AclEntryType.NAMED_GROUP;
      }
      case MASK: {
        return AclEntryType.MASK;
      }
      case OTHER: {
        return AclEntryType.OTHER;
      }
      default: {
        throw new UnsupportedOperationException(
            "Unknown HDFS ACL entry type: " + (entry.getType()));
      }
    }
  }

  /**
   * Convert Alluxio Acl to Hdfs AclFeature.
   * @param acl Alluxio AccessControlList
   * @param dAcl Alluxio DefaultAccessControlList
   * @return Hdfs AclFeature
   */
  public static org.apache.hadoop.hdfs.server.namenode.AclFeature
      toHdfsAclFeature(AccessControlList acl, @Nullable DefaultAccessControlList dAcl) {
    ArrayList<org.apache.hadoop.fs.permission.AclEntry> hdfsEntries = new ArrayList<>();
    if (acl != null) {
      hdfsEntries.addAll(acl.getEntries().stream().map(HdfsAclConverter::toHdfsAclEntry)
          .collect(Collectors.toList()));
    }
    if (dAcl != null) {
      hdfsEntries.addAll(HdfsAclConverter.getDefaultACLEntriesIncludeMask(dAcl).stream()
          .map(HdfsAclConverter::toHdfsAclEntry).collect(Collectors.toList()));
    }
    return HdfsAclConverter.generateHdfsAclFeature(hdfsEntries);
  }

  /**
   * Convert Alluxio InodeAttributes to Hdfs AclFeature.
   * @param attr Alluxio InodeAttributes
   * @return Hdfs AclFeature
   */
  public static org.apache.hadoop.hdfs.server.namenode.AclFeature toHdfsAclFeature(
      InodeAttributes attr) {
    if (attr.isDirectory()) {
      return HdfsAclConverter.toHdfsAclFeature(attr.getACL(), attr.getDefaultACL());
    }
    return HdfsAclConverter.toHdfsAclFeature(attr.getACL(), null);
  }

  /**
   * Convert Alluxio InodeAttributes to Hdfs AclFeature.
   * @param inode Alluxio InodeAttributes
   * @return Hdfs AclFeature
   */
  public static org.apache.hadoop.hdfs.server.namenode.AclFeature toHdfsAclFeature(
      InodeView inode) {
    if (inode.isDirectory()) {
      return HdfsAclConverter.toHdfsAclFeature(inode.getACL(), inode.getDefaultACL());
    }
    return HdfsAclConverter.toHdfsAclFeature(inode.getACL(), null);
  }

  static org.apache.hadoop.hdfs.server.namenode.AclFeature generateHdfsAclFeature(
      List<org.apache.hadoop.fs.permission.AclEntry> aclEntries) {
    ArrayList<org.apache.hadoop.fs.permission.AclEntry> featureEntries =
        new ArrayList<>();
    for (org.apache.hadoop.fs.permission.AclEntry a : aclEntries) {
      if (a.getPermission() == org.apache.hadoop.fs.permission.FsAction.NONE) {
        continue;
      }
      featureEntries.add(a);
    }
    int[] featureDigits =
        org.apache.hadoop.hdfs.server.namenode.AclEntryStatusFormat.toInt(featureEntries);
    return new org.apache.hadoop.hdfs.server.namenode.AclFeature(featureDigits);
  }

  /**
   * Convert Hdfs InodeAttributes to Alluxio Acl.
   * @param attrs Hdfs InodeAttributes
   * @return Alluxio Acl
   */
  public static Pair<AccessControlList, DefaultAccessControlList> toAlluxioAcl(
      org.apache.hadoop.hdfs.server.namenode.INodeAttributes attrs) {
    List<org.apache.hadoop.fs.permission.AclEntry> hdfsAclEntries =
        org.apache.hadoop.hdfs.server.namenode.AclStorage.readINodeAcl(attrs);
    AccessControlList acl = new AccessControlList();
    acl.setOwningUser(attrs.getUserName());
    acl.setOwningGroup(attrs.getGroupName());
    DefaultAccessControlList dAcl = null;
    if (attrs.isDirectory()) {
      dAcl = new DefaultAccessControlList();
      dAcl.setOwningUser(acl.getOwningUser());
      dAcl.setOwningGroup(acl.getOwningGroup());
    }
    for (org.apache.hadoop.fs.permission.AclEntry e : hdfsAclEntries) {
      if (e.getScope() == org.apache.hadoop.fs.permission.AclEntryScope.DEFAULT) {
        if (dAcl == null) {
          throw new UnsupportedOperationException("Default ACL is not supported on file!");
        }
        dAcl.setEntry(HdfsAclConverter.toAlluxioAclEntry(e));
        continue;
      }
      acl.setEntry(HdfsAclConverter.toAlluxioAclEntry(e));
    }
    return new Pair<>(acl, dAcl);
  }

  /**
   * get default ACL Entries Mask.
   * @param dAcl DefaultAccessControlList
   * @return Alluxio Acl
   */
  public static List<AclEntry> getDefaultACLEntriesIncludeMask(DefaultAccessControlList dAcl) {
    ExtendedACLEntries extendedEntries = dAcl.getExtendedEntries();
    if (extendedEntries != null && extendedEntries.getMask().toModeBits() != Mode.Bits.NONE) {
      return new ImmutableList.Builder<AclEntry>().addAll(dAcl.getEntries()).add(
          new AclEntry.Builder().setType(AclEntryType.MASK).setIsDefault(true)
              .setActions(extendedEntries.getMask()).build()).build();
    }
    return dAcl.getEntries();
  }
}

