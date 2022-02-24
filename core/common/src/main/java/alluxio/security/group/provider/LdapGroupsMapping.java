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

package alluxio.security.group.provider;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.TxPropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.retry.CountingRetry;
import alluxio.security.group.GroupMappingService;

import com.sun.jndi.ldap.LdapCtxFactory;
import org.apache.commons.io.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

/**
 * An implementation of {@link GroupMappingService} which
 * connects directly to an LDAP server for determining group membership.
 *
 * This provider should be used only if it is necessary to map users to
 * groups that reside exclusively in an Active Directory or LDAP installation.
 * The common case for a Alluxio installation will be that LDAP users and groups
 * materialized on the Unix servers, and for an installation like that,
 * ShellBasedUnixGroupsMapping is preferred. However, in cases where
 * those users and groups aren't materialized in Unix, but need to be used for
 * access control, this class may be used to communicate directly with the LDAP
 * server.
 *
 * It is important to note that resolving group mappings will incur network
 * traffic, and may cause degraded performance, although user-group mappings
 * will be cached via the infrastructure provided by {@link GroupMappingService}.
 *
 * This implementation does not support configurable search limits. If a filter
 * is used for searching users or groups which returns more results than are
 * allowed by the server, an exception will be thrown.
 *
 * The implementation also does not attempt to resolve group hierarchies. In
 * order to be considered a member of a group, the user must be an explicit
 * member in LDAP.
 */
public final class LdapGroupsMapping implements GroupMappingService {
  private static final Logger LOG = LoggerFactory.getLogger(LdapGroupsMapping.class);

  private static final int LDAP_SERVER_REQUEST_RETRY_COUNT = 3;
  private static final String SIMPLE_AUTHENTICATION = "simple";
  private static final String SSL = "ssl";
  private static final String SSL_KEYSTORE_KEY = "javax.net.ssl.keyStore";
  private static final String SSL_KEYSTORE_PASSWORD_KEY = "javax.net.ssl.keyStorePassword";
  private static final SearchControls SEARCH_CONTROLS = new SearchControls();

  static {
    SEARCH_CONTROLS.setSearchScope(SearchControls.SUBTREE_SCOPE);
  }

  /*
   * Posix attributes
   */
  public static final String POSIX_GROUP = "posixGroup";
  public static final String POSIX_ACCOUNT = "posixAccount";
  public static final String INET_ORG_PERSON = "inetOrgPerson";

  private final AlluxioConfiguration mConf;
  private final String mGroupNameAttr;
  private final String mPosixUidAttr;
  private final String mPosixGidAttr;
  private final String mSearchBase;
  private final String mUserSearchFilter;
  private final String mGroupSearchFilter;
  private final String mGroupQuery;

  private DirContext mDirContext;
  private boolean mIsPosix;

  /**
   * Constructs a new {@link LdapGroupsMapping}.
   * @param conf the Alluxio configuration
   */
  public LdapGroupsMapping(AlluxioConfiguration conf) {
    mConf = conf;
    mGroupNameAttr =
        mConf.get(TxPropertyKey.SECURITY_GROUP_MAPPING_LDAP_ATTR_GROUP_NAME);
    mPosixUidAttr =
        mConf.get(TxPropertyKey.SECURITY_GROUP_MAPPING_LDAP_ATTR_POSIX_UID);
    mPosixGidAttr =
        mConf.get(TxPropertyKey.SECURITY_GROUP_MAPPING_LDAP_ATTR_POSIX_GID);
    SEARCH_CONTROLS.setTimeLimit(
        mConf.getInt(TxPropertyKey.SECURITY_GROUP_MAPPING_LDAP_SEARCH_TIMEOUT));
    // Limit the attributes returned to only those required to speed up the search.
    // See HADOOP-10626 and HADOOP-12001 for more details.
    SEARCH_CONTROLS.setReturningAttributes(
        new String[] {mGroupNameAttr, mPosixUidAttr, mPosixGidAttr});
    mSearchBase = mConf.get(TxPropertyKey.SECURITY_GROUP_MAPPING_LDAP_BASE);
    LOG.info("LDAP search base = " + mSearchBase);
    mUserSearchFilter = mConf.get(TxPropertyKey.SECURITY_GROUP_MAPPING_LDAP_SEARCH_FILTER_USER);
    LOG.info("user query = " + mUserSearchFilter);
    mGroupSearchFilter =
        mConf.get(TxPropertyKey.SECURITY_GROUP_MAPPING_LDAP_SEARCH_FILTER_GROUP);
    mIsPosix = mGroupSearchFilter.contains(POSIX_GROUP)
        && (mUserSearchFilter.contains(POSIX_ACCOUNT)
            || mUserSearchFilter.contains(INET_ORG_PERSON));
    if (mIsPosix) {
      mGroupQuery = String.format("(&%s(|(%s={0})(%s={1})))",
          mGroupSearchFilter, mPosixGidAttr, mPosixUidAttr);
    } else {
      String groupMemberAttr =
          mConf.get(TxPropertyKey.SECURITY_GROUP_MAPPING_LDAP_ATTR_MEMBER);
      mGroupQuery = String.format("(&%s(%s={0}))", mGroupSearchFilter, groupMemberAttr);
    }
    LOG.info("group query = " + mGroupQuery);
  }

  @Override
  public List<String> getGroups(String user) throws IOException {
    CountingRetry retry = new CountingRetry(LDAP_SERVER_REQUEST_RETRY_COUNT);
    while (retry.attempt()) {
      if (mDirContext == null) {
        try {
          mDirContext = createDirContext();
        } catch (IOException | NamingException e) {
          throw new IOException(
              ExceptionMessage.CANNOT_INITIALIZE_DIR_CONTEXT.getMessage(e));
        }
      }
      try {
        return searchForGroups(user);
      } catch (NamingException e) {
        LOG.error(ExceptionMessage.CANNOT_GET_GROUPS_FROM_LDAP_SERVER.getMessage(user,
            retry.getAttemptCount()), e);
        mDirContext = null;
      }
    }
    throw new IOException(ExceptionMessage.CANNOT_GET_GROUPS_FROM_LDAP_SERVER.getMessage(
        user, retry.getAttemptCount()));
  }

  private List<String> searchForGroups(String user) throws NamingException {
    List<String> groups = new ArrayList<>();
    NamingEnumeration<SearchResult> results =
        mDirContext.search(mSearchBase, mUserSearchFilter, new Object[] {user}, SEARCH_CONTROLS);
    if (results.hasMoreElements()) {
      SearchResult result = results.nextElement();
      String userDistinguishedName = result.getNameInNamespace();
      LOG.debug("user DN = " + userDistinguishedName);

      NamingEnumeration<SearchResult> groupResults = null;
      if (mIsPosix) {
        String gidNumber = null;
        String uidNumber = null;
        Attribute gidAttribute = result.getAttributes().get(mPosixGidAttr);
        Attribute uidAttribute = result.getAttributes().get(mPosixUidAttr);
        if (gidAttribute != null) {
          gidNumber = gidAttribute.get().toString();
        }
        if (uidAttribute != null) {
          uidNumber = uidAttribute.get().toString();
        }
        if (uidNumber != null && gidNumber != null) {
          groupResults =
              mDirContext.search(mSearchBase, mGroupQuery,
                  new Object[] { gidNumber, uidNumber },
                  SEARCH_CONTROLS);
        }
      } else {
        groupResults =
            mDirContext.search(mSearchBase, mGroupQuery,
                new Object[] {userDistinguishedName},
                SEARCH_CONTROLS);
      }
      if (groupResults != null) {
        while (groupResults.hasMoreElements()) {
          SearchResult groupResult = groupResults.nextElement();
          Attribute groupName = groupResult.getAttributes().get(mGroupNameAttr);
          groups.add(groupName.get().toString());
        }
      }
    }
    LOG.debug("groups = " + groups);
    return groups;
  }

  private DirContext createDirContext() throws IOException, NamingException {
    Hashtable<String, String> env = new Hashtable<>();
    env.put("java.naming.factory.initial", LdapCtxFactory.class.getName());
    env.put("java.naming.provider.url",
        mConf.get(TxPropertyKey.SECURITY_GROUP_MAPPING_LDAP_URL));
    if (mConf.isSet(TxPropertyKey.SECURITY_GROUP_MAPPING_LDAP_SSL) && mConf.getBoolean(
        TxPropertyKey.SECURITY_GROUP_MAPPING_LDAP_SSL)) {
      env.put("java.naming.security.protocol", SSL);
      System.setProperty(SSL_KEYSTORE_KEY,
          mConf.get(TxPropertyKey.SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE));
      System.setProperty(SSL_KEYSTORE_PASSWORD_KEY,
          getPassword(TxPropertyKey.SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE_PASSWORD,
              TxPropertyKey.SECURITY_GROUP_MAPPING_LDAP_SSL_KEYSTORE_PASSWORD_FILE));
    }
    env.put("java.naming.security.authentication", SIMPLE_AUTHENTICATION);
    env.put("java.naming.security.principal",
        mConf.get(TxPropertyKey.SECURITY_GROUP_MAPPING_LDAP_BIND_USER));
    env.put("java.naming.security.credentials",
        getPassword(TxPropertyKey.SECURITY_GROUP_MAPPING_LDAP_BIND_PASSWORD,
            TxPropertyKey.SECURITY_GROUP_MAPPING_LDAP_BIND_PASSWORD_FILE));
    return new InitialDirContext(env);
  }

  private String getPassword(PropertyKey passwordKey, PropertyKey passwordFileKey)
      throws IOException {
    if (mConf.isSet(passwordKey)) {
      String password = mConf.get(passwordKey);
      return password;
    }
    String passwordFile = "";
    if (mConf.isSet(passwordFileKey)) {
      passwordFile = mConf.get(passwordFileKey);
    }
    if (passwordFile.isEmpty()) {
      return "";
    }
    StringBuilder passwordBuilder = new StringBuilder();
    try (Reader reader = new InputStreamReader(
        new FileInputStream(passwordFile), Charsets.UTF_8)) {
      int c = reader.read();
      while (c > -1) {
        passwordBuilder.append((char) c);
        c = reader.read();
      }
      return passwordBuilder.toString().trim();
    } catch (IOException e) {
      throw new IOException(
          ExceptionMessage.CANNOT_READ_PASSWORD_FILE.getMessage(passwordFile), e);
    }
  }
}
