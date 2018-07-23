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

package alluxio.underfs.obs;

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link OBSUnderFileSystem}.
 */
@ThreadSafe
public class OBSAUnderFileSystemFactory implements UnderFileSystemFactory {

  public static final String HEADER_OBSA = "obsa://";

  /**
   * Constructs a new {@link OBSUnderFileSystemFactory}.
   */
  public OBSAUnderFileSystemFactory() {
  }

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "path");

    if (checkOBSCredentials(conf)) {
      try {
        return OBSAUnderFileSystem.createInstance(new AlluxioURI(path), conf);
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    String err = "OBS credentials or endpoint not available, cannot create OBS Under File System.";
    throw Throwables.propagate(new IOException(err));
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(HEADER_OBSA);
  }

  /**
   * @param conf optional configuration object for the UFS
   * @return true if access, secret and endpoint keys are present, false otherwise
   */
  private boolean checkOBSCredentials(UnderFileSystemConfiguration conf) {
    return conf.containsKey(OBSPropertyKey.OBS_ACCESS_KEY)
        && conf.containsKey(OBSPropertyKey.OBS_SECRET_KEY)
        && conf.containsKey(OBSPropertyKey.OBS_ENDPOINT);
  }
}
