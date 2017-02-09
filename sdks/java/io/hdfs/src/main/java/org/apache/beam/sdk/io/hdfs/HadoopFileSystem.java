/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.beam.sdk.io.hdfs;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adapts {@link org.apache.hadoop.fs.FileSystem} connectors to be used as
 * Apache Beam {@link FileSystem FileSystems}.
 */
class HadoopFileSystem extends FileSystem<HadoopResourceId> {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopFileSystem.class);

  private final org.apache.hadoop.fs.FileSystem hadoopFs;

  HadoopFileSystem(Configuration config) {
    try {
      this.hadoopFs = org.apache.hadoop.fs.FileSystem.get(config);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to create hadoop FileSystem from config: ", config), e);
    }
  }

  @Override
  protected WritableByteChannel create(HadoopResourceId resourceId, CreateOptions createOptions)
      throws IOException {
    return Channels.newChannel(hadoopFs.create(resourceId.getPath()));
  }

  @Override
  protected ReadableByteChannel open(HadoopResourceId resourceId) throws IOException {
    return Channels.newChannel(hadoopFs.open(resourceId.getPath()));
  }

  @Override
  protected void copy(
      List<HadoopResourceId> srcResourceIds,
      List<HadoopResourceId> destResourceIds) throws IOException {
    copy(srcResourceIds, destResourceIds, false /* deleteSource */);
  }

  @VisibleForTesting
  void copy(
      List<HadoopResourceId> srcResourceIds,
      List<HadoopResourceId> destResourceIds,
      boolean deleteSource) throws IOException {
    checkArgument(
        srcResourceIds.size() == destResourceIds.size(),
        "Number of source resources %s must equal number of destination resources %s",
        srcResourceIds.size(),
        destResourceIds.size());
    int numFiles = srcResourceIds.size();
    for (int i = 0; i < numFiles; i++) {
      HadoopResourceId src = srcResourceIds.get(i);
      HadoopResourceId dst = destResourceIds.get(i);
      if (deleteSource) {
        LOG.debug("Renaming {} to {}", src, dst);
      } else {
        LOG.debug("Copying {} to {}", src, dst);
      }
      try {
        // Copy the source, replacing the existing destination.
        // Paths.get(x) will not work on Windows OSes cause of the ":" after the drive letter.
        org.apache.hadoop.fs.FileUtil.copy(
            hadoopFs,
            src.getPath(),
            hadoopFs,
            dst.getPath(),
            deleteSource,
            true /* overwrite */,
            hadoopFs.getConf());
      } catch (NoSuchFileException | FileNotFoundException e) {
        LOG.debug("{} does not exist.", src);
        // Suppress exception if resource does not exist.
        // TODO: re-throw FileNotFoundException once FileSystems supports ignoreMissingFile.
      }
    }
  }

  @Override
  protected void rename(
      List<HadoopResourceId> srcResourceIds,
      List<HadoopResourceId> destResourceIds) throws IOException {
    copy(srcResourceIds, destResourceIds, true /* deleteSource */);
  }

  @Override
  protected void delete(Collection<HadoopResourceId> resourceIds) throws IOException {
    for (HadoopResourceId resourceId : resourceIds) {
      LOG.debug("deleting resource {}", resourceId);
      org.apache.hadoop.fs.Path path = resourceId.getPath();
      // Delete the resource if it exists.
      if (hadoopFs.exists(path)) {
        hadoopFs.delete(path, false /* recursive */);
      }
      // TODO: throw FileNotFoundException once FileSystems supports ignoreMissingFile.
    }
  }
}
