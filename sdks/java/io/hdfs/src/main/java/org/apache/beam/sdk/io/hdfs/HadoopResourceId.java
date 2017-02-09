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
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.Objects;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.hadoop.fs.Path;

/**
 * {@link ResourceId} implementation for the Hadoop file system.
 */
class HadoopResourceId implements ResourceId {

  private final Path path;
  private final boolean isDirectory;

  static HadoopResourceId fromPath(Path path, boolean isDirectory) {
    checkNotNull(path, "path");
    return new HadoopResourceId(path, isDirectory);
  }

  private HadoopResourceId(Path path, boolean isDirectory) {
    this.path = path;
    this.isDirectory = isDirectory;
  }

  @Override
  public HadoopResourceId resolve(String other, ResolveOptions resolveOptions) {
    checkState(
        isDirectory,
        String.format("Expected the path is a directory, but had [%s].", path));
    checkArgument(
        resolveOptions.equals(StandardResolveOptions.RESOLVE_FILE)
            || resolveOptions.equals(StandardResolveOptions.RESOLVE_DIRECTORY),
        String.format("ResolveOptions: [%s] is not supported.", resolveOptions));
    checkArgument(
        !(resolveOptions.equals(StandardResolveOptions.RESOLVE_FILE)
            && other.endsWith("/")),
        "The resolved file: [%s] should not end with '/'.", other);
    return fromPath(
        new Path(path, other),
        resolveOptions.equals(StandardResolveOptions.RESOLVE_DIRECTORY));
  }

  @Override
  public HadoopResourceId getCurrentDirectory() {
    if (isDirectory) {
      return this;
    } else {
      Path parent = path.getParent();
      if (parent == null) {
        // TODO: set parent to . or /
      }
      checkState(
          parent != null,
          String.format("Failed to get the current directory for path: [%s].", path));
      return fromPath(
          parent,
          true /* isDirectory */);
    }
  }

  @Override
  public String getScheme() {
    return "hdfs";
  }

  Path getPath() {
    return path;
  }

  @Override
  public String toString() {
    return String.format("HadoopResourceId: [%s]", path);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof HadoopResourceId)) {
      return false;
    }
    HadoopResourceId other = (HadoopResourceId) obj;
    return this.path.equals(other.path)
        && this.isDirectory == other.isDirectory;
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, isDirectory);
  }
}
