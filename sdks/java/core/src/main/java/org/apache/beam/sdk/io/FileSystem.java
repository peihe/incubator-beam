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
package org.apache.beam.sdk.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;

/**
 * File system interface in Beam.
 *
 * <p>It defines APIs for writing file systems agnostic code.
 *
 * <p>All methods are protected, and they are for file system providers to implement.
 * Clients should use {@link FileSystems} utility.
 */
public abstract class FileSystem<ResourceIdT extends ResourceId> {
  /**
   * Returns a write channel for the given {@link ResourceIdT}.
   *
   * <p>The resource is not expanded; it is used verbatim.
   *
   * @param resourceId the reference of the file-like resource to create
   * @param createOptions the configuration of the create operation
   */
  protected abstract WritableByteChannel create(
      ResourceIdT resourceId, CreateOptions createOptions) throws IOException;

  /**
   * Returns a read channel for the given {@link ResourceIdT}.
   *
   * <p>The resource is not expanded; it is used verbatim.
   *
   * <p>If seeking is supported, then this returns a
   * {@link java.nio.channels.SeekableByteChannel}.
   *
   * @param resourceId the reference of the file-like resource to open
   */
  protected abstract ReadableByteChannel open(ResourceIdT resourceId) throws IOException;

  /**
   * Copies a {@link List} of file-like resources from one location to another.
   *
   * <p>The number of source resources must equal the number of destination resources.
   * Destination resources will be created recursively.
   *
   * @param srcResourceIds the references of the source resources
   * @param destResourceIds the references of the destination resources
   *
   * @throws FileNotFoundException if the source resources are missing. When copy throws,
   * each resource might or might not be copied. In such scenarios, callers can use {@code match()}
   * to determine the state of the resources.
   */
  protected abstract void copy(
      List<ResourceIdT> srcResourceIds,
      List<ResourceIdT> destResourceIds) throws IOException;

  /**
   * Renames a {@link List} of file-like resources from one location to another.
   *
   * <p>The number of source resources must equal the number of destination resources.
   * Destination resources will be created recursively.
   *
   * @param srcResourceIds the references of the source resources
   * @param destResourceIds the references of the destination resources
   *
   * @throws FileNotFoundException if the source resources are missing. When rename throws,
   * the state of the resources is unknown but safe:
   * for every (source, destination) pair of resources, the following are possible:
   * a) source exists, b) destination exists, c) source and destination both exist.
   * Thus no data is lost, however, duplicated resource are possible.
   * In such scenarios, callers can use {@code match()} to determine the state of the resource.
   */
  protected abstract void rename(
      List<ResourceIdT> srcResourceIds,
      List<ResourceIdT> destResourceIds) throws IOException;

  /**
   * Deletes a collection of resources.
   *
   * <p>It is allowed but not recommended to delete directories recursively.
   * Callers depends on {@link FileSystems} and uses {@code DeleteOptions}.
   *
   * @param resourceIds the references of the resources to delete.
   *
   * @throws FileNotFoundException if resources are missing. When delete throws,
   * each resource might or might not be deleted. In such scenarios, callers can use {@code match()}
   * to determine the state of the resources.
   */
  protected abstract void delete(Collection<ResourceIdT> resourceIds) throws IOException;

  /**
   * This is the entry point to convert users provided specs to URIs.
   * Callers should use match() to resolve users specs ambiguities before
   * calling other methods.
   *
   * @return List<MatchResult>, which is in the same order of the input specs.
   *
   * @throw IOException if all specs failed to match due to issues like:
   * network connection, authorization.
   * Exception for individual spec need to be deferred until callers retrieve
   * metadata with {@link MatchResult#metadata()}.
   *
   *
   * Implementation should handle the following ambiguities of users provided sepc:
   * 1). spec could be a glob or a uri. match() should be able to tell and
   *     choose efficient implementations.
   * 2). spec doesn’t end with ‘/’ may refer to files or directories:
   *     file:/home/dir/ should be returned for spec “file:/home/dir”.
   *     (However, spec ends with ‘/’ always refers to directories.)
   * Note: File systems glob support is different. However, it is required to
   * support glob in the final component of a path (eg file:/foo/bar/*.txt).
   *
   * Throws if the spec is invalid.
   */
  protected abstract List<MatchResult> match(List<String> specs) throws IOException;
}
