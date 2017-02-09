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
package org.apache.beam.sdk.io.fs;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.io.FileSystem;

/**
 * An abstract {@link FileSystem} whose methods are public and could be mocked.
 */
public abstract class MockableFileSystem extends FileSystem<ResourceId> {
  @Override
  public abstract WritableByteChannel create(ResourceId resourceId, CreateOptions options)
      throws IOException;
  @Override
  public abstract ReadableByteChannel open(ResourceId resourceId) throws IOException;

  @Override
  public abstract void copy(
      List<ResourceId> srcResourceIds,
      List<ResourceId> destResourceIds) throws IOException;

  @Override
  public abstract void rename(
      List<ResourceId> srcResourceIds,
      List<ResourceId> destResourceIds) throws IOException;

  @Override
  public abstract void delete(Collection<ResourceId> resourceIds) throws IOException;
}
