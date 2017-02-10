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
package org.apache.beam.sdk.io.gcp.storage;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.sdk.util.GcsUtil.executeBatches;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.util.ResilientOperation;
import com.google.cloud.hadoop.util.RetryDeterminer;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link FileSystem} implementation for Google Cloud Storage.
 */
class GcsFileSystem extends FileSystem<GcsResourceId> {

  private static final Logger LOG = LoggerFactory.getLogger(GcsFileSystem.class);

  /**
   * Maximum number of requests permitted in a GCS batch request.
   */
  private static final int MAX_REQUESTS_PER_BATCH = 100;

  private final GcsOptions options;

  GcsFileSystem(GcsOptions options) {
    this.options = checkNotNull(options, "options");
  }

  @Override
  protected WritableByteChannel create(GcsResourceId resourceId, CreateOptions createOptions)
      throws IOException {
    return options.getGcsUtil().create(resourceId.getGcsPath(), createOptions.mimeType());
  }

  @Override
  protected ReadableByteChannel open(GcsResourceId resourceId) throws IOException {
    return options.getGcsUtil().open(resourceId.getGcsPath());
  }

  @Override
  protected void rename(
      List<GcsResourceId> srcResourceIds,
      List<GcsResourceId> destResourceIds) throws IOException {
    copy(srcResourceIds, destResourceIds);
    delete(srcResourceIds);
  }

  @Override
  protected void delete(Collection<GcsResourceId> resourceIds) throws IOException {
    options.getGcsUtil().remove(toFilenames(resourceIds));
  }

  @Override
  protected void copy(List<GcsResourceId> srcResourceIds, List<GcsResourceId> destResourceIds)
      throws IOException {
    options.getGcsUtil().copy(toFilenames(srcResourceIds), toFilenames(destResourceIds));
  }

  private List<String> toFilenames(Collection<GcsResourceId> resources) {
    return FluentIterable.from(resources)
        .transform(
            new Function<GcsResourceId, String>() {
              @Override
              public String apply(GcsResourceId resource) {
                return resource.getGcsPath().toString();
              }})

  protected List<MatchResult> match(List<String> specs) throws IOException {
    List<GcsPath> gcsPaths = convertToGcsPath(specs);

    Map<Integer, GcsPath> globs = Maps.newHashMap();
    Map<Integer, GcsPath> nonGlobs = Maps.newHashMap();

    for (int i = 0; i < gcsPaths.size(); ++i) {
      GcsPath path = gcsPaths.get(i);
      Integer index = Integer.valueOf(i);
      if (GcsUtil.isGlob(path)) {
        globs.put(index, path);
      } else {
        nonGlobs.put(index, path);
      }
    }

    Map<Integer, MatchResult> globsMatchResults = matchGlobs(globs);
    Map<Integer, MatchResult> nonGlobsMatchResults = matchNonGlobs(nonGlobs);

    return mergeMatchResults(globsMatchResults, nonGlobsMatchResults);
  }

  private Map<Integer, MatchResult> matchGlobs(Map<Integer, GcsPath> globs) {
    ImmutableMap.Builder<Integer, MatchResult> ret = ImmutableMap.builder();
    for (Entry<Integer, GcsPath> entry : globs.entrySet()) {
      MatchResult matchResult;
      try {
        matchResult = expand(entry.getValue());
      } catch (IOException e) {
        matchResult = MatchResult.create(Status.ERROR, e);
      }
      ret.put(entry.getKey(), matchResult);
    }
    return ret.build();
  }

  private List<MatchResult> mergeMatchResults(Map<Integer, MatchResult> globsMatchResults,
      Map<Integer, MatchResult> nonGlobsMatchResults) {
    int count = globsMatchResults.size() + nonGlobsMatchResults.size();
    ImmutableList.Builder<MatchResult> ret = ImmutableList.builder();
    for (int i = 0; i < count; ++i) {
      if (globsMatchResults.containsKey(i)) {
        ret.add(globsMatchResults.get(i));
      } else if (nonGlobsMatchResults.containsKey(i)) {
        ret.add(nonGlobsMatchResults.get(i));
      } else {
        throw new IllegalArgumentException("Could not find %dth MatchResult.");
      }
    }
    return ret.build();
  }

  /**
   * Expands a pattern into matched paths. The pattern path may contain globs, which are expanded
   * in the result. For patterns that only match a single object, we ensure that the object
   * exists.
   */
  public MatchResult expand(GcsPath gcsPattern) throws IOException {
    checkArgument(GcsUtil.isGcsPatternSupported(gcsPattern.getObject()));
    Matcher m = GLOB_PREFIX.matcher(gcsPattern.getObject());
    Pattern p = Pattern.compile(GcsUtil.globToRegexp(gcsPattern.getObject()));
    // Part before the first wildcard character.
    String prefix = m.group("PREFIX");

    LOG.debug("matching files in bucket {}, prefix {} against pattern {}", gcsPattern.getBucket(),
        prefix, p.toString());

    // List all objects that start with the prefix (including objects in sub-directories).
    Storage.Objects.List listObject = storageClient.objects().list(gcsPattern.getBucket());
    listObject.setMaxResults(MAX_LIST_ITEMS_PER_CALL);
    listObject.setPrefix(prefix);

    String pageToken = null;
    List<Metadata> results = new LinkedList<>();
    do {
      if (pageToken != null) {
        listObject.setPageToken(pageToken);
      }

      Objects objects;
      try {
        objects = ResilientOperation.retry(
            ResilientOperation.getGoogleRequestCallable(listObject),
            BACKOFF_FACTORY.backoff(),
            RetryDeterminer.SOCKET_ERRORS,
            IOException.class);
      } catch (Exception e) {
        return MatchResult.create(
            Status.ERROR,
            new IOException("Unable to match files in bucket " + gcsPattern.getBucket()
                +  ", prefix " + prefix + " against pattern " + p.toString(), e));
      }
      //Objects objects = listObject.execute();
      checkNotNull(objects);

      if (objects.getItems() == null) {
        break;
      }

      // Filter objects based on the regex.
      for (StorageObject o : objects.getItems()) {
        String name = o.getName();
        // Skip directories, which end with a slash.
        if (p.matcher(name).matches() && !name.endsWith("/")) {
          LOG.debug("Matched object: {}", name);
          results.add(toMetadata(GcsPath.fromObject(o), o));
        }
      }

      pageToken = objects.getNextPageToken();
    } while (pageToken != null);

    return MatchResult.create(Status.OK, results.toArray(new Metadata[results.size()]));
  }

  private Map<Integer, MatchResult> matchNonGlobs(Map<Integer, GcsPath> nonGlobs)
      throws IOException {
    List<Integer> indexes = Lists.newArrayList();
    List<GcsPath> paths = Lists.newArrayList();

    for (Entry<Integer, GcsPath> entry : nonGlobs.entrySet()) {
      indexes.add(entry.getKey());
      paths.add(entry.getValue());
    }

    List<StorageObject[]> results = Lists.newArrayList();
    executeBatches(makeGetBatches(paths, results));
    checkState(
        paths.size() == results.size(),
        "MatchResult size: %d is not equal to paths size: %d.", results.size(), paths.size());

    ImmutableMap.Builder<Integer, MatchResult> ret = ImmutableMap.builder();
    for (int i = 0; i < results.size(); ++i) {
      ret.put(indexes.get(i), toMetadata(paths.get(i), results.get(i)[0]));
    }
    return ret.build();
  }

  private List<GcsPath> convertToGcsPath(Collection<String> uris) {
    return FluentIterable.from(uris)
        .transform(new Function<String, GcsPath>() {
          @Override
          public GcsPath apply(String uri) {
            return GcsPath.fromUri(uri);
          }})
        .toList();
  }

  private Metadata toMetadata(GcsPath gcsPath, StorageObject storageObject) {
    // TODO It is incorrect to return true here for files with content encoding set to gzip.
    Metadata.Builder ret = Metadata.builder()
        .setIsReadSeekEfficient(true)
        .setResourceId(GcsResourceId.fromGcsPath(gcsPath));
    BigInteger size = storageObject.getSize();
    if (size != null) {
      ret.setSizeBytes(size.longValue());
    }
    return ret.build();
  }
}
