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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import avro.shaded.com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.beam.sdk.io.fs.CreateOptions.StandardCreateOptions;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link HadoopFileSystem}.
 */
@RunWith(JUnit4.class)
public class HadoopFileSystemTest {

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private HadoopFileSystem hadoopFileSystem;

  @Before
  public void setup() throws IOException {
    hadoopFileSystem = new HadoopFileSystem(new Configuration());
  }


  @Test
  public void testCreateWithExistingFile() throws Exception {
    File existingFile = temporaryFolder.newFile();
    testCreate(new org.apache.hadoop.fs.Path(existingFile.toString()));
  }

  @Test
  public void testCreateWithinExistingDirectory() throws Exception {
    testCreate(
        new org.apache.hadoop.fs.Path(
            temporaryFolder.getRoot().toPath().resolve("file.txt").toString()));
  }

  @Test
  public void testCreateWithNonExistentSubDirectory() throws Exception {
    testCreate(
        new org.apache.hadoop.fs.Path(
            temporaryFolder.getRoot().toPath()
                .resolve("non-existent-dir").resolve("file.txt").toString()));
  }

  private void testCreate(org.apache.hadoop.fs.Path path) throws Exception {
    String expected = "my test string";
    // First with the path string
    createFileWithContent(path, expected);
    assertThat(
        readLines(path),
        containsInAnyOrder(expected));
  }

  private List<String> readLines(Path file) throws Exception {
    BufferedReader bufferedReader = new BufferedReader(
        Channels.newReader(hadoopFileSystem.open(
            toHadoopResourceId(file)), StandardCharsets.UTF_8.name()));

    List<String> lines = Lists.newArrayList();
    while (true) {
      String s = bufferedReader.readLine();
      if (s == null) {
        break;
      }
      lines.add(s);
    }
    return lines;
  }

  private void createFileWithContent(Path file, String content) throws Exception {
    try (Writer writer = Channels.newWriter(
        hadoopFileSystem.create(
            toHadoopResourceId(file), StandardCreateOptions.builder().setMimeType(MimeTypes.TEXT).build()),
        StandardCharsets.UTF_8.name())) {
      writer.write(content);
    }
  }

  private HadoopResourceId toHadoopResourceId(Path file) {
    return HadoopResourceId.fromPath(file, false /* isDirectory */);
  }
}
