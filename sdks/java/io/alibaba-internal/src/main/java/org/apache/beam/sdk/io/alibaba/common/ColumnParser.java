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
package org.apache.beam.sdk.io.alibaba.common;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.commons.lang3.StringUtils;

public class ColumnParser extends SimpleFunction<String[], List<List<String>>> {

  private String delimiter;
  private SimpleDateFormat format;

  public ColumnParser() {
    this("\u0001");
  }

  public ColumnParser(String delimiter) {
    this.delimiter = delimiter;
    this.format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  }

  public ColumnParser withDelimiter(String delimiter) {
    this.delimiter = delimiter;
    return this;
  }

  @Override
  public List<List<String>> apply(String[] rows) {
    List<List<String>> result = new ArrayList<>(rows.length);
    for (String row : rows) {
      String[] columns = StringUtils.splitPreserveAllTokens(row, delimiter);
      result.add(Arrays.asList(columns));
    }
    return result;
  }

}
