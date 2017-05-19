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

import java.util.List;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class TTBlockParser implements SerializableFunction<byte[], List<List<String>>> {

  private RowParser rowParser;
  private ColumnParser columnParser;

  public TTBlockParser() {
    rowParser = new RowParser();
    columnParser = new ColumnParser();
  }

  public TTBlockParser withCharsetName(String charsetName) {
    this.rowParser = rowParser.withCharsetName(charsetName);
    return this;
  }

  public TTBlockParser withRowDelimiter(String delimiter) {
    this.rowParser = rowParser.withDelimiter(delimiter);
    return this;
  }

  public TTBlockParser withColumnDelimiter(String delimiter) {
    this.columnParser = columnParser.withDelimiter(delimiter);
    return this;
  }

  @Override
  public List<List<String>> apply(byte[] input) {
    return columnParser.apply(rowParser.apply(input));
  }
}
