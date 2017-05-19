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

import java.io.UnsupportedEncodingException;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.commons.lang3.StringUtils;

public class RowParser extends SimpleFunction<byte[], String[]> {

  private String charsetName;
  private String delimiter;

  public RowParser() {
    this("UTF-8", "\n");
  }

  public RowParser(String charsetName, String delimiter) {
    this.charsetName = charsetName;
    this.delimiter = delimiter;
  }

  public RowParser withCharsetName(String charsetName) {
    this.charsetName = charsetName;
    return this;
  }

  public RowParser withDelimiter(String delimiter) {
    this.delimiter = delimiter;
    return this;
  }

  @Override
  public String[] apply(byte[] bytes) {
    try {
      String fullString = new String(bytes, charsetName);
      return StringUtils.split(fullString, delimiter);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

}
