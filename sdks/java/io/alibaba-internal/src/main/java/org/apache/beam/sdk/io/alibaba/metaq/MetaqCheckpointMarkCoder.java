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
package org.apache.beam.sdk.io.alibaba.metaq;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;

public class MetaqCheckpointMarkCoder extends StructuredCoder<MetaqCheckpointMark> {
  private static final long serialVersionUID = 1L;
  private static Coder<String> NULL_STRING_CODER = NullableCoder.of(StringUtf8Coder.of());

  @Override
  public void encode(MetaqCheckpointMark value, OutputStream outStream) throws IOException {
    NULL_STRING_CODER.encode(value.getOffset(), outStream);
  }

  @Override
  public MetaqCheckpointMark decode(InputStream inStream) throws IOException {
    return new MetaqCheckpointMark(NULL_STRING_CODER.decode(inStream));
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return null;
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
  }
}

