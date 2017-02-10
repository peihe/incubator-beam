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

import com.google.auto.value.AutoValue;
import java.io.IOException;

/**
 *
 */
public abstract class MatchResult {

  private MatchResult() {}

  public static MatchResult create(final Status status, final Metadata[] metadata) {
    return new MatchResult() {
      @Override
      public Status status() {
        return status;
      }

      @Override
      public Metadata[] metadata() throws IOException {
        return metadata;
      }
    };
  }

  public static MatchResult create(final Status status, final IOException e) {
    return new MatchResult() {
      @Override
      public Status status() {
        return status;
      }

      @Override
      public Metadata[] metadata() throws IOException {
        throw e;
      }
    };
  }

  public static MatchResult unknown() {
    return new MatchResult() {
      @Override
      public Status status() {
        return Status.UNKNOWN;
      }

      @Override
      public Metadata[] metadata() throws IOException {
        throw new IOException("MatchResult status is UNKNOWN, and metadata is not available.");
      }
    };
  }

  abstract public Status status();
  abstract public Metadata[] metadata() throws IOException;

  @AutoValue
  public abstract static class Metadata {
    abstract public ResourceId resourceId();
    abstract public long sizeBytes();
    abstract public boolean isReadSeekEfficient();

    static public Builder builder() {
      return new AutoValue_MatchResult_Metadata.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      abstract public Builder setResourceId(ResourceId value);
      abstract public Builder setSizeBytes(long value);
      abstract public Builder setIsReadSeekEfficient(boolean value);
      abstract public Metadata build();
    }
  }

  public enum Status {
    UNKNOWN,
    OK,
    NOT_FOUND,
    ERROR,
  }
}
