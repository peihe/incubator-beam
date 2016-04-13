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
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.options.BigQueryOptions;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobConfigurationLoad;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;

import java.io.IOException;
import java.io.Serializable;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * An interface for real, mock, or fake implementations of Cloud BigQuery services.
 */
public interface BigQueryServices extends Serializable {

  /**
   * Returns a real, mock, or fake {@link JobService}.
   */
  public JobService getJobService(BigQueryOptions bqOptions);

  /**
   * Returns a real, mock, or fake {@link TableService}.
   */
  public TableService getTableService(BigQueryOptions bqOptions);

  public Reader getReaderFromTable(BigQueryOptions bqOptions, TableReference tableRef);

  public Reader getReaderFromQuery(
      BigQueryOptions bqOptions, String query, String projectId, @Nullable Boolean flatten);

  /**
   * An interface for the Cloud BigQuery load service.
   */
  public interface JobService {
    /**
     * Start a BigQuery load job.
     */
    public void startLoadJob(String jobId, JobConfigurationLoad loadConfig)
        throws InterruptedException, IOException;
    /**
     * Start a BigQuery query job.
     */
    public void startExtractJob(String jobId, JobConfigurationExtract extractConfig)
        throws InterruptedException, IOException;

    /**
     * Start a BigQuery extract job.
     */
    public void startQueryJob(String jobId, JobConfigurationQuery query)
        throws IOException, InterruptedException;

    /**
     * Poll the status of a BigQuery load job.
     *
     * <p>Retries the poll request for at most {@code maxAttempts} times until the job is DONE.
     */
    public Job pollJobStatus(String projectId, String jobId, int maxAttempts)
        throws InterruptedException, IOException;
  }

  /**
   * An interface for the Cloud BigQuery table service.
   */
  public interface TableService {
    /**
     * Gets the specified table resource by table ID.
     */
    public Table getTable(String projectId, String datasetId, String tableId)
        throws InterruptedException, IOException;
  }

  /**
   * An interface to read the Cloud BigQuery directly.
   */
  public interface Reader {
    /**
     * Initializes the reader and advances the reader to the first record.
     */
    public boolean start() throws IOException;

    /**
     * Advances the reader to the next valid record.
     */
    public boolean advance() throws IOException;

    /**
     * Returns the value of the data item that was read by the last {@link #start} or
     * {@link #advance} call. The returned value must be effectively immutable and remain valid
     * indefinitely.
     *
     * <p>Multiple calls to this method without an intervening call to {@link #advance} should
     * return the same result.
     *
     * @throws java.util.NoSuchElementException if {@link #start} was never called, or if
     *         the last {@link #start} or {@link #advance} returned {@code false}.
     */
    public TableRow getCurrent() throws NoSuchElementException;

    /**
     * Closes the reader. The reader cannot be used after this method is called.
     */
    public void close() throws IOException;
  }
}
