/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.io.storage.hadoop;

import org.apache.hudi.common.schema.HoodieSchema;

import lombok.Getter;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.TypeDescription;

/**
 * In-memory ORC file format metadata surfaced by {@link HoodieAvroOrcWriter} after the file is
 * written. It carries everything needed to derive column statistics ({@code HoodieColumnRangeMetadata})
 * from the ORC file without re-reading it, playing the same role that {@code ParquetMetadata} plays
 * for Parquet files.
 *
 * <ul>
 *   <li>{@code columnStatistics} - file level {@link ColumnStatistics} captured via
 *       {@code org.apache.orc.Writer#getStatistics()} (indexed by ORC column id).</li>
 *   <li>{@code orcSchema} - the ORC {@link TypeDescription}, used to resolve a column name to its
 *       ORC column id.</li>
 *   <li>{@code schema} - the Hudi {@link HoodieSchema}, used to build the per-column value metadata.</li>
 * </ul>
 */
@Getter
public class OrcColumnStatsMetadata {

  private final ColumnStatistics[] columnStatistics;
  private final TypeDescription orcSchema;
  private final HoodieSchema schema;

  public OrcColumnStatsMetadata(ColumnStatistics[] columnStatistics, TypeDescription orcSchema, HoodieSchema schema) {
    this.columnStatistics = columnStatistics;
    this.orcSchema = orcSchema;
    this.schema = schema;
  }

}
