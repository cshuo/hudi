/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.core.io.storage.HoodieOrcConfig;
import org.apache.hudi.io.storage.hadoop.HoodieAvroOrcWriter;
import org.apache.hudi.io.storage.hadoop.OrcColumnStatsMetadata;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.orc.CompressionKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests that {@link OrcUtils} derives column statistics from the in-memory ORC file format
 * metadata produced by {@link HoodieAvroOrcWriter}.
 */
public class TestOrcColumnStats {

  @TempDir
  Path tempDir;

  private static final String SCHEMA_JSON =
      "{\"type\":\"record\",\"name\":\"testRec\",\"fields\":["
          + "{\"name\":\"rk\",\"type\":\"string\"},"
          + "{\"name\":\"i\",\"type\":[\"null\",\"int\"],\"default\":null},"
          + "{\"name\":\"l\",\"type\":[\"null\",\"long\"],\"default\":null},"
          + "{\"name\":\"f\",\"type\":[\"null\",\"float\"],\"default\":null},"
          + "{\"name\":\"d\",\"type\":[\"null\",\"double\"],\"default\":null},"
          + "{\"name\":\"s\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"b\",\"type\":[\"null\",\"boolean\"],\"default\":null},"
          + "{\"name\":\"dec\",\"type\":[\"null\",{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}],\"default\":null},"
          + "{\"name\":\"dt\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},"
          + "{\"name\":\"ts\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null}"
          + "]}";

  @Test
  public void testReadColumnStatsFromOrcMetadata() throws Exception {
    Schema avroSchema = new Schema.Parser().parse(SCHEMA_JSON);
    HoodieSchema schema = HoodieSchema.fromAvroSchema(avroSchema);
    StoragePath filePath = new StoragePath(tempDir.toString() + "/f1_1-0-1_000.orc");

    BloomFilter filter = BloomFilterFactory.createBloomFilter(1000, 0.00001, -1, BloomFilterTypeCode.SIMPLE.name());
    StorageConfiguration conf = HoodieTestUtils.getDefaultStorageConfWithDefaults();
    int orcStripeSize = Integer.parseInt(HoodieStorageConfig.ORC_STRIPE_SIZE.defaultValue());
    int orcBlockSize = Integer.parseInt(HoodieStorageConfig.ORC_BLOCK_SIZE.defaultValue());
    int maxFileSize = Integer.parseInt(HoodieStorageConfig.ORC_FILE_MAX_SIZE.defaultValue());
    HoodieOrcConfig config = new HoodieOrcConfig(conf, CompressionKind.ZLIB, orcStripeSize, orcBlockSize, maxFileSize, filter);

    HoodieAvroOrcWriter writer = new HoodieAvroOrcWriter("000", filePath, config, schema, new LocalTaskContextSupplier());
    // i: [5, null, 3] ; l: [100, 200, 50] ; f: [2.5, 1.5, 3.5] ; d: [2.5, 1.5, 3.5]
    // s: [banana, apple, cherry] ; b: [true, false, true] ; dec: [12.34, 99.99, 1.00]
    // dt (epoch days): [100, 50, 200] ; ts (epoch millis): [1000, 3000, 2000]
    writer.writeAvro("k1", record(avroSchema, "k1", 5, 100L, 2.5f, 2.5d, "banana", true, new BigDecimal("12.34"), 100, 1000L));
    writer.writeAvro("k2", record(avroSchema, "k2", null, 200L, 1.5f, 1.5d, "apple", false, new BigDecimal("99.99"), 50, 3000L));
    writer.writeAvro("k3", record(avroSchema, "k3", 3, 50L, 3.5f, 3.5d, "cherry", true, new BigDecimal("1.00"), 200, 2000L));
    // getFileFormatMetadata is populated on close.
    writer.close();
    OrcColumnStatsMetadata metadata = (OrcColumnStatsMetadata) writer.getFileFormatMetadata();
    assertNotNull(metadata);

    List<String> columns = Arrays.asList("rk", "i", "l", "f", "d", "s", "b", "dec", "dt", "ts");
    List<HoodieColumnRangeMetadata<Comparable>> stats = new OrcUtils().readColumnStatsFromMetadata(
        metadata, filePath.getName(), Option.of(columns), HoodieIndexVersion.V2);
    Map<String, HoodieColumnRangeMetadata<Comparable>> byColumn = stats.stream()
        .collect(Collectors.toMap(HoodieColumnRangeMetadata::getColumnName, Function.identity()));

    assertEquals(columns.size(), byColumn.size());

    assertRange(byColumn.get("rk"), "k1", "k3", 0L, 3L);
    assertRange(byColumn.get("i"), 3, 5, 1L, 3L);
    assertRange(byColumn.get("l"), 50L, 200L, 0L, 3L);
    assertRange(byColumn.get("f"), 1.5f, 3.5f, 0L, 3L);
    assertRange(byColumn.get("d"), 1.5d, 3.5d, 0L, 3L);
    assertRange(byColumn.get("s"), "apple", "cherry", 0L, 3L);
    assertRange(byColumn.get("b"), false, true, 0L, 3L);
    assertRange(byColumn.get("dec"), new BigDecimal("1.00"), new BigDecimal("99.99"), 0L, 3L);
    assertRange(byColumn.get("dt"), LocalDate.ofEpochDay(50), LocalDate.ofEpochDay(200), 0L, 3L);
    assertRange(byColumn.get("ts"), Instant.ofEpochMilli(1000), Instant.ofEpochMilli(3000), 0L, 3L);
  }

  @Test
  public void testAllNullColumnHasNullRange() throws Exception {
    Schema avroSchema = new Schema.Parser().parse(SCHEMA_JSON);
    HoodieSchema schema = HoodieSchema.fromAvroSchema(avroSchema);
    StoragePath filePath = new StoragePath(tempDir.toString() + "/f2_1-0-1_000.orc");

    BloomFilter filter = BloomFilterFactory.createBloomFilter(1000, 0.00001, -1, BloomFilterTypeCode.SIMPLE.name());
    StorageConfiguration conf = HoodieTestUtils.getDefaultStorageConfWithDefaults();
    HoodieOrcConfig config = new HoodieOrcConfig(conf, CompressionKind.ZLIB,
        Integer.parseInt(HoodieStorageConfig.ORC_STRIPE_SIZE.defaultValue()),
        Integer.parseInt(HoodieStorageConfig.ORC_BLOCK_SIZE.defaultValue()),
        Integer.parseInt(HoodieStorageConfig.ORC_FILE_MAX_SIZE.defaultValue()), filter);

    HoodieAvroOrcWriter writer = new HoodieAvroOrcWriter("000", filePath, config, schema, new LocalTaskContextSupplier());
    // dt is a non-null date column; provide values (0 epoch day) while other columns stay null.
    writer.writeAvro("k1", record(avroSchema, "k1", null, null, null, null, null, null, null, 0, null));
    writer.writeAvro("k2", record(avroSchema, "k2", null, null, null, null, null, null, null, 0, null));
    writer.close();
    OrcColumnStatsMetadata metadata = (OrcColumnStatsMetadata) writer.getFileFormatMetadata();

    Map<String, HoodieColumnRangeMetadata<Comparable>> byColumn = new OrcUtils()
        .readColumnStatsFromMetadata(metadata, filePath.getName(), Option.of(Arrays.asList("i", "s")), HoodieIndexVersion.V2)
        .stream().collect(Collectors.toMap(HoodieColumnRangeMetadata::getColumnName, Function.identity()));

    HoodieColumnRangeMetadata<Comparable> intStats = byColumn.get("i");
    assertNull(intStats.getMinValue());
    assertNull(intStats.getMaxValue());
    assertEquals(2L, intStats.getNullCount());
    assertEquals(2L, intStats.getValueCount());
  }

  @Test
  public void testColumnStatsIncludePendingFinalStripe() throws Exception {
    Schema avroSchema = new Schema.Parser().parse(SCHEMA_JSON);
    HoodieSchema schema = HoodieSchema.fromAvroSchema(avroSchema);
    StoragePath filePath = new StoragePath(tempDir.toString() + "/f3_1-0-1_000.orc");

    BloomFilter filter = BloomFilterFactory.createBloomFilter(1000, 0.00001, -1, BloomFilterTypeCode.SIMPLE.name());
    StorageConfiguration conf = HoodieTestUtils.getDefaultStorageConfWithDefaults();
    // Keep the stripe buffered until close so the test covers statistics from the final pending stripe.
    HoodieOrcConfig config = new HoodieOrcConfig(conf, CompressionKind.ZLIB,
        1 << 30,
        1024,
        Integer.parseInt(HoodieStorageConfig.ORC_FILE_MAX_SIZE.defaultValue()), filter);

    HoodieAvroOrcWriter writer = new HoodieAvroOrcWriter("000", filePath, config, schema, new LocalTaskContextSupplier());
    writer.writeAvro("k1", record(avroSchema, "k1", 42, null, null, null, null, null, null, 0, null));
    writer.close();

    OrcColumnStatsMetadata metadata = (OrcColumnStatsMetadata) writer.getFileFormatMetadata();
    HoodieColumnRangeMetadata<Comparable> intStats = new OrcUtils()
        .readColumnStatsFromMetadata(metadata, filePath.getName(), Option.of(Arrays.asList("i")), HoodieIndexVersion.V2)
        .get(0);

    assertRange(intStats, 42, 42, 0L, 1L);
  }

  private static void assertRange(HoodieColumnRangeMetadata<Comparable> range,
                                  Comparable expectedMin, Comparable expectedMax,
                                  long expectedNullCount, long expectedValueCount) {
    assertNotNull(range);
    assertEquals(expectedMin, range.getMinValue());
    assertEquals(expectedMax, range.getMaxValue());
    assertEquals(expectedNullCount, range.getNullCount());
    assertEquals(expectedValueCount, range.getValueCount());
  }

  private static GenericData.Record record(Schema schema, String rk, Integer i, Long l, Float f, Double d,
                                           String s, Boolean b, BigDecimal dec, Integer dt, Long ts) {
    GenericData.Record record = new GenericData.Record(schema);
    record.put("rk", rk);
    record.put("i", i);
    record.put("l", l);
    record.put("f", f);
    record.put("d", d);
    record.put("s", s);
    record.put("b", b);
    record.put("dec", dec);
    record.put("dt", dt);
    record.put("ts", ts);
    return record;
  }
}
