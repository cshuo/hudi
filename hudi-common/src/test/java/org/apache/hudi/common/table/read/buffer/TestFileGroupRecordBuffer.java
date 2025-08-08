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

package org.apache.hudi.common.table.read.buffer;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.IteratorMode;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.apache.hudi.common.table.read.buffer.FileGroupRecordBuffer.getOrderingValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import java.util.HashMap;
import java.util.function.Function;

/**
 * Tests {@link FileGroupRecordBuffer}
 */
class TestFileGroupRecordBuffer {
  // Note: Testing framework: JUnit 5 (Jupiter) with Mockito, following project conventions.
  private String schemaString = "{"
      + "\"type\": \"record\","
      + "\"name\": \"EventRecord\","
      + "\"namespace\": \"com.example.avro\","
      + "\"fields\": ["
      + "{\"name\": \"id\", \"type\": \"string\"},"
      + "{\"name\": \"ts\", \"type\": \"long\"},"
      + "{\"name\": \"op\", \"type\": \"string\"},"
      + "{\"name\": \"_hoodie_is_deleted\", \"type\": \"boolean\"}"
      + "]"
      + "}";
  private Schema schema = new Schema.Parser().parse(schemaString);
  private final HoodieReaderContext readerContext = mock(HoodieReaderContext.class);
  private final RecordContext recordContext = mock(RecordContext.class);
  private final FileGroupReaderSchemaHandler schemaHandler =
      mock(FileGroupReaderSchemaHandler.class);
  private final UpdateProcessor updateProcessor = mock(UpdateProcessor.class);
  private HoodieTableMetaClient hoodieTableMetaClient = mock(HoodieTableMetaClient.class);
  private TypedProperties props;
  private HoodieReadStats readStats = mock(HoodieReadStats.class);

  @BeforeEach
  void setUp() {
    props = new TypedProperties();
    when(readerContext.getRecordContext()).thenReturn(recordContext);
    when(readerContext.getSchemaHandler()).thenReturn(schemaHandler);
    when(schemaHandler.getRequiredSchema()).thenReturn(schema);
    when(schemaHandler.getDeleteContext()).thenReturn(new DeleteContext(props, schema));
    when(readerContext.getRecordMerger()).thenReturn(Option.empty());
    when(readerContext.getRecordSerializer()).thenReturn(new DefaultSerializer<>());
    when(readerContext.getRecordSizeEstimator()).thenReturn(new DefaultSizeEstimator<>());
    when(readerContext.getIteratorMode()).thenReturn(IteratorMode.ENGINE_RECORD);
  }

  @Test
  void testGetOrderingValueFromDeleteRecord() {
    HoodieReaderContext readerContext = mock(HoodieReaderContext.class);
    DeleteRecord deleteRecord = mock(DeleteRecord.class);
    mockDeleteRecord(deleteRecord, null);
    assertEquals(OrderingValues.getDefault(), getOrderingValue(readerContext, deleteRecord));
    mockDeleteRecord(deleteRecord, OrderingValues.getDefault());
    assertEquals(OrderingValues.getDefault(), getOrderingValue(readerContext, deleteRecord));
    Comparable orderingValue = "xyz";
    Comparable convertedValue = "_xyz";
    mockDeleteRecord(deleteRecord, orderingValue);
    when(recordContext.convertOrderingValueToEngineType(orderingValue)).thenReturn(convertedValue);
    when(readerContext.getRecordContext()).thenReturn(recordContext);
    assertEquals(convertedValue, getOrderingValue(readerContext, deleteRecord));
  }

  @ParameterizedTest
  @CsvSource({"true,false", "false,true"})
  void testInvalidCustomDeleteConfigs(boolean configureCustomDeleteKey,
                                      boolean configureCustomDeleteMarker) {
    String customDeleteKey = "colC";
    String customDeleteValue = "D";
    List<String> dataSchemaFields = new ArrayList<>(Arrays.asList(
        HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD,
        "colA", "colB", "colC", "colD"));

    Schema dataSchema = SchemaTestUtil.getSchemaFromFields(dataSchemaFields);

    TypedProperties props = new TypedProperties();
    if (configureCustomDeleteKey) {
      props.setProperty(DELETE_KEY, customDeleteKey);
    }
    if (configureCustomDeleteMarker) {
      props.setProperty(DELETE_MARKER, customDeleteValue);
    }
    Throwable exception = assertThrows(IllegalArgumentException.class,
        () -> new DeleteContext(props, dataSchema));
    assertEquals("Either custom delete key or marker is not specified",
        exception.getMessage());
  }

  private void mockDeleteRecord(DeleteRecord deleteRecord,
                                Comparable orderingValue) {
    when(deleteRecord.getOrderingValue()).thenReturn(orderingValue);
  }

  @Test
  void testIsCustomDeleteRecord() {
    String customDeleteKey = "op";
    String customDeleteValue = "d";
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "12345");
    record.put("ts", System.currentTimeMillis());
    record.put(customDeleteKey, "d");
    when(recordContext.isDeleteRecord(any(), any())).thenCallRealMethod();

    props.setProperty(DELETE_KEY, customDeleteKey);
    props.setProperty(DELETE_MARKER, customDeleteValue);
    DeleteContext deleteContext = new DeleteContext(props, schema);
    when(recordContext.getValue(any(), any(), any())).thenReturn(null);
    assertFalse(recordContext.isDeleteRecord(record, deleteContext));

    props.setProperty(DELETE_KEY, customDeleteKey);
    props.setProperty(DELETE_MARKER, customDeleteValue);
    when(recordContext.getValue(eq(record), any(), eq(customDeleteKey))).thenReturn("d");
    assertTrue(readerContext.getRecordContext().isDeleteRecord(record, deleteContext));
  }

  @Test
  void testProcessCustomDeleteRecord() throws IOException {
    String customDeleteKey = "op";
    String customDeleteValue = "d";
    props.setProperty(DELETE_KEY, customDeleteKey);
    props.setProperty(DELETE_MARKER, customDeleteValue);
    KeyBasedFileGroupRecordBuffer keyBasedBuffer =
        new KeyBasedFileGroupRecordBuffer(
            readerContext,
            hoodieTableMetaClient,
            RecordMergeMode.COMMIT_TIME_ORDERING,
            PartialUpdateMode.NONE,
            props,
            Collections.emptyList(),
            updateProcessor
        );

    // CASE 1: With custom delete marker.
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "12345");
    record.put("ts", System.currentTimeMillis());
    record.put("op", "d");
    record.put("_hoodie_is_deleted", false);
    when(recordContext.getOrderingValue(any(), any(), anyList())).thenReturn(1);
    when(recordContext.convertOrderingValueToEngineType(any())).thenReturn(1);
    BufferedRecord<GenericRecord> bufferedRecord = BufferedRecords.fromEngineRecord(record, schema, readerContext.getRecordContext(), Collections.singletonList("ts"), true);

    keyBasedBuffer.processNextDataRecord(bufferedRecord, "12345");
    Map<Serializable, BufferedRecord<GenericRecord>> records = keyBasedBuffer.getLogRecords();
    assertEquals(1, records.size());
    BufferedRecord<GenericRecord> deleteRecord = records.get("12345");
    assertNull(deleteRecord.getRecordKey(), "The record key metadata field is missing");
    assertEquals(1, deleteRecord.getOrderingValue());

    // CASE 2: With _hoodie_is_deleted is true.
    GenericRecord anotherRecord = new GenericData.Record(schema);
    anotherRecord.put("id", "54321");
    anotherRecord.put("ts", System.currentTimeMillis());
    anotherRecord.put("op", "i");
    anotherRecord.put("_hoodie_is_deleted", true);
    bufferedRecord = BufferedRecords.fromEngineRecord(anotherRecord, schema, readerContext.getRecordContext(), Collections.singletonList("ts"), true);

    keyBasedBuffer.processNextDataRecord(bufferedRecord, "54321");
    records = keyBasedBuffer.getLogRecords();
    assertEquals(2, records.size());
    deleteRecord = records.get("54321");
    assertNull(deleteRecord.getRecordKey(), "The record key metadata field is missing");
    assertEquals(1, deleteRecord.getOrderingValue());
  }
}

  @Test
  @DisplayName("DeleteContext initializes successfully when both custom delete key and marker are set")
  void testValidCustomDeleteConfigs() {
    // Given
    String customDeleteKey = "op";
    String customDeleteValue = "d";
    TypedProperties localProps = new TypedProperties();
    localProps.setProperty(DELETE_KEY, customDeleteKey);
    localProps.setProperty(DELETE_MARKER, customDeleteValue);

    // When / Then: constructing DeleteContext should not throw
    DeleteContext ctx = new DeleteContext(localProps, schema);
    assertEquals(customDeleteKey, localProps.getString(DELETE_KEY));
    assertEquals(customDeleteValue, localProps.getString(DELETE_MARKER));
    // Basic sanity: schema should be the one provided
    assertEquals(schema, ctx.getSchema());
  }

  @Test
  @DisplayName("Default delete detection via _hoodie_is_deleted when no custom delete configs are set")
  void testDefaultDeleteDetectionWithoutCustomConfig() {
    // No custom delete configs
    TypedProperties localProps = new TypedProperties();
    DeleteContext deleteContext = new DeleteContext(localProps, schema);

    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "abc");
    record.put("ts", 1000L);
    record.put("op", "i");
    record.put("_hoodie_is_deleted", true);

    // Use real method for isDeleteRecord
    when(recordContext.isDeleteRecord(any(), any())).thenCallRealMethod();
    when(recordContext.getValue(eq(record), any(), eq("_hoodie_is_deleted"))).thenReturn(true);

    assertTrue(recordContext.isDeleteRecord(record, deleteContext),
        "Record should be treated as delete when _hoodie_is_deleted is true and no custom config is set");
  }

  @Test
  @DisplayName("Non-delete record should be buffered as a regular record (not converted to delete)")
  void testProcessNonDeleteRecordBufferedNormally() throws IOException {
    // Given: no custom delete configs and _hoodie_is_deleted = false
    TypedProperties localProps = new TypedProperties();
    KeyBasedFileGroupRecordBuffer buffer =
        new KeyBasedFileGroupRecordBuffer(
            readerContext,
            hoodieTableMetaClient,
            RecordMergeMode.COMMIT_TIME_ORDERING,
            PartialUpdateMode.NONE,
            localProps,
            Collections.emptyList(),
            updateProcessor
        );

    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "live-1");
    record.put("ts", 1111L);
    record.put("op", "i");
    record.put("_hoodie_is_deleted", false);

    when(recordContext.getOrderingValue(any(), any(), anyList())).thenReturn(1111L);
    when(recordContext.convertOrderingValueToEngineType(any())).thenReturn(1111L);

    BufferedRecord<GenericRecord> bufferedRecord =
        BufferedRecords.fromEngineRecord(record, schema, readerContext.getRecordContext(), Collections.singletonList("ts"), true);

    // When
    buffer.processNextDataRecord(bufferedRecord, "live-1");

    // Then
    Map<Serializable, BufferedRecord<GenericRecord>> records = buffer.getLogRecords();
    assertEquals(1, records.size(), "One non-delete record should be buffered");
    BufferedRecord<GenericRecord> liveRecord = records.get("live-1");
    // For a non-delete record, record key should remain set (not null)
    assertEquals("live-1", liveRecord.getRecordKey(), "Non-delete record should retain its record key");
    assertEquals(1111L, liveRecord.getOrderingValue(), "Ordering value should reflect 'ts' field");
  }

  @Test
  @DisplayName("Custom delete detection ignores mismatched marker values")
  void testCustomDeleteDetectionMismatchMarker() {
    // Given: custom delete key is present but value is mismatched
    String customDeleteKey = "op";
    String customDeleteValue = "d";
    props.setProperty(DELETE_KEY, customDeleteKey);
    props.setProperty(DELETE_MARKER, customDeleteValue);
    DeleteContext deleteContext = new DeleteContext(props, schema);

    GenericRecord record = new GenericData.Record(schema);
    record.put("id", "no-del");
    record.put("ts", 2000L);
    record.put(customDeleteKey, "u"); // not equal to 'd'
    record.put("_hoodie_is_deleted", false);

    when(recordContext.isDeleteRecord(any(), any())).thenCallRealMethod();
    when(recordContext.getValue(eq(record), any(), eq(customDeleteKey))).thenReturn("u");

    assertFalse(recordContext.isDeleteRecord(record, deleteContext),
        "Record should not be considered a delete when custom marker mismatches");
  }

  @ParameterizedTest(name = "Ordering value conversion handles input: {0}")
  @ValueSource(strings = {"", "   ", "123", "xyz"})
  void testGetOrderingValueFromDeleteRecordVariousInputs(String raw) {
    HoodieReaderContext ctx = mock(HoodieReaderContext.class);
    RecordContext localRecordContext = mock(RecordContext.class);
    DeleteRecord del = mock(DeleteRecord.class);

    when(del.getOrderingValue()).thenReturn(raw);
    when(localRecordContext.convertOrderingValueToEngineType(any())).thenAnswer(inv -> {
      Object v = inv.getArguments()[0];
      if (v == null) return null;
      // Simulate a conversion that prefixes underscore, like in existing test
      return "_" + v.toString();
    });
    when(ctx.getRecordContext()).thenReturn(localRecordContext);

    Comparable ordering = getOrderingValue(ctx, del);
    if (raw == null || raw.isEmpty() || raw.trim().isEmpty()) {
      // For empty-like strings, converted non-null is allowed; assert matches conversion
      assertEquals("_" + raw, ordering);
    } else {
      assertEquals("_" + raw, ordering);
    }
  }

  @Test
  @DisplayName("Processing multiple records for same key should keep latest by ordering value")
  void testProcessMultipleRecordsWithOrdering() throws IOException {
    // Given
    TypedProperties localProps = new TypedProperties();
    KeyBasedFileGroupRecordBuffer buffer =
        new KeyBasedFileGroupRecordBuffer(
            readerContext,
            hoodieTableMetaClient,
            RecordMergeMode.COMMIT_TIME_ORDERING,
            PartialUpdateMode.NONE,
            localProps,
            Collections.emptyList(),
            updateProcessor
        );

    // First record with lower ts
    GenericRecord r1 = new GenericData.Record(schema);
    r1.put("id", "dupKey");
    r1.put("ts", 1L);
    r1.put("op", "i");
    r1.put("_hoodie_is_deleted", false);

    // Second record with higher ts
    GenericRecord r2 = new GenericData.Record(schema);
    r2.put("id", "dupKey");
    r2.put("ts", 2L);
    r2.put("op", "u");
    r2.put("_hoodie_is_deleted", false);

    // Mock ordering conversions
    when(recordContext.getOrderingValue(any(), any(), anyList())).thenAnswer(inv -> {
      GenericRecord rec = inv.getArgument(0);
      return rec.get("ts");
    });
    when(recordContext.convertOrderingValueToEngineType(any())).thenAnswer(inv -> inv.getArgument(0));

    BufferedRecord<GenericRecord> br1 = BufferedRecords.fromEngineRecord(r1, schema, readerContext.getRecordContext(), Collections.singletonList("ts"), true);
    BufferedRecord<GenericRecord> br2 = BufferedRecords.fromEngineRecord(r2, schema, readerContext.getRecordContext(), Collections.singletonList("ts"), true);

    // When: process both, second should override first due to higher ordering value
    buffer.processNextDataRecord(br1, "dupKey");
    buffer.processNextDataRecord(br2, "dupKey");

    // Then
    Map<Serializable, BufferedRecord<GenericRecord>> records = buffer.getLogRecords();
    assertEquals(1, records.size(), "Only one record (latest) should remain for the same key");
    BufferedRecord<GenericRecord> latest = records.get("dupKey");
    assertEquals(2L, latest.getOrderingValue(), "Latest record by ordering value should be retained");
    assertEquals("dupKey", latest.getRecordKey(), "Record key should be preserved for non-deletes");
  }

  @Test
  @DisplayName("Gracefully handle null DeleteRecord ordering value (defaults applied)")
  void testGetOrderingValueFromDeleteRecordNull() {
    HoodieReaderContext ctx = mock(HoodieReaderContext.class);
    DeleteRecord del = mock(DeleteRecord.class);
    when(del.getOrderingValue()).thenReturn(null);

    // When RecordContext is absent or conversion not applicable, should default
    assertEquals(OrderingValues.getDefault(), getOrderingValue(ctx, del));

    // When RecordContext present but null incoming, still default
    RecordContext localRecordContext = mock(RecordContext.class);
    when(ctx.getRecordContext()).thenReturn(localRecordContext);
    assertEquals(OrderingValues.getDefault(), getOrderingValue(ctx, del));
  }

