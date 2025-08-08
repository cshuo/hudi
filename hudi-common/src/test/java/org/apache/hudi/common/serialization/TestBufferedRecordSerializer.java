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

package org.apache.hudi.common.serialization;

import org.apache.hudi.avro.AvroRecordSerializer;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordSerializer;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.testutils.SchemaTestUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.Arrays;
import java.nio.charset.StandardCharsets;

public class TestBufferedRecordSerializer {
  @Test
  void testNonNullRecordWithDeleteFlag() throws IOException {
    GenericRecord rec = buildSimpleRecord("mark", 9, "gray");
    AvroRecordSerializer avroRecordSerializer = new AvroRecordSerializer(i -> SchemaTestUtil.getSimpleSchema());
    BufferedRecordSerializer<IndexedRecord> serializer = new BufferedRecordSerializer<>(avroRecordSerializer);
    BufferedRecord<IndexedRecord> bufferedRecord = new BufferedRecord<>("rk-del", 9L, rec, 1, true);
    bufferedRecord.setHoodieOperation(HoodieOperation.UPDATE_BEFORE);
    byte[] bytes = serializer.serialize(bufferedRecord);
    BufferedRecord<IndexedRecord> decoded = serializer.deserialize(bytes);
    Assertions.assertTrue(decoded.isDelete(), "isDelete flag should be preserved");
    Assertions.assertEquals(HoodieOperation.UPDATE_BEFORE, decoded.getHoodieOperation());
    Assertions.assertEquals(bufferedRecord.getRecord().toString(), decoded.getRecord() == null ? null : decoded.getRecord().toString());
  }

  @Test
  void testLargePayloadRoundTrip() throws IOException {
    char[] chars = new char[5000];
    Arrays.fill(chars, x);
    String largeColor = new String(chars);
    GenericRecord rec = buildSimpleRecord("large", 1, largeColor);
    AvroRecordSerializer avroRecordSerializer = new AvroRecordSerializer(i -> SchemaTestUtil.getSimpleSchema());
    BufferedRecordSerializer<IndexedRecord> serializer = new BufferedRecordSerializer<>(avroRecordSerializer);
    BufferedRecord<IndexedRecord> bufferedRecord = new BufferedRecord<>("rk-large", 1L, rec, 1, false);
    byte[] bytes = serializer.serialize(bufferedRecord);
    BufferedRecord<IndexedRecord> decoded = serializer.deserialize(bytes);
    Assertions.assertEquals(bufferedRecord.getRecord().toString(), decoded.getRecord().toString());
  }

  @Test
  void testDeleteRecordWithNullPartitionSerAndDe() throws IOException {
    DeleteRecord del = DeleteRecord.create("del_id", null, 555L);
    BufferedRecord<IndexedRecord> bufferedRecord = BufferedRecords.fromDeleteRecord(del, 10);
    bufferedRecord.setHoodieOperation(HoodieOperation.DELETE);
    AvroRecordSerializer avroRecordSerializer = new AvroRecordSerializer(i -> SchemaTestUtil.getSimpleSchema());
    BufferedRecordSerializer<IndexedRecord> serializer = new BufferedRecordSerializer<>(avroRecordSerializer);
    byte[] bytes = serializer.serialize(bufferedRecord);
    BufferedRecord<IndexedRecord> decoded = serializer.deserialize(bytes);
    Assertions.assertEquals(bufferedRecord.getRecordKey(), decoded.getRecordKey());
    Assertions.assertEquals(bufferedRecord.getOrderingValue(), decoded.getOrderingValue());
    Assertions.assertTrue(decoded.isDelete(), "Decoded record should be marked as delete");
    Assertions.assertNull(decoded.getRecord(), "Delete record should not carry an Avro payload");
  }

  @Test
  void testHoodieMetadataRecordWithMapRoundTrip() throws IOException {
    HoodieMetadataRecord metadataRecord = new HoodieMetadataRecord("__all_partitions__", 3, new HashMap<>(), null, null, null, null);
    AvroRecordSerializer avroRecordSerializer = new AvroRecordSerializer(i -> HoodieMetadataRecord.SCHEMA$);
    BufferedRecordSerializer<IndexedRecord> serializer = new BufferedRecordSerializer<>(avroRecordSerializer);
    BufferedRecord<IndexedRecord> bufferedRecord = new BufferedRecord<>("__all_partitions__", 3L, metadataRecord, 3, false);
    byte[] bytes = serializer.serialize(bufferedRecord);
    BufferedRecord<IndexedRecord> decoded = serializer.deserialize(bytes);
    Assertions.assertEquals(bufferedRecord.getRecordKey(), decoded.getRecordKey());
    Assertions.assertEquals(bufferedRecord.getOrderingValue(), decoded.getOrderingValue());
    Assertions.assertEquals(bufferedRecord.getSchemaId(), decoded.getSchemaId());
    Assertions.assertEquals(bufferedRecord.isDelete(), decoded.isDelete());
    Assertions.assertEquals(bufferedRecord.getHoodieOperation(), decoded.getHoodieOperation());
    for (int i = 0; i < metadataRecord.getSchema().getFields().size(); i++) {
      Assertions.assertEquals(metadataRecord.get(i), decoded.getRecord().get(i));
    }
  }

  @Test
  void testNonDefaultSchemaIdRoundTrip() throws IOException {
    GenericRecord rec = buildSimpleRecord("alice", 42, "green");
    AvroRecordSerializer avroRecordSerializer = new AvroRecordSerializer(i -> SchemaTestUtil.getSimpleSchema());
    BufferedRecordSerializer<IndexedRecord> serializer = new BufferedRecordSerializer<>(avroRecordSerializer);
    BufferedRecord<IndexedRecord> bufferedRecord = new BufferedRecord<>("rk2", 42L, rec, 99, false);
    byte[] bytes = serializer.serialize(bufferedRecord);
    BufferedRecord<IndexedRecord> decoded = serializer.deserialize(bytes);
    Assertions.assertEquals(99, decoded.getSchemaId());
    Assertions.assertEquals(bufferedRecord.getRecord().toString(), decoded.getRecord().toString());
  }

  @ParameterizedTest
  @EnumSource(value = HoodieOperation.class, names = {"INSERT", "UPSERT", "BULK_INSERT"})
  void testHoodieOperationRoundTrip(HoodieOperation op) throws IOException {
    GenericRecord rec = buildSimpleRecord("bob", 7, "blue");
    AvroRecordSerializer avroRecordSerializer = new AvroRecordSerializer(i -> SchemaTestUtil.getSimpleSchema());
    BufferedRecordSerializer<IndexedRecord> serializer = new BufferedRecordSerializer<>(avroRecordSerializer);
    BufferedRecord<IndexedRecord> bufferedRecord = new BufferedRecord<>("rk", 7L, rec, 1, false);
    bufferedRecord.setHoodieOperation(op);
    byte[] bytes = serializer.serialize(bufferedRecord);
    BufferedRecord<IndexedRecord> decoded = serializer.deserialize(bytes);
    Assertions.assertEquals(op, decoded.getHoodieOperation(), "HoodieOperation should be preserved");
    Assertions.assertEquals(bufferedRecord.getRecord().toString(), decoded.getRecord().toString());
  }

  @Test
  void testNullRecordNonDeleteRoundTrip() throws IOException {
    AvroRecordSerializer avroRecordSerializer = new AvroRecordSerializer(i -> SchemaTestUtil.getSimpleSchema());
    BufferedRecordSerializer<IndexedRecord> serializer = new BufferedRecordSerializer<>(avroRecordSerializer);
    BufferedRecord<IndexedRecord> bufferedRecord = new BufferedRecord<>("rk-null", 123L, null, 1, false);
    byte[] bytes = serializer.serialize(bufferedRecord);
    BufferedRecord<IndexedRecord> decoded = serializer.deserialize(bytes);
    Assertions.assertEquals(bufferedRecord.getRecordKey(), decoded.getRecordKey());
    Assertions.assertEquals(bufferedRecord.getOrderingValue(), decoded.getOrderingValue());
    Assertions.assertEquals(bufferedRecord.getSchemaId(), decoded.getSchemaId());
    Assertions.assertEquals(bufferedRecord.isDelete(), decoded.isDelete());
    Assertions.assertEquals(bufferedRecord.getHoodieOperation(), decoded.getHoodieOperation());
    Assertions.assertNull(decoded.getRecord(), "Record payload should remain null on round-trip");
  }

  @Test
  void testDeserializeGarbageBytesThrows() throws IOException {
    AvroRecordSerializer avroRecordSerializer = new AvroRecordSerializer(i -> SchemaTestUtil.getSimpleSchema());
    BufferedRecordSerializer<IndexedRecord> serializer = new BufferedRecordSerializer<>(avroRecordSerializer);
    byte[] garbage = "not-a-valid-buffered-record".getBytes(StandardCharsets.UTF_8);
    Assertions.assertThrows(Exception.class, () -> serializer.deserialize(garbage),
        "Deserializing garbage bytes should fail with an exception");
  }

  /**
   * Build a simple Avro record using SchemaTestUtil.getSimpleSchema().
   */
  private GenericRecord buildSimpleRecord(String name, Integer favoriteNumber, String favoriteColor) throws IOException {
    Schema schema = SchemaTestUtil.getSimpleSchema();
    GenericRecord record = new GenericData.Record(schema);
    record.put("name", name);
    record.put("favorite_number", favoriteNumber);
    record.put("favorite_color", favoriteColor);
    return record;
  }

  @Test
  void testAvroRecordSerAndDe() throws IOException {
    Schema schema = SchemaTestUtil.getSimpleSchema();
    GenericRecord record = new GenericData.Record(schema);
    record.put("name", "lily");
    record.put("favorite_number", 100);
    record.put("favorite_color", "red");
    AvroRecordSerializer avroRecordSerializer = new AvroRecordSerializer(integer -> schema);
    byte[] avroBytes = avroRecordSerializer.serialize(record);
    IndexedRecord result = avroRecordSerializer.deserialize(avroBytes, 1);
    Assertions.assertEquals(record, result);

    avroRecordSerializer = new AvroRecordSerializer(integer -> HoodieMetadataRecord.SCHEMA$);
    HoodieMetadataRecord metadataRecord = new HoodieMetadataRecord("__all_partitions__", 1, new HashMap<>(), null, null, null, null);
    avroBytes = avroRecordSerializer.serialize(metadataRecord);
    result = avroRecordSerializer.deserialize(avroBytes, 1);
    for (int i = 0; i < metadataRecord.getSchema().getFields().size(); i++) {
      Assertions.assertEquals(metadataRecord.get(i), result.get(i));
    }
  }

  @Test
  void testBufferedRecordSerAndDe() throws IOException {
    Schema schema = SchemaTestUtil.getSimpleSchema();
    GenericRecord record = new GenericData.Record(schema);
    record.put("name", "lily");
    record.put("favorite_number", 100);
    record.put("favorite_color", "red");
    BufferedRecord<IndexedRecord> bufferedRecord = new BufferedRecord<>("id", 100, record, 1, false);

    AvroRecordSerializer avroRecordSerializer = new AvroRecordSerializer(integer -> schema);
    BufferedRecordSerializer<IndexedRecord> bufferedRecordSerializer = new BufferedRecordSerializer<>(avroRecordSerializer);

    byte[] bytes = bufferedRecordSerializer.serialize(bufferedRecord);
    BufferedRecord<IndexedRecord> result = bufferedRecordSerializer.deserialize(bytes);
    Assertions.assertEquals(bufferedRecord, result);

    avroRecordSerializer = new AvroRecordSerializer(integer -> HoodieMetadataRecord.SCHEMA$);
    bufferedRecordSerializer = new BufferedRecordSerializer<>(avroRecordSerializer);
    HoodieMetadataRecord metadataRecord = new HoodieMetadataRecord("__all_partitions__", 1, new HashMap<>(), null, null, null, null);
    bufferedRecord = new BufferedRecord<>("__all_partitions__", 0, metadataRecord, 1, false);
    bytes = bufferedRecordSerializer.serialize(bufferedRecord);
    result = bufferedRecordSerializer.deserialize(bytes);

    Assertions.assertEquals(bufferedRecord.getRecordKey(), result.getRecordKey());
    Assertions.assertEquals(bufferedRecord.getOrderingValue(), result.getOrderingValue());
    Assertions.assertEquals(bufferedRecord.getSchemaId(), result.getSchemaId());
    Assertions.assertEquals(bufferedRecord.isDelete(), result.isDelete());
    Assertions.assertEquals(bufferedRecord.getHoodieOperation(), result.getHoodieOperation());
    for (int i = 0; i < metadataRecord.getSchema().getFields().size(); i++) {
      Assertions.assertEquals(metadataRecord.get(i), result.getRecord().get(i));
    }
    // assert the records are equivalent
    Assertions.assertEquals(bufferedRecord.getRecord().toString(), result.getRecord().toString());
  }

  @ParameterizedTest
  @EnumSource(value = HoodieOperation.class,  names = {"UPDATE_BEFORE", "DELETE"})
  void testDeleteRecordSerAndDe(HoodieOperation hoodieOperation) throws IOException {
    Schema schema = SchemaTestUtil.getSimpleSchema();
    DeleteRecord record = DeleteRecord.create("id", "partition", 100);
    BufferedRecord<IndexedRecord> bufferedRecord = BufferedRecords.fromDeleteRecord(record, 100);
    bufferedRecord.setHoodieOperation(hoodieOperation);

    AvroRecordSerializer avroRecordSerializer = new AvroRecordSerializer(integer -> schema);
    BufferedRecordSerializer<IndexedRecord> bufferedRecordSerializer = new BufferedRecordSerializer<>(avroRecordSerializer);

    byte[] bytes = bufferedRecordSerializer.serialize(bufferedRecord);
    BufferedRecord<IndexedRecord> result = bufferedRecordSerializer.deserialize(bytes);

    Assertions.assertEquals(bufferedRecord.getRecordKey(), result.getRecordKey());
    Assertions.assertEquals(bufferedRecord.getOrderingValue(), result.getOrderingValue());
    Assertions.assertEquals(bufferedRecord.getSchemaId(), result.getSchemaId());
    Assertions.assertEquals(bufferedRecord.isDelete(), result.isDelete());
    Assertions.assertEquals(bufferedRecord.getHoodieOperation(), result.getHoodieOperation());
    Assertions.assertNull(result.getRecord());
  }
}
