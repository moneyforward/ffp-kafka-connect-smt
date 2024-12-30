package com.moneyforward.ffp.smt;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.json.JsonWriterSettings;
import org.junit.After;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class StringifyJsonTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(StringifyJsonTest.class);

    private final StringifyJson<SinkRecord> xform = new StringifyJson.Value<>();

    private final Schema keySchema = SchemaBuilder.struct().field("key", Schema.STRING_SCHEMA).build();
    private final Struct key = new Struct(keySchema).put("key", "key");

    private final String propTargetFields = "target_fields";

    @After
    public void teardown() {
        xform.close();
    }

    private void runAssertions(Schema valueSchema, Object inputValue, String outputSchema, String outputValue) {
        final SinkRecord record = new SinkRecord("test", 0, this.keySchema, this.key, valueSchema, inputValue, 0);
        final SinkRecord output = xform.apply(record);
        final Struct inputStruct = (Struct) record.value();
        final Struct outputStruct = (Struct) output.value();

        Assertions.assertEquals(output.key(), record.key(), "record.key");
        Assertions.assertEquals(inputValue.toString(), inputStruct.toString(), "inputValue");
        Assertions.assertEquals(outputValue, outputStruct.toString(), "outputStruct");
        Assertions.assertEquals(outputSchema, outputStruct.schema().fields().toString(), "outputSchema");
    }

    @Test
    public void noSchema() {
        final Map<String, String> props = new HashMap<>();
        props.put(propTargetFields, "target_field");
        xform.configure(props);

        final Map<String, Object> inputValue = new HashMap<>();
        inputValue.put("first_field", 42);
        inputValue.put("other_field", 24);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, inputValue, 0);

        Assertions.assertThrows(DataException.class, () -> {
            xform.apply(record);
        });
    }

    @Test
    public void nullTargetField() {
        final Map<String, String> props = new HashMap<>();
        props.put(propTargetFields, "target_field");
        xform.configure(props);

        final Schema valueSchema = SchemaBuilder.struct()
                .field("target_field", Schema.OPTIONAL_STRING_SCHEMA)
                .field("other_field", Schema.INT32_SCHEMA);
        final Struct inputValue = new Struct(valueSchema)
                .put("target_field", null)
                .put("other_field", 24);

        final String outputValue = "Struct{target_field=null,other_field=24}";
        final String outputSchema = "[Field{name=target_field, index=0, schema=Schema{STRING}}, Field{name=other_field, index=1, schema=Schema{INT32}}]";

        runAssertions(valueSchema, inputValue, outputSchema, outputValue);
    }

    @Test
    public void primitiveField() {
        final Map<String, String> props = new HashMap<>();
        props.put(propTargetFields, "name,age,enabled");
        xform.configure(props);

        final Schema valueSchema = SchemaBuilder.struct()
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("age", Schema.OPTIONAL_INT32_SCHEMA)
                .field("enabled", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .field("other_field", Schema.INT32_SCHEMA);
        final Struct inputValue = new Struct(valueSchema)
                .put("name", "John Doe")
                .put("age", 42)
                .put("enabled", true)
                .put("other_field", 24);

        final String outputValue = "Struct{name=John Doe,age=42,enabled=true,other_field=24}";
        final String outputSchema = "[Field{name=name, index=0, schema=Schema{STRING}}, Field{name=age, index=1, schema=Schema{STRING}}, Field{name=enabled, index=2, schema=Schema{STRING}}, Field{name=other_field, index=3, schema=Schema{INT32}}]";

        runAssertions(valueSchema, inputValue, outputSchema, outputValue);
    }

    @Test
    public void logicalField() {
        final Map<String, String> props = new HashMap<>();
        props.put(propTargetFields, "created_at,decimal_value,time,timestamp");
        xform.configure(props);

        final Schema valueSchema = SchemaBuilder.struct()
                .field("created_at", Date.SCHEMA)
                .field("decimal_value", Decimal.schema(0))
                .field("time", Time.SCHEMA)
                .field("timestamp", Timestamp.SCHEMA)
                .field("other_field", Schema.INT32_SCHEMA);
        final Struct inputValue = new Struct(valueSchema)
                .put("created_at", new java.util.Date(1672531200000L))
                .put("decimal_value", new java.math.BigDecimal(42))
                .put("time", new java.util.Date(1672531200000L))
                .put("timestamp", new java.util.Date(1672531200000L))
                .put("other_field", 24);

        final String outputValue = "Struct{created_at=Sun Jan 01 07:00:00 GMT+07:00 2023,decimal_value=42,time=Sun Jan 01 07:00:00 GMT+07:00 2023,timestamp=Sun Jan 01 07:00:00 GMT+07:00 2023,other_field=24}";
        final String outputSchema = "[Field{name=created_at, index=0, schema=Schema{STRING}}, Field{name=decimal_value, index=1, schema=Schema{STRING}}, Field{name=time, index=2, schema=Schema{STRING}}, Field{name=timestamp, index=3, schema=Schema{STRING}}, Field{name=other_field, index=4, schema=Schema{INT32}}]";

        runAssertions(valueSchema, inputValue, outputSchema, outputValue);
    }

    @Test
    public void avroMapField() {
        final Map<String, String> props = new HashMap<>();
        props.put(propTargetFields, "target_field");
        xform.configure(props);

        org.apache.avro.Schema nestedUnionSchema = org.apache.avro.SchemaBuilder.unionOf().
                nullType()
                .and().stringType()
                .and().booleanType()
                .and().intType()
                .endUnion();
        org.apache.avro.Schema nestedMapSchema = org.apache.avro.SchemaBuilder.map().values(nestedUnionSchema);

        org.apache.avro.Schema unionSchema = org.apache.avro.SchemaBuilder.unionOf().
                nullType()
                .and().stringType()
                .and().booleanType()
                .and().intType()
                .and().type(nestedMapSchema)
                .endUnion();

        org.apache.avro.Schema mapSchema = org.apache.avro.SchemaBuilder.map().values(unionSchema);
        org.apache.avro.Schema recordSchema = org.apache.avro.SchemaBuilder.record("record")
                .fields()
                .name("target_field").type(mapSchema).noDefault()
                .name("other_field").type().intType().noDefault()
                .endRecord();

        AvroData avroData = new AvroData(10);

        GenericData.Record avroRecord = new GenericData.Record(recordSchema);
        avroRecord.put("target_field", new HashMap<>(){{
            put("first", "1st");
            put("second", 2);
            put("third", true);
            put("fourth", null);
            put("fifth", new HashMap<>(){{
                put("eleven", "value-eleven");
                put("twelve", 12);
            }});
        }});
        avroRecord.put("other_field", 111);

        SchemaAndValue schemaAndValue = avroData.toConnectData(recordSchema, avroRecord);

        final String outputValue = "Struct{target_field={\"fifth\": {\"eleven\": \"value-eleven\", \"twelve\": 12}, \"fourth\": null, \"third\": true, \"first\": \"1st\", \"second\": 2},other_field=111}";
        final String outputSchema = "[Field{name=target_field, index=0, schema=Schema{STRING}}, Field{name=other_field, index=1, schema=Schema{INT32}}]";
        runAssertions(schemaAndValue.schema(), schemaAndValue.value(), outputSchema, outputValue);
    }

    @Test
    public void avroArrayField() {
        final Map<String, String> props = new HashMap<>();
        props.put(propTargetFields, "target_field");
        xform.configure(props);

        org.apache.avro.Schema arraySchema = org.apache.avro.SchemaBuilder.array().items().stringType();

        org.apache.avro.Schema recordSchema = org.apache.avro.SchemaBuilder.record("record")
                .fields()
                .name("target_field").type(arraySchema).noDefault()
                .name("other_field").type().intType().noDefault()
                .endRecord();

        AvroData avroData = new AvroData(10);

        GenericData.Record avroRecord = new GenericData.Record(recordSchema);

        avroRecord.put("target_field", new LinkedList<>(){{
            add("first");
            add("second");
        }});
        avroRecord.put("other_field", 111);

        SchemaAndValue schemaAndValue = avroData.toConnectData(recordSchema, avroRecord);

        final String outputValue = "Struct{target_field=[\"first\", \"second\"],other_field=111}";
        final String outputSchema = "[Field{name=target_field, index=0, schema=Schema{STRING}}, Field{name=other_field, index=1, schema=Schema{INT32}}]";
        runAssertions(schemaAndValue.schema(), schemaAndValue.value(), outputSchema, outputValue);
    }

    @Test
    public void avroStructTest() {
        final Map<String, String> props = new HashMap<>();
        props.put(propTargetFields, "target_field");
        xform.configure(props);

        org.apache.avro.Schema nestedSchema = org.apache.avro.SchemaBuilder.record("nested")
                .fields()
                .name("str_field").type().stringType().noDefault()
                .name("int_field").type().intType().noDefault()
                .endRecord();

        org.apache.avro.Schema recordSchema = org.apache.avro.SchemaBuilder.record("record")
                .fields()
                .name("target_field").type(nestedSchema).noDefault()
                .name("other_field").type().intType().noDefault()
                .endRecord();

        AvroData avroData = new AvroData(10);
        GenericData.Record avroRecord = new GenericData.Record(recordSchema);
        GenericData.Record nestedRecord = new GenericData.Record(nestedSchema);

        nestedRecord.put("str_field", "hello world");
        nestedRecord.put("int_field", 42);

        avroRecord.put("target_field", nestedRecord);
        avroRecord.put("other_field", 111);

        SchemaAndValue schemaAndValue = avroData.toConnectData(recordSchema, avroRecord);
        LOGGER.info("SchemaAndValue.schema: {}", schemaAndValue.schema().fields().toString());
        LOGGER.info("SchemaAndValue.value: {}", schemaAndValue.value().toString());

        final String outputValue = "Struct{target_field={\"str_field\": \"hello world\", \"int_field\": 42},other_field=111}";
        final String outputSchema = "[Field{name=target_field, index=0, schema=Schema{STRING}}, Field{name=other_field, index=1, schema=Schema{INT32}}]";
        runAssertions(schemaAndValue.schema(), schemaAndValue.value(), outputSchema, outputValue);
    }

    @Test
    public void bsonArrayTest() {
        BsonArray bsonArray = new BsonArray();
        bsonArray.add(new BsonString("Hello"));
        bsonArray.add(new BsonInt32(42));
        bsonArray.add(BsonDocument.parse("{\"key\": \"value\"}"));

        // Chuyển BsonArray thành chuỗi JSON hợp lệ
        String jsonString = bsonArray.toString();
        // In chuỗi JSON
        System.out.println("Valid JSON String:");
        System.out.println(jsonString);

        BsonDocument temp = new BsonDocument("x", bsonArray);
        String json = temp.toJson(JsonWriterSettings.builder().build());
        System.out.println(json);
        System.out.println(json.substring(6, json.length() - 1));
    }
}
