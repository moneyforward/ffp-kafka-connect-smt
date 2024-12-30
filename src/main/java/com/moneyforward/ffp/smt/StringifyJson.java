package com.moneyforward.ffp.smt;

import com.moneyforward.ffp.smt.converter.Converter;
import com.moneyforward.ffp.smt.converter.JsonConverter;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class StringifyJson<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(StringifyJson.class);
    private static final String PURPOSE = "StringifyJson SMT";
    private static final Converter converter = new JsonConverter();

    private interface ConfigName {
        String FIELD_NAME = "target_fields";
    }

    private List<String> targetFields;

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELD_NAME,
                    ConfigDef.Type.LIST,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Field name to stringify");

    @Override
    public R apply(R record) {
        LOGGER.info("Applying StringifyJson transformation. Record {}", record);
        if (operatingSchema(record) == null) {
            throw new DataException("StringifyJson transformation expects a record with schema");
        }

        Object recordValue = operatingValue(record);
        if (recordValue == null) {
            LOGGER.info("Record value is null. Record {}", record);
            return record;
        }

        final Struct value = requireStruct(recordValue, PURPOSE);
        HashMap<String, String> convertedFields = stringifyFields(value, targetFields);
        if (convertedFields.isEmpty()) {
            LOGGER.info("No target fields present in the record, nothing changed. Record {}", record);
            return record;
        }
        LOGGER.info("Converted fields: {}", convertedFields);

        final Schema updatedSchema = makeUpdatedSchema(null, value, convertedFields);
        final Struct updatedValue = makeUpdatedValue(null, value, updatedSchema, convertedFields);

        LOGGER.info("Updated value: {}", updatedValue);

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(String parentKey, Struct value, HashMap<String, String> stringifiedFields) {
        if (value == null || value.schema() == null) {
            return null;
        }

        final SchemaBuilder builder = SchemaBuilder.struct();
        for (Field field : value.schema().fields()) {
            final Schema fieldSchema;
            final String absoluteKey = joinKeys(parentKey, field.name());

            if (stringifiedFields.containsKey(absoluteKey)) {
                fieldSchema = field.schema().isOptional() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
            } else if (field.schema().type().equals(Schema.Type.STRUCT)) {
                Schema sb = makeUpdatedSchema(absoluteKey, value.getStruct(field.name()), stringifiedFields);
                fieldSchema = (sb != null) ? sb : SchemaBuilder.struct().optional().build();
            } else {
                fieldSchema = field.schema();
            }
            builder.field(field.name(), fieldSchema);
        }

        return builder.build();
    }

    private Struct makeUpdatedValue(String parentKey, Struct value, Schema updatedSchema, HashMap<String, String> stringifiedFields) {
        if (value == null) {
            return null;
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            final Object fieldValue;
            final String absoluteKey = joinKeys(parentKey, field.name());

            if (stringifiedFields.containsKey(absoluteKey)) {
                fieldValue = stringifiedFields.get(absoluteKey);
            } else if (field.schema().type().equals(Schema.Type.STRUCT)) {
                fieldValue = makeUpdatedValue(absoluteKey, value.getStruct(field.name()),
                        updatedSchema.field(field.name()).schema(), stringifiedFields);
            } else {
                fieldValue = value.get(field.name());
            }

            updatedValue.put(field.name(), fieldValue);
        }

        return updatedValue;
    }

    private String joinKeys(String parent, String child) {
        if (parent == null) {
            return child;
        }
        return parent + "." + child;
    }

    private static HashMap<String, String> stringifyFields(Struct value, List<String> targetFields) {
        HashMap<String, String> result = new HashMap<>(targetFields.size());
        final Schema valueSchema = value.schema();

        LOGGER.info("Stringify value {} with target fields {}", value, targetFields);

        for (String field : targetFields) {
            String[] pathArr = field.split("\\.");
            List<String> path = Arrays.asList(pathArr);

            Field schemaField = getSchemaField(path, valueSchema);
            if (schemaField == null) {
                LOGGER.warn("target field {} is not present in the record schema {}", field, valueSchema);
                continue;
            }

            Object fieldValue = getFieldValue(path, value);
            if (fieldValue == null) {
                result.put(field, "null");
                LOGGER.warn("target field {} is not present in the record value {}", field, value);
                continue;
            }

            Schema.Type fieldValueType = schemaField.schema().type();
            if (fieldValueType.equals(Schema.Type.STRUCT)
                    || fieldValueType.equals(Schema.Type.MAP)
                    || fieldValueType.equals(Schema.Type.ARRAY)) {
                LOGGER.info("target field {} is a complex type {}, converting to JSON string", field, fieldValueType);
                BsonDocument bsonDoc = converter.convert(schemaField, fieldValue);
                LOGGER.info("Converted BsonDocument {}", bsonDoc);
                if (fieldValueType.equals(Schema.Type.ARRAY)) {
                    BsonArray innerDoc = bsonDoc.get(schemaField.name()).asArray();
                    BsonDocument temp = new BsonDocument("x", innerDoc);
                    String jsonStr = temp.toJson();
                    // 6 means `{"x": `, 1 means `}`
                    result.put(field, temp.toJson().substring(6, jsonStr.length() - 1));
                } else {
                    BsonDocument innerDoc = bsonDoc.get(schemaField.name()).asDocument();
                    result.put(field, innerDoc.toJson());
                }
            } else {
                // Primitive types or Logical types
                // TODO: support multiple format of datetime logical types
                result.put(field, fieldValue.toString());
            }
        }
        LOGGER.info("Stringify Output {}", result);
        return result;
    }

    private static Field getSchemaField(List<String> path, Schema schema) {
        if (path.isEmpty()) {
            return null;
        } else if (path.size() == 1) {
            return schema.field(path.get(0));
        } else {
            return getSchemaField(path.subList(1, path.size()), schema.field(path.get(0)).schema());
        }
    }

    private static Object getFieldValue(List<String> path, Struct value) {
        if (path.isEmpty()) {
            return null;
        } else if (path.size() == 1) {
            return value.get(path.get(0));
        } else {
            return getFieldValue(path.subList(1, path.size()), value.getStruct(path.get(0)));
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> map) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, map);
        targetFields = config.getList(ConfigName.FIELD_NAME);
        LOGGER.info("Configured with target fields: {}", targetFields);
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Value<R extends ConnectRecord<R>> extends StringifyJson<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    updatedSchema,
                    updatedValue,
                    record.timestamp()
            );
        }
    }
}
