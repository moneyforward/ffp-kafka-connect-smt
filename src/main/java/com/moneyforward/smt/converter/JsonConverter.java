package com.moneyforward.smt.converter;

import com.moneyforward.smt.converter.fieldConverter.*;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class JsonConverter implements Converter {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonConverter.class);

    private final Map<Schema.Type, FieldConverter> converters = new HashMap<>();
    private final Map<String, FieldConverter> logicalConverters = new HashMap<>();

    private static final String AVRO_UNION_TYPE_NAME = "io.confluent.connect.avro.Union";

    public static final Set<String> LOGICAL_TYPE_NAMES = new HashSet<>(
            Arrays.asList(Date.LOGICAL_NAME, Decimal.LOGICAL_NAME, Time.LOGICAL_NAME, Timestamp.LOGICAL_NAME)
    );

    public JsonConverter() {
        //standard types
        registerPrimitiveFieldConverter(new BooleanFieldConverter());
        registerPrimitiveFieldConverter(new Int8FieldConverter());
        registerPrimitiveFieldConverter(new Int16FieldConverter());
        registerPrimitiveFieldConverter(new Int32FieldConverter());
        registerPrimitiveFieldConverter(new Int64FieldConverter());
        registerPrimitiveFieldConverter(new Float32FieldConverter());
        registerPrimitiveFieldConverter(new Float64FieldConverter());
        registerPrimitiveFieldConverter(new StringFieldConverter());
        registerPrimitiveFieldConverter(new BytesFieldConverter());

        //logical types
        registerLogicalFieldConverter(new DateFieldConverter());
        registerLogicalFieldConverter(new TimeFieldConverter());
        registerLogicalFieldConverter(new TimestampFieldConverter());
        registerLogicalFieldConverter(new DecimalFieldConverter());
    }

    private void registerPrimitiveFieldConverter(FieldConverter converter) {
        converters.put(converter.getSchema().type(), converter);
    }

    private void registerLogicalFieldConverter(FieldConverter converter) {
        logicalConverters.put(converter.getSchema().name(), converter);
    }

    @Override
    public BsonDocument convert(Field field, Object value) {
        if (value == null) {
            throw new DataException("error: value was null for JSON conversion");
        }

        BsonDocument bsonDocument = new BsonDocument();
        processField(bsonDocument, value, field);
        return bsonDocument;
    }

    private void processField(BsonDocument doc, Object value, Field field) {
        LOGGER.info("processField {} value {}", field.name(), value);
        if (value == null) {
            LOGGER.info("processField: no value for field -> adding null");
            doc.put(field.name(), BsonNull.VALUE);
            return;
        }

        try {
            if (isSupportedLogicalType(field.schema())) {
                LOGGER.info("handling primitive type '{}' name='{}'", field.schema().type(), field.name());
                doc.put(field.name(), getFieldConverter(field.schema()).toBsonValue(value, field.schema()));
                return;
            }

            switch (field.schema().type()) {
                case BOOLEAN:
                case FLOAT32:
                case FLOAT64:
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case STRING:
                case BYTES:
                    LOGGER.info("handling primitive type '{}' name='{}'", field.schema().type(), field.name());
                    doc.put(field.name(), getFieldConverter(field.schema()).toBsonValue(value, field.schema()));
                    break;
                case STRUCT:
                    handleStructField(doc, (Struct) value, field);
                    break;
                case ARRAY:
                    doc.put(field.name(), handleArrayField((List) value, field));
                    break;
                case MAP:
                    handleMapField(doc, (Map) value, field);
                    break;
                default:
                    LOGGER.error("Invalid schema. unexpected / unsupported schema type '{}' for field '{}' value='{}'",
                            field.schema().type(), field.name(), value);
                    throw new DataException("unexpected / unsupported schema type " + field.schema().type());
            }
        } catch (Exception exc) {
            LOGGER.error("Error while processing field. schema type '{}' for field '{}' value='{}'",
                    field.schema().type(), field.name(), value);
            throw new DataException("error while processing field " + field.name(), exc);
        }
    }

    private void handleMapField(BsonDocument doc, Map m, Field field) {
        LOGGER.info("handleMapField: handling complex type 'map' for field {}, map {}", field, m);
        if (m == null) {
            LOGGER.info("handleMapField: no field in map -> adding null");
            doc.put(field.name(), BsonNull.VALUE);
            return;
        }
        BsonDocument bd = new BsonDocument();
        for (Object entry : m.keySet()) {
            String key = (String) entry;
            Struct val = (Struct) m.get(key);
            Schema.Type valueSchemaType = field.schema().valueSchema().type();
            LOGGER.info("handleMapField: processing map entry key='{}' valueSchemaType='{}'", key, valueSchemaType);
            if (val == null) {
                LOGGER.info("no value for key -> adding null");
                bd.put(key, BsonNull.VALUE);
            } else if (valueSchemaType.isPrimitive()) {
                bd.put(key, getFieldConverter(field.schema().valueSchema()).toBsonValue(val, field.schema()));
            } else if (valueSchemaType.equals(Schema.Type.ARRAY)) {
                final Field elementField = new Field(key, 0, field.schema().valueSchema());
                final List list = (List) val;
                LOGGER.info("handleMapField: adding array values to {} of type valueSchema={} value='{}'",
                        elementField.name(), elementField.schema().valueSchema(), list);
                bd.put(key, handleArrayField(list, elementField));
            } else { // STRUCT
                LOGGER.info("handleMapField: handling field schema {}", valueSchemaType);
                bd.put(key, toBsonDoc(field.schema().valueSchema(), val));
            }
        }
        doc.put(field.name(), bd);
    }

    private BsonValue handleArrayField(List list, Field field) {
        LOGGER.info("handling complex type 'array' of types '{}'",
                field.schema().valueSchema().type());
        if (list == null) {
            LOGGER.info("no array -> adding null");
            return BsonNull.VALUE;
        }
        BsonArray array = new BsonArray();
        Schema.Type st = field.schema().valueSchema().type();
        for (Object element : list) {
            if (st.isPrimitive()) {
                array.add(getFieldConverter(field.schema().valueSchema()).toBsonValue(element, field.schema()));
            } else if (st == Schema.Type.ARRAY) {
                Field elementField = new Field("first", 0, field.schema().valueSchema());
                array.add(handleArrayField((List) element, elementField));
            } else {
                array.add(toBsonDoc(field.schema().valueSchema(), (Struct) element));
            }
        }
        return array;
    }

    private void handleStructField(BsonDocument doc, Struct struct, Field field) {
        LOGGER.info("handling complex type 'struct' {}", struct);
        if (struct != null) {
            LOGGER.info(struct.toString());
            doc.put(field.name(), toBsonDoc(field.schema(), struct));
        } else {
            LOGGER.info("no field in struct -> adding null");
            doc.put(field.name(), BsonNull.VALUE);
        }
    }

    private BsonValue toBsonDoc(Schema schema, Struct value) {
        LOGGER.info("toBsonDoc schema={} value={} / schema.type {}", schema, value, schema.name());

        BsonDocument doc = new BsonDocument();
        if (AVRO_UNION_TYPE_NAME.equals(schema.name())) {
            for (Field f : schema.fields()) {
                if (value.get(f) != null) {
                    LOGGER.info("toBsonDoc: union processing field {} has value {}", f, value.get(f));
                    processField(doc, value.get(f), f);
                    return doc.get(f.name());
                }
            }
            return BsonNull.VALUE;
        } else {
            schema.fields().forEach(f -> processField(doc, value.get(f), f));
        }
        return doc;
    }

    private FieldConverter getFieldConverter(Schema schema) {
        FieldConverter converter;

        if (isSupportedLogicalType(schema)) {
            converter = logicalConverters.get(schema.name());
        } else {
            converter = converters.get(schema.type());
        }

        if (converter == null) {
            throw new ConnectException("error no registered converter found for " + schema.type().getName());
        }

        return converter;
    }

    private boolean isSupportedLogicalType(Schema schema) {
        if (schema.name() == null) {
            return false;
        }

        return LOGICAL_TYPE_NAMES.contains(schema.name());

    }
}
