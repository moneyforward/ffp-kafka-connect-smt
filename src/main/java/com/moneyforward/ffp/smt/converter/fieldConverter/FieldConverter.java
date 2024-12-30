package com.moneyforward.ffp.smt.converter.fieldConverter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FieldConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(FieldConverter.class);

    private final Schema schema;

    public FieldConverter(Schema schema) {
        this.schema = schema;
    }

    public Schema getSchema() { return schema; }

    public abstract BsonValue toBsonValue(Object data);

    public BsonValue toBsonValue(Object data, Schema fieldSchema) {
        if(!fieldSchema.isOptional()) {
            if(data == null)
                throw new DataException("error: schema not optional but data was null");

            LOGGER.trace("field not optional and data is '{}'", data);
            return toBsonValue(data);
        }

        if(data != null) {
            LOGGER.trace("field optional and data is '{}'", data);
            return toBsonValue(data);
        }

        if(fieldSchema.defaultValue() != null) {
            LOGGER.trace("field optional and no data but default value is '{}'",fieldSchema.defaultValue().toString());
            return toBsonValue(fieldSchema.defaultValue());
        }

        LOGGER.trace("field optional, no data and no default value thus '{}'", BsonNull.VALUE);
        return BsonNull.VALUE;
    }
}
