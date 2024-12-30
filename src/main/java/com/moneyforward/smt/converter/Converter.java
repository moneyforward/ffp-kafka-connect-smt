package com.moneyforward.smt.converter;

import org.apache.kafka.connect.data.Field;
import org.bson.BsonDocument;

public interface Converter {
    BsonDocument convert(Field field, Object value);
}
