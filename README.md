# Kafka Connect SMT to convert values to JSON strings
This lib implements Kafka connect SMT (Single Message Transformation) to convert values of specified fields to JSON strings (stringify).

## Config
Use it in connector config file
```
"transforms": "stringifyjson",
"transforms.stringifyjson.type": "com.moneyforward.smt.StringifyJson$Value",
"transforms.stringifyjson.target_fields": "field1,field2",
```

## Install to Kafka Connect
After build, copy file `target/kafka-connect-smt-{VERSION}-jar-with-dependencies.jar` to Kafka Connect plugin directory.

```
COPY ./target/kafka-connect-smt-1.0.0-jar-with-dependencies.jar /usr/share/java/ffp-kafka-connect-smt/kafka-connect-smt-1.0.0-jar-with-dependencies.jar
```

## Build Release File
- Increase version in `pom.xml`
- Run `mvn clean package`

## References
We are inspired by the following projects:
- https://github.com/max-prosper/stringify-json-smt
- https://github.com/an0r0c/kafka-connect-transform-tojsonstring