package de.hpi.datastreams.serialization;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import de.hpi.datastreams.messages.KeyRange;
import de.hpi.datastreams.messages.Message;

/**
 * An interface for registering types that can be de/serialized with {@link JSONSerde}.
 * https://github.com/apache/kafka/blob/2.2/streams/examples/src/main/java/org/apache/kafka/streams/examples/pageview/PageViewTypedDemo.java
 */
@SuppressWarnings("DefaultAnnotationParam") // being explicit for the example
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_t")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Message.class, name = "msg"),
        @JsonSubTypes.Type(value = KeyRange.class, name = "keyRange"),
})
public
interface JSONSerdeCompatible {

}
