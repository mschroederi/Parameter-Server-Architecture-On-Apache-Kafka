package de.hpi.datastreams.serialization;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import de.hpi.datastreams.messages.*;

/**
 * An interface for registering types that can be de/serialized with {@link JSONSerde}.
 * https://github.com/apache/kafka/blob/2.2/streams/examples/src/main/java/org/apache/kafka/streams/examples/pageview/PageViewTypedDemo.java
 */
@SuppressWarnings("DefaultAnnotationParam")
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_t")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DataMessage.class, name = "dMsg"),
        @JsonSubTypes.Type(value = WeightsMessage.class, name = "wMsg"),
        @JsonSubTypes.Type(value = GradientMessage.class, name = "gMsg"),
        @JsonSubTypes.Type(value = KeyRange.class, name = "keyRange"),
})
public
interface JSONSerdeCompatible {

}
