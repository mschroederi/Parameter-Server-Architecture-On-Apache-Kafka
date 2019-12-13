package de.hpi.datastreams.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import de.hpi.datastreams.serialization.JSONSerdeCompatible;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

public class GradientMessage extends BaseMessage implements JSONSerdeCompatible {

    @JsonProperty("partitionKey")
    @Getter
    @Setter
    Long partitionKey;

    @JsonCreator
    public GradientMessage(@JsonProperty("vectorClock") Integer vectorClock,
                           @JsonProperty("keyRange") KeyRange keyRange,
                           @JsonProperty("values") Map<Integer, Float> keyValues,
                           @JsonProperty("partitionKey") Long partitionKey) {

        super(vectorClock, keyRange, keyValues);
        this.partitionKey = partitionKey;
    }

    @Override
    public String toString() {
        return String.format("GradientMessage(%d, %s, %s, %d)",
                vectorClock, keyRange.toString(), values.toString(), Math.toIntExact(partitionKey));
    }
}
