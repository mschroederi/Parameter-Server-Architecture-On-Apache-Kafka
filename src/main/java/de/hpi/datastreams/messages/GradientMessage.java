package de.hpi.datastreams.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import de.hpi.datastreams.serialization.JSONSerdeCompatible;

import java.util.Map;

public class GradientMessage extends BaseMessage implements JSONSerdeCompatible {

    @JsonCreator
    public GradientMessage(@JsonProperty("vectorClock") Integer vectorClock, @JsonProperty("keyRange") KeyRange keyRange, @JsonProperty("values") Map<Integer, Float> keyValues) {
        super(vectorClock, keyRange, keyValues);
    }

    @Override
    public String toString() {
        return String.format("GradientMessage(%d, %s, %s)", vectorClock, keyRange.toString(), values.toString());
    }
}
