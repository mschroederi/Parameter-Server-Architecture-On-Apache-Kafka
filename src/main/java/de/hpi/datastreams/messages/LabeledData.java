package de.hpi.datastreams.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import de.hpi.datastreams.serialization.JSONSerdeCompatible;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

public class LabeledData implements JSONSerdeCompatible {

    @JsonProperty("label")
    @Getter
    @Setter
    Integer label;

    @JsonProperty("inputData")
    @Getter
    @Setter
    Map<Integer, Float> inputData = new HashMap<>();

    @JsonCreator
    public LabeledData(@JsonProperty("inputData") Map<Integer, Float> inputData, @JsonProperty("label") Integer label) {
        this.inputData = inputData;
        this.label = label;
    }

    @Override
    public String toString() {
        return String.format("LabeledData(%s, %d)", inputData.toString(), label);
    }
}
