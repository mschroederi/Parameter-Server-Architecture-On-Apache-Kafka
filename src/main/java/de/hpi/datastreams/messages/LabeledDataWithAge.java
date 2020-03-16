package de.hpi.datastreams.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import de.hpi.datastreams.serialization.JSONSerdeCompatible;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;


@NoArgsConstructor
public class LabeledDataWithAge implements JSONSerdeCompatible {

    @JsonProperty("inputFeatures")
    @Getter
    @Setter
    Map<Integer, Float> inputFeatures;

    @JsonProperty("label")
    @Getter
    @Setter
    Integer label;

    @JsonProperty("insertionID")
    @Getter
    @Setter
    Long insertionID;

    @JsonCreator
    public LabeledDataWithAge(@JsonProperty("inputFeatures") Map<Integer, Float> data, @JsonProperty("label") Integer label, @JsonProperty("insertionID") Long insertionID) {
        this.inputFeatures = data;
        this.label = label;
        this.insertionID = insertionID;
    }

    @JsonIgnore
    public static LabeledDataWithAge from(Map<Integer, Float> data, Integer label, Long insertionID) {
        return new LabeledDataWithAge(data, label, insertionID);
    }

    @Override
    public String toString() {
        return String.format("DataEntry(%s, %d, %d)", inputFeatures.toString(), label, insertionID);
    }
}
