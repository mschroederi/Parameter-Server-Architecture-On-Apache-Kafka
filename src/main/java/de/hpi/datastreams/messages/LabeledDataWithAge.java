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

    @JsonProperty("age")
    @Getter
    @Setter
    Integer age;

    @JsonCreator
    public LabeledDataWithAge(@JsonProperty("inputFeatures") Map<Integer, Float> data, @JsonProperty("label") Integer label, @JsonProperty("age") Integer age) {
        this.inputFeatures = data;
        this.label = label;
        this.age = age;
    }

    @JsonIgnore
    public void increaseAge() {
        this.age++;
    }

    @JsonIgnore
    public static LabeledDataWithAge from(Map<Integer, Float> data, Integer label) {
        return new LabeledDataWithAge(data, label, 0);
    }

    @Override
    public String toString() {
        return String.format("DataEntry(%s, %d, %d)", inputFeatures.toString(), label, age);
    }
}
