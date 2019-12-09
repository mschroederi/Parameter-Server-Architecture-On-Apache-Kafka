package de.hpi.datastreams.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class BaseMessage {

    @JsonProperty("vectorClock")
    @Getter
    @Setter
    Integer vectorClock;

    @JsonProperty("keyRange")
    @Getter
    @Setter
    KeyRange keyRange;

    @JsonProperty("values")
    @Getter
    @Setter
    Map<Integer, Float> values = new HashMap<>();

    @JsonCreator
    public BaseMessage(@JsonProperty("vectorClock") Integer vectorClock, @JsonProperty("keyRange") KeyRange keyRange, @JsonProperty("values") Map<Integer, Float> values) {
        this.vectorClock = vectorClock;
        this.keyRange = keyRange;
        this.values = values;
    }

    @JsonIgnore
    public Integer getKeyRangeStart() {
        return this.getKeyRange().getStart();
    }

    @JsonIgnore
    public Integer getKeyRangeEnd() {
        return this.getKeyRange().getEnd();
    }

    public Optional<Float> getValue(int key) {
        if (!this.values.containsKey(key)) {
            return Optional.empty();
        }

        return Optional.of(this.values.get(key));
    }

    @Override
    public String toString() {
        return String.format("Message(%d, %s, %s)", vectorClock, keyRange.toString(), values.toString());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (obj instanceof BaseMessage) {
            BaseMessage msg = (BaseMessage) obj;
            return this.vectorClock.equals(msg.vectorClock) && this.keyRange.equals(msg.keyRange) && this.values.equals(msg.values);
        }
        return false;
    }
}

