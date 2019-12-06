package de.hpi.datastreams.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import de.hpi.datastreams.serialization.JSONSerdeCompatible;
import jdk.nashorn.internal.runtime.options.Option;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@NoArgsConstructor
public class Message implements JSONSerdeCompatible {

    @JsonProperty("vectorClock")
    @Getter
    @Setter
    Integer vectorClock;

    @JsonProperty("keyRange")
    @Getter
    @Setter
    KeyRange keyRange;

    @JsonProperty("keyValue")
    @Getter
    @Setter
    Map<Integer, Float> keyValues;

    @JsonCreator
    public Message(@JsonProperty("vectorClock") Integer vectorClock, @JsonProperty("keyRange") KeyRange keyRange, @JsonProperty("keyValue") Map<Integer, Float> keyValues) {
        this.vectorClock = vectorClock;
        this.keyRange = keyRange;
        this.keyValues = keyValues;
    }

    public Optional<Float> getValue(int key) {
        if (!this.keyValues.containsKey(key)) {
            return Optional.empty();
        }

        return Optional.of(this.keyValues.get(key));
    }

    @Override
    public String toString() {
        return String.format("Message(%d, %s, %s)", vectorClock, keyRange.toString(), keyValues.toString());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (obj instanceof Message) {
            Message msg = (Message) obj;
            return this.vectorClock.equals(msg.vectorClock) && this.keyRange.equals(msg.keyRange) && this.keyValues.equals(msg.keyValues);
        }
        return false;
    }
}

