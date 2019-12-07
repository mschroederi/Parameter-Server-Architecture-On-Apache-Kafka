package de.hpi.datastreams.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import de.hpi.datastreams.serialization.JSONSerdeCompatible;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@NoArgsConstructor
public class KeyValue implements JSONSerdeCompatible {

    @JsonProperty("key")
    @Getter
    @Setter
    Integer key;

    @JsonProperty("value")
    @Getter
    @Setter
    Float value;

    public KeyValue(@JsonProperty("key") Integer start, @JsonProperty("value") Float end) {
        this.key = start;
        this.value = end;
    }

    @Override
    public String toString() {
        return String.format("KeyValue(%d, %f)", key, value);
    }

    public Integer key(){
        return this.key;
    }

    public Float value(){
        return this.value;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (obj instanceof KeyValue) {
            KeyValue msg = (KeyValue) obj;
            return this.key.equals(msg.key) && this.value.equals(msg.value);
        }
        return false;
    }
}
