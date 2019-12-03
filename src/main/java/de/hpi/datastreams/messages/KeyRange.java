package de.hpi.datastreams.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import de.hpi.datastreams.serialization.JSONSerdeCompatible;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@NoArgsConstructor
public class KeyRange implements JSONSerdeCompatible {

    @JsonProperty("start")
    @Getter
    @Setter
    Integer start;

    @JsonProperty("end")
    @Getter
    @Setter
    Integer end;

    public KeyRange(@JsonProperty("start") Integer start, @JsonProperty("end") Integer end) {
        this.start = start;
        this.end = end;
    }

    @Override
    public String toString() {
        return String.format("KeyRange(%d, %d)", start, end);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (obj instanceof KeyRange) {
            KeyRange msg = (KeyRange) obj;
            return this.start.equals(msg.start) && this.end.equals(msg.end);
        }
        return false;
    }
}
