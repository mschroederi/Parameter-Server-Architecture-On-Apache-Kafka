package de.hpi.datastreams.ml;

import lombok.Getter;
import org.apache.spark.mllib.linalg.Vector;

import java.io.Serializable;

public class SerializableLabeledPoint implements Serializable {
    @Getter
    private double label;
    @Getter
    private Vector features;

    public SerializableLabeledPoint(double label, Vector features) {
        this.label = label;
        this.features = features;
    }
}
