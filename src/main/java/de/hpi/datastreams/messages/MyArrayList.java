package de.hpi.datastreams.messages;

import de.hpi.datastreams.serialization.JSONSerdeCompatible;

import java.util.ArrayList;

public class MyArrayList<T> extends ArrayList<T> implements JSONSerdeCompatible {
}
