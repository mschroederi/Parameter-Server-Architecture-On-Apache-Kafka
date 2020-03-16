package de.hpi.datastreams.messages;

import de.hpi.datastreams.serialization.JSONSerdeCompatible;

import java.util.HashMap;

public class SerializableHashMap extends HashMap<Integer, Float> implements JSONSerdeCompatible {
}
