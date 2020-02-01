package de.hpi.datastreams.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import de.hpi.datastreams.serialization.JSONSerdeCompatible;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.*;
import java.util.function.Consumer;

@NoArgsConstructor
public class MyArrayList<T extends JSONSerdeCompatible> implements JSONSerdeCompatible {

    @JsonProperty("data")
    @Getter
    @Setter
    ArrayList<T> data = new ArrayList<>();

    @JsonCreator
    public MyArrayList(@JsonProperty("data") ArrayList<T> data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return String.format("MyArrayList(%s)", this.data);
    }

    @JsonIgnore
    public void forEach(Consumer<T> action) {
        this.data.forEach(action);
    }

    @JsonIgnore
    public int size() {
        return this.data.size();
    }

    @JsonIgnore
    public boolean isEmpty() {
        return this.data.isEmpty();
    }

    @JsonIgnore
    public boolean contains(Object o) {
        return this.data.contains(o);
    }

    @JsonIgnore
    public Iterator<T> iterator() {
        return this.data.iterator();
    }

    @JsonIgnore
    public Object[] toArray() {
        return this.data.toArray();
    }

    @JsonIgnore
    public <T1> T1[] toArray(T1[] t1s) {
        return this.data.toArray(t1s);
    }

    @JsonIgnore
    public boolean add(T t) {
        return this.data.add(t);
    }

    @JsonIgnore
    public boolean remove(Object o) {
        return this.data.remove(o);
    }

    @JsonIgnore
    public boolean containsAll(Collection<?> collection) {
        return this.containsAll(collection);
    }

    @JsonIgnore
    public boolean addAll(Collection<T> collection) {
        return this.data.addAll(collection);
    }

    @JsonIgnore
    public boolean addAll(int i, Collection<T> collection) {
        return this.data.addAll(i, collection);
    }

    @JsonIgnore
    public boolean removeAll(Collection<?> collection) {
        return this.data.removeAll(collection);
    }

    @JsonIgnore
    public boolean retainAll(Collection<?> collection) {
        return this.data.retainAll(collection);
    }

    @JsonIgnore
    public void clear() {
        this.data.clear();
    }

    @JsonIgnore
    public T get(int i) {
        return this.data.get(i);
    }

    @JsonIgnore
    public T set(int i, T t) {
        return this.data.set(i, t);
    }

    @JsonIgnore
    public void add(int i, T t) {
        this.data.add(i, t);
    }

    @JsonIgnore
    public T remove(int i) {
        return this.data.remove(i);
    }

    @JsonIgnore
    public int indexOf(Object o) {
        return this.data.indexOf(o);
    }

    @JsonIgnore
    public int lastIndexOf(Object o) {
        return this.data.lastIndexOf(o);
    }

    @JsonIgnore
    public ListIterator<T> listIterator() {
        return this.data.listIterator();
    }

    @JsonIgnore
    public ListIterator<T> listIterator(int i) {
        return this.data.listIterator(i);
    }

    @JsonIgnore
    public List<T> subList(int i, int i1) {
        return this.data.subList(i, i1);
    }
}
