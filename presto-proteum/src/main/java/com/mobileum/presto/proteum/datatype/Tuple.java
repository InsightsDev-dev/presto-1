package com.mobileum.presto.proteum.datatype;

public class Tuple<E,T> {
    private E first;
    private T second;
    public Tuple(E first, T second) {
        super();
        this.first = first;
        this.second = second;
    }
    public E getFirst() {
        return first;
    }
    public void setFirst(E first) {
        this.first = first;
    }
    public T getSecond() {
        return second;
    }
    public void setSecond(T second) {
        this.second = second;
    }
}
