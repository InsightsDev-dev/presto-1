package com.mobileum.presto.proteum.datatype;

import java.io.Serializable;

import com.facebook.presto.connector.proteum.UnsafeMemory;
import com.fasterxml.jackson.annotation.JsonProperty;

public class IDataType<T> implements Serializable{
    protected String connector;
    protected transient IDataType<T> delegate;
    public IDataType(@JsonProperty("connector") String connector) {
        this.connector = connector;
        this.delegate = getProteumTypeFromString(connector);
    }
    
    public IDataType() {
    }

    public T readData(UnsafeMemory memory){
        return delegate.readData(memory);
    };
    
    @JsonProperty
    public String getConnector(){
        return this.connector;
    }
    
    public static IDataType getProteumTypeFromString(String type){
        if (type.equalsIgnoreCase("int"))
            return new IntDataType();
        if(type.equalsIgnoreCase("long"))
            return new LongDataType();
        if (type.equalsIgnoreCase("double"))
            return new DoubleDataType();
        if(type.equalsIgnoreCase("tsrange"))
            return new RangeDataType();
        return new StringDataType();
    }
}
