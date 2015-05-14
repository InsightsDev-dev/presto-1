package com.mobileum.presto.proteum.datatype;

import com.facebook.presto.connector.proteum.UnsafeMemory;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DoubleDataType extends IDataType<Double>{
    public DoubleDataType() {
        super();
    }
    
    @Override
    public Double readData(UnsafeMemory memory) {
        return memory.getDouble();
    }

}
