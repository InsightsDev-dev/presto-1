package com.mobileum.presto.proteum.datatype;

import com.facebook.presto.connector.proteum.UnsafeMemory;
import com.fasterxml.jackson.annotation.JsonProperty;

public class IntDataType extends IDataType<Integer>{
    public IntDataType() {
        super();
    }
    
    @Override
    public Integer readData(UnsafeMemory memory) {
        return memory.getInt();
    }

}
