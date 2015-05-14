package com.mobileum.presto.proteum.datatype;

import com.facebook.presto.connector.proteum.UnsafeMemory;
import com.fasterxml.jackson.annotation.JsonProperty;

public class StringDataType extends IDataType<String>{
    
    public StringDataType() {
        super();
    }
    
    
    @Override
    public String readData(UnsafeMemory memory) {
        return memory.getString();
    }

}
