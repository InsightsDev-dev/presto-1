package com.mobileum.presto.proteum.datatype;

import com.facebook.presto.connector.proteum.UnsafeMemory;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LongDataType extends IDataType<Long>{
    public LongDataType() {
        super();
    }
    
    @Override
    public Long readData(UnsafeMemory memory) {
        // TODO Auto-generated method stub
        return memory.getLong();
    }

}
