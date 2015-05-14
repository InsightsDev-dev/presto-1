package com.mobileum.presto.proteum.datatype;

import com.facebook.presto.connector.proteum.UnsafeMemory;

public class RangeDataType extends IDataType<Tuple<Long, Long>>{
    public RangeDataType(){
        super();
    }
    @Override
    public Tuple<Long, Long> readData(UnsafeMemory memory) {
        // TODO Auto-generated method stub
        return new Tuple<Long, Long>(memory.getLong(), memory.getLong());
    }
}
