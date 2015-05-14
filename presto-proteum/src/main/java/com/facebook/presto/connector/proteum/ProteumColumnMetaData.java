package com.facebook.presto.connector.proteum;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.mobileum.presto.proteum.datatype.IDataType;

public class ProteumColumnMetaData extends ColumnMetadata{
    private IDataType proteumType;
    public ProteumColumnMetaData(String name, Type type, IDataType proteumtype, int ordinalPosition,
            boolean partitionKey) {
        super(name, type, ordinalPosition, partitionKey);
        this.proteumType = proteumtype;
    }
    public IDataType getProteumDataType(){
        return proteumType;
    }
}
