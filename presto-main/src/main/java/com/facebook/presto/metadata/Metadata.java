package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;

import java.util.List;
import java.util.Map;

public interface Metadata
{
    FunctionInfo getFunction(QualifiedName name, List<TupleInfo.Type> parameterTypes);

    FunctionInfo getFunction(FunctionHandle handle);

    List<FunctionInfo> listFunctions();

    List<String> listSchemaNames(String catalogName);

    TableMetadata getTable(QualifiedTableName tableName);

    List<QualifiedTableName> listTables(QualifiedTablePrefix prefix);

    List<TableColumn> listTableColumns(QualifiedTablePrefix prefix);

    List<String> listTablePartitionKeys(QualifiedTableName tableName);

    List<Map<String, String>> listTablePartitionValues(QualifiedTablePrefix prefix);

    void createTable(TableMetadata table);

    QualifiedTableName getTableName(TableHandle tableHandle);

    TableColumn getTableColumn(TableHandle tableHandle, ColumnHandle columnHandle);
}
