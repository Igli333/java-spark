package com.spark.ch15;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class PointAttributionUdaf extends UserDefinedAggregateFunction {

    private static final int MAX_POINT_PER_ORDER = 3;

    @Override
    public StructType inputSchema() {
        List<StructField> inputFields = new ArrayList<>();
        inputFields.add(DataTypes.createStructField("_c0", DataTypes.IntegerType, true));
        return DataTypes.createStructType(inputFields);
    }

    @Override
    public StructType bufferSchema() {
        List<StructField> bufferFields = new ArrayList<>();
        bufferFields.add(DataTypes.createStructField("sum", DataTypes.IntegerType, true));
        return null;
    }

    @Override
    public DataType dataType() {
        return DataTypes.IntegerType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, 0);
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        int initialValue = buffer.getInt(0);
        int inputValue = input.getInt(0);
        int outputValue = 0;

        if (inputValue < MAX_POINT_PER_ORDER) {
            outputValue = inputValue;
        } else {
            outputValue = MAX_POINT_PER_ORDER;
        }

        outputValue += initialValue;
        buffer.update(0, outputValue);
    }

    @Override
    public void merge(MutableAggregationBuffer buffer, Row row) {
        buffer.update(0, buffer.getInt(0) + row.getInt(0));
    }

    @Override
    public Object evaluate(Row row) {
        return row.getInt(0);
    }
}
