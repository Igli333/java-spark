package com.spark.ch14;

import org.apache.spark.sql.api.java.UDF8;

import java.io.Serializable;
import java.sql.Timestamp;

public class IsOpenUdf implements UDF8<String, String, String, String, String, String, String, Timestamp, Boolean>, Serializable {
    @Override
    public Boolean call(String hoursMon, String hoursTue, String hoursWed, String hoursThu, String hoursFri, String hoursSat, String hoursSun, Timestamp dateTime) throws Exception {
        return IsOpenService.isOpen(hoursMon, hoursTue, hoursWed, hoursThu, hoursFri, hoursSat, hoursSun, dateTime);
    }
}
