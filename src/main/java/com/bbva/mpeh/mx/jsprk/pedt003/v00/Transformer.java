package com.bbva.mpeh.mx.jsprk.pedt003.v00;

import com.bbva.lrba.spark.transformers.Transform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;

public class Transformer implements Transform {

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> datasetsFromRead) {
        Map<String, Dataset<Row>> datasetsToWrite = new HashMap<>();

        Dataset<Row> dataset = datasetsFromRead.get("PED3");
        datasetsToWrite.put("PED3_OUT", dataset);

        return datasetsToWrite;
    }

}