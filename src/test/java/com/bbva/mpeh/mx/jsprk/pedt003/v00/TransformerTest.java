package com.bbva.mpeh.mx.jsprk.pedt003.v00;

import com.bbva.lrba.spark.test.LRBASparkTest;
import com.bbva.lrba.spark.wrapper.DatasetUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TransformerTest extends LRBASparkTest {

    private Transformer transformer;

    @BeforeEach
    void setUp() {
        this.transformer = new Transformer();
    }

    @Test
    void transform_Output() {
        StructType schema = DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField("NUMCLIEN", DataTypes.StringType, false),
                        DataTypes.createStructField("DIREC1", DataTypes.StringType, false),
                        DataTypes.createStructField("ECALLE1", DataTypes.StringType, false),
                        DataTypes.createStructField("ECALLE2", DataTypes.StringType, false),
                        DataTypes.createStructField("DIREC3", DataTypes.StringType, false),
                        DataTypes.createStructField("APPTO", DataTypes.StringType, false),
                        DataTypes.createStructField("DIREC2", DataTypes.StringType, false),
                        DataTypes.createStructField("POBLACI", DataTypes.StringType, false),
                        DataTypes.createStructField("ESTADO", DataTypes.StringType, false),
                        DataTypes.createStructField("CODPOST", DataTypes.StringType, false),
                        DataTypes.createStructField("CODPAIS", DataTypes.StringType, false),
                        DataTypes.createStructField("ADDRESS", DataTypes.StringType, false),
                        DataTypes.createStructField("PETDOMIC", DataTypes.StringType, false),
                        DataTypes.createStructField("FEALTDOM", DataTypes.DateType, false),
                        DataTypes.createStructField("PEHSTAMP", DataTypes.DateType, false)

                });
        Row firstRow = RowFactory.create("AAA00056", "TEST_DIREC1", "TEST_ECALLE1", "TEST_ECALLE1","TEST_DIREC3",
                "TEST_APPTO", "TEST_DIREC2", "TEST_POBLACI", "TEST_ESTADO", "TEST_CODPOST", "TEST_CODPAIS",
                "TEST_ADDRESS", "TEST_PETDOMIC", Date.valueOf("2022-05-18"), Date.valueOf("2022-05-18"));
        Row secondRow = RowFactory.create("AAA00071", "TEST_DIREC1", "TEST_ECALLE1", "TEST_ECALLE1","TEST_DIREC3",
                "TEST_APPTO", "TEST_DIREC2", "TEST_POBLACI", "TEST_ESTADO", "TEST_CODPOST", "TEST_CODPAIS",
                "TEST_ADDRESS", "TEST_PETDOMIC", Date.valueOf("2022-05-18"), Date.valueOf("2022-05-18"));
        Row thirdRow = RowFactory.create("AAA00033", "TEST_DIREC1", "TEST_ECALLE1", "TEST_ECALLE1","TEST_DIREC3",
                "TEST_APPTO", "TEST_DIREC2", "TEST_POBLACI", "TEST_ESTADO", "TEST_CODPOST", "TEST_CODPAIS",
                "TEST_ADDRESS", "TEST_PETDOMIC", Date.valueOf("2022-05-18"), Date.valueOf("2022-05-18"));

        final List<Row> listRows = Arrays.asList(firstRow, secondRow, thirdRow);

        DatasetUtils<Row> datasetUtils = new DatasetUtils<>();
        Dataset<Row> dataset = datasetUtils.createDataFrame(listRows, schema);

        final Map<String, Dataset<Row>> datasetMap = this.transformer.transform(new HashMap<>(Map.of("PED3", dataset)));

        assertNotNull(datasetMap);
        assertEquals(1, datasetMap.size());
    }

}