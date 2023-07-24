package com.bbva.mpeh.mx.jsprk.pedt003.v00;

import com.bbva.lrba.builder.annotation.Builder;
import com.bbva.lrba.builder.spark.RegisterSparkBuilder;
import com.bbva.lrba.builder.spark.domain.SourcesList;
import com.bbva.lrba.builder.spark.domain.TargetsList;
import com.bbva.lrba.properties.LRBAProperties;
import com.bbva.lrba.spark.domain.datasource.Source;
import com.bbva.lrba.spark.domain.datatarget.Target;
import com.bbva.lrba.spark.domain.transform.TransformConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Builder
public class JobPedt003Builder extends RegisterSparkBuilder {

    Logger logger = LoggerFactory.getLogger(JobPedt003Builder.class);

    private LRBAProperties lrbaProperties;

    public JobPedt003Builder() {
        lrbaProperties = new LRBAProperties();
    }

    public void setLrbaProperties(LRBAProperties lrbaProperties) {
        this.lrbaProperties = lrbaProperties;
    }

    @Override
    public SourcesList registerSources() {
        String schemaDB2 = lrbaProperties.get("SCHEMA_DB2_MPEH");
        logger.info("Schema DB2 MPEH: {}", schemaDB2);

        String query = "SELECT NUMCLIEN, NUMDOMIC, PETDOMIC, FEDOCACT, TIPOPERT, DIREC1, ECALLE1, ECALLE2, DIREC2, " +
                "DIREC3, APTTO, POBLACI, ESTADO, CODPOST, CODPAIS, TIPTEL1, PREFIJ1, NUMTEL1, EXTTEL1, TIPTEL2, " +
                "PREFIJ2, NUMTEL2, EXTTEL2, USODOMI, USUALTA, FEALTDOM, NUMTER, USUARIO, PENOFMOD, PEHSTAMP, " +
                "TIPOVIAL, TIPOVIV, TIPOASEN, ENTFED, CENTREP, RUMREP, PEYSTAT, CTRL_EST, CTRL_FE, PEFECHA, " +
                "USUARIO1, PETRM FROM " + schemaDB2 + ".PEDT003";

        return SourcesList.builder()
                .add(Source.Jdbc.NativeQuery.builder()
                        .alias("PED3")
                        .serviceName("db2.MPEH.BATCH")
                        .sql(query)
                        .build())
                .build();
    }

    @Override
    public TransformConfig registerTransform() {
        return TransformConfig.TransformClass.builder().transform(new Transformer()).build();
    }

    @Override
    public TargetsList registerTargets() {
        return TargetsList.builder()
                .add(Target.File.Parquet.builder()
                        .alias("PED3_OUT")
                        .physicalName("pedt003_complete.parquet")
                        .serviceName("bts.MPEH.BATCH")
                        .build())
                .build();
    }

}