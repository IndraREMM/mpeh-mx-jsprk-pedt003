package com.bbva.mpeh.mx.jsprk.pedt003.v00;

import com.bbva.lrba.builder.spark.domain.SourcesList;
import com.bbva.lrba.builder.spark.domain.TargetsList;
import com.bbva.lrba.properties.LRBAProperties;
import com.bbva.lrba.spark.domain.datasource.Source;
import com.bbva.lrba.spark.domain.datatarget.Target;
import com.bbva.lrba.spark.domain.transform.TransformConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;

class JobPedt003BuilderTest {

    private JobPedt003Builder jobPedt003Builder;
    private LRBAProperties lrbaProperties;

    @BeforeEach
    void setUp() {
        this.jobPedt003Builder = new JobPedt003Builder();

        lrbaProperties = Mockito.mock(LRBAProperties.class);
        Mockito.when(lrbaProperties.get("SCHEMA_DB2_MPEH")).thenReturn("mock_url");
        jobPedt003Builder.setLrbaProperties(lrbaProperties);
    }

    @Test
    void registerSources_na_SourceList() {
        final SourcesList sourcesList = this.jobPedt003Builder.registerSources();
        assertNotNull(sourcesList);
        assertNotNull(sourcesList.getSources());
        assertEquals(1, sourcesList.getSources().size());

        final Source source = sourcesList.getSources().get(0);
        assertNotNull(source);
        assertEquals("PED3", source.getAlias());
    }

    @Test
    void registerTransform_na_Transform() {
        final TransformConfig transformConfig = this.jobPedt003Builder.registerTransform();
        assertNotNull(transformConfig);
        assertNotNull(transformConfig.getTransform());
    }

    @Test
    void registerTargets_na_TargetList() {
        final TargetsList targetsList = this.jobPedt003Builder.registerTargets();
        assertNotNull(targetsList);
        assertNotNull(targetsList.getTargets());
        assertEquals(1, targetsList.getTargets().size());

        final Target target = targetsList.getTargets().get(0);
        assertNotNull(target);
        assertEquals("PED3_OUT", target.getAlias());
    }

}