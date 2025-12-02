/*-
 * #%L
 * athena-vertica
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.vertica.query;

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.sql.dialect.VerticaSqlDialect;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;
import org.stringtemplate.v4.STGroupFile;

import java.sql.ResultSet;
import java.util.Arrays;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class VerticaExportQueryBuilderTest {

    private static final String TEST_QUERY_ID = "query-123";
    private static final String TEST_BUCKET = "s3://test-bucket";
    private static final String PREPARED_SQL = "SELECT * FROM prepared_table";
    private static final String TEMPLATE_NAME = "templateVerticaExportQuery";
    private static final String QPT_TEMPLATE_NAME = "templateVerticaExportQPTQuery";
    private static final String EXPECTED_SQL_TEMPLATE = "SELECT <colNames> FROM <table> TO <s3ExportBucket> QUERYID <queryID>";

    @Mock private ST template;
    @Mock private ResultSet resultSet;
    @Mock private Schema schema;

    private VerticaExportQueryBuilder builder;

    @Before
    public void setUp() {
        builder = new VerticaExportQueryBuilder(template);
        when(template.render()).thenReturn(EXPECTED_SQL_TEMPLATE);
    }

    @Test
    public void constructor_NullTemplate_ShouldThrowException() {
        assertThrows(NullPointerException.class, () -> new VerticaExportQueryBuilder(null));
    }

    @Test
    public void getTemplateName_ShouldReturnExpectedName() {
        assertEquals(TEMPLATE_NAME, VerticaExportQueryBuilder.getTemplateName());
    }

    @Test
    public void getQptTemplateName_ShouldReturnExpectedName() {
        assertEquals(QPT_TEMPLATE_NAME, VerticaExportQueryBuilder.getQptTemplateName());
    }

    @Test
    public void withPreparedStatementSQL_ValidSQL_ShouldSetCorrectly() {
        builder.withPreparedStatementSQL(PREPARED_SQL);
        assertEquals(PREPARED_SQL, builder.getPreparedStatementSQL());
    }

    @Test
    public void withColumns_NoTimestamp_ShouldSetCorrectColumnNames() throws Exception {
        mockResultSet(new String[]{"id", "name"}, new String[]{"integer", "varchar"});
        mockSchema(new String[]{"id", "name"});

        builder.withColumns(resultSet, schema);
        assertEquals("id,name", builder.getColNames());
    }

    @Test
    public void withColumns_WithTimestamp_ShouldTransformTimestampCorrectly() throws Exception {
        mockResultSet(new String[]{"id", "created_at"}, new String[]{"integer", "timestamp"});
        mockSchema(new String[]{"id", "created_at"});

        builder.withColumns(resultSet, schema);
        assertEquals("id,CAST(created_at AS VARCHAR) AS created_at", builder.getColNames());
    }

    @Test
    public void withS3ExportBucket_ValidBucket_ShouldSetCorrectly() {
        builder.withS3ExportBucket(TEST_BUCKET);
        assertEquals(TEST_BUCKET, builder.getS3ExportBucket());
    }

    @Test
    public void withQueryID_ValidQueryID_ShouldSetCorrectly() {
        builder.withQueryID(TEST_QUERY_ID);
        assertEquals(TEST_QUERY_ID, builder.getQueryID());
    }

    @Test
    public void buildSetAwsRegionSql_ValidRegion_ShouldReturnExpectedStatement() {
        assertEquals("ALTER SESSION SET AWSRegion='us-west-2'", builder.buildSetAwsRegionSql("us-west-2"));
    }

    @Test
    public void buildSetAwsRegionSql_NullOrEmpty_ShouldReturnDefaultRegion() {
        assertEquals("ALTER SESSION SET AWSRegion='us-east-1'", builder.buildSetAwsRegionSql(null));
        assertEquals("ALTER SESSION SET AWSRegion='us-east-1'", builder.buildSetAwsRegionSql(""));
    }

    @Test
    public void build_ValidSetup_ShouldReturnExpectedTemplate() {
        builder.withPreparedStatementSQL(PREPARED_SQL)
                .withS3ExportBucket(TEST_BUCKET)
                .withQueryID(TEST_QUERY_ID);

        String result = builder.build();
        assertEquals(EXPECTED_SQL_TEMPLATE, result);
    }

    @Test
    public void withPreparedStatementSQL_EmptySQL_ShouldSetEmpty() {
        builder.withPreparedStatementSQL("");
        assertEquals("Empty SQL should be set correctly", "", builder.getPreparedStatementSQL());
        assertNotNull("Prepared SQL should not be null", builder.getPreparedStatementSQL());
    }

    @Test
    public void withS3ExportBucket_EmptyBucket_ShouldSetEmpty() {
        builder.withS3ExportBucket("");
        assertEquals("Empty bucket should be set correctly", "", builder.getS3ExportBucket());
        assertNotNull("S3 export bucket should not be null", builder.getS3ExportBucket());
    }

    @Test
    public void withQueryID_EmptyQueryID_ShouldSetEmpty() {
        builder.withQueryID("");
        assertEquals("Empty query ID should be set correctly", "", builder.getQueryID());
        assertNotNull("Query ID should not be null", builder.getQueryID());
    }

    @Test(expected = NullPointerException.class)
    public void withColumns_NullResultSet_ShouldThrowException() throws Exception {
        mockSchema(new String[]{"id", "name"});
        builder.withColumns(null, schema);
    }

    @Test(expected = NullPointerException.class)
    public void withColumns_NullSchema_ShouldThrowException() throws Exception {
        mockResultSet(new String[]{"id", "name"}, new String[]{"integer", "varchar"});
        builder.withColumns(resultSet, null);
    }

    @Test
    public void fromTable_ValidSchemaAndTable_ShouldSetTable() {
        builder.fromTable("testSchema", "testTable");
        assertNotNull("Table should not be null", builder.getTable());
        assertTrue("Table should be a string", builder.getTable() instanceof String);
        String table = builder.getTable();
        assertFalse("Table should contain schema and table info", table.isEmpty());
    }

    @Test(expected = NullPointerException.class)
    public void fromTable_NullTable_ShouldThrowException() {
        builder.fromTable("testSchema", null);
    }

    @Test
    public void fromTable_EmptySchemaAndTable() {
        builder.fromTable("", "");
        assertNotNull("Table should not be null even with empty inputs", builder.getTable());
        assertTrue("Table should be a string", builder.getTable() instanceof String);
        String table = builder.getTable();
        assertNotNull("Table string should not be null", table);
    }

    @Test
    public void withQueryPlan_SimpleClause() {
        // SQL: SELECT employee_id FROM basic_write_nonexist WHERE employee_id = 'EMP001'
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSExoRCAEaDWVxdWFsOmFueV9hbnkahgQSgwQK8wM68AMKBRIDCgEYEtwDEtkDCgIKABKuAwqrAwoCCgASjAMKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgpkaWZmZXJlbmNlCgpiaWdfbnVtYmVyCgxzbWFsbF9udW1iZXIKC3RpbnlfbnVtYmVyCgpieXRlX3ZhbHVlCgVwcmljZQoLZmxvYXRfdmFsdWUSmQEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYoCAhgBCgRiAhABCgRiAhABCgRiAhABCgQqAhABCgQqAhABCgQqAhABCgfCAQQQEyABCgQqAhABCgfCAQQQEyABCgQqAhABCgQqAhABCgQ6AhABCgQaAhABCgQSAhABCgQSAhABCgRaAhABCgRaAhABGAI6FgoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaIhogGgQKAhABIgoaCBIGCgISACIAIgwaCgoIYgZFTVAwMDEaCBIGCgISACIAEgtFTVBMT1lFRV9JRA==");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");
        String expectedQuery =
                "SELECT \"employee_id\"\n" +
                "FROM \"public\".\"basic_write_nonexist\"\n" +
                "WHERE \"employee_id\" = 'EMP001'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SimpleClause_Updated_Substrait_Plan() {
        // SQL: SELECT employee_id FROM basic_write_nonexist WHERE employee_id = 'EMP001'
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("", "GqYHEqMHCuEFGt4FCgIKABLTBTrQBQoYEhYKFBQVFhcYGRobHB0eHyAhIiMkJSYnEsUDCsIDCgIKABLaAgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2UKDnBhcnRpdGlvbl9uYW1lEpgBCge6AQQIARgBCgQ6AhABCgRiAhABCgRiAhABCgRiAhABCgWCAQIQAQoFigICGAEKBGICEAEKCcIBBggEEBMgAQoJwgEGCAQQEyABCgQ6AhABCgQ6AhABCgQ6AhABCgnCAQYIAxATIAEKBDoCEAEKCcIBBggDEBMgAQoEOgIQAQoJwgEGCAMQEyABCgQ6AhABCgRiAhABGAI6XwoxT1JBQ0xFX0JBU0lDX0RCVEFCTEVfU0NIRU1BX0dBTU1BX0VVX1dFU1RfMV9JTlRFRwoqT1JBQ0xFX0JBU0lDX0RCVEFCTEVfR0FNTUFfRVVfV0VTVF8xX0lOVEVHGggSBgoCEgAiABoKEggKBBICCAEiABoKEggKBBICCAIiABoKEggKBBICCAMiABoKEggKBBICCAQiABoKEggKBBICCAUiABoKEggKBBICCAYiABoKEggKBBICCAciABoKEggKBBICCAgiABoKEggKBBICCAkiABoKEggKBBICCAoiABoKEggKBBICCAsiABoKEggKBBICCAwiABoKEggKBBICCA0iABoKEggKBBICCA4iABoKEggKBBICCA8iABoKEggKBBICCBAiABoKEggKBBICCBEiABoKEggKBBICCBIiABoKEggKBBICCBMiABgAIAoSC2VtcGxveWVlX2lkEglpc19hY3RpdmUSDWVtcGxveWVlX25hbWUSCWpvYl90aXRsZRIHYWRkcmVzcxIJam9pbl9kYXRlEgl0aW1lc3RhbXASCGR1cmF0aW9uEgZzYWxhcnkSBWJvbnVzEgVoYXNoMRIFaGFzaDISBGNvZGUSBWRlYml0EgVjb3VudBIGYW1vdW50EgdiYWxhbmNlEgRyYXRlEgpkaWZmZXJlbmNlEg5wYXJ0aXRpb25fbmFtZTILEEoqB2lzdGhtdXM=");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");
        String expectedQuery =
                "SELECT \"employee_id\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE \"employee_id\" = 'EMP001'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_NotClause() {
        // SQL: SELECT employee_name,bonus FROM basic_write_nonexist WHERE bonus != '5000'
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSFxoVCAEaEW5vdF9lcXVhbDphbnlfYW55GsADEr0DCqQDOqEDCgYSBAoCExQS/gIS+wIKAgoAEtACCs0CCgIKABKuAgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2USfQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEKgIQAQoEKgIQAQoEKgIQAQoHwgEEEBMgAQoEKgIQAQoHwgEEEBMgAQoEKgIQAQoHwgEEEBMgAQoEKgIQARgCOhYKFGJhc2ljX3dyaXRlX25vbmV4aXN0GiIaIBoECgIQASIMGgoSCAoEEgIICSIAIgoaCAoGYgQ1MDAwGgoSCAoEEgIIAiIAGgoSCAoEEgIICSIAEg1FTVBMT1lFRV9OQU1FEgVCT05VUw==");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");
        String expectedQuery =
                "SELECT \"employee_name\", \"bonus\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE \"bonus\" <> '5000'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectFewColumns() {
        // SQL: SELECT employee_id, employee_name, salary FROM basic_write_nonexist
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "GrIDEq8DCogDOoUDCgcSBQoDExQVEtcCCtQCCgIKABK1AgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2USgwEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBDoCEAEKBDoCEAEKBDoCEAEKCcIBBggCEAogAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoIEgYKAhIAIgAaChIICgQSAggCIgAaChIICgQSAggIIgASC0VNUExPWUVFX0lEEg1FTVBMT1lFRV9OQU1FEgZTQUxBUlk=");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");

        String expectedQuery =
                "SELECT \"employee_id\", \"employee_name\", \"salary\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_WithLimit() {
        // SQL: SELECT employee_id, employee_name FROM basic_write_nonexist LIMIT 10
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "GpsGEpgGCuYEGuMECgIKABLYBDrVBAoXEhUKExMUFRYXGBkaGxwdHh8gISIjJCUS1wIK1AIKAgoAErUCCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRKDAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOhYKFGJhc2ljX3dyaXRlX25vbmV4aXN0GggSBgoCEgAiABoKEggKBBICCAEiABoKEggKBBICCAIiABoKEggKBBICCAMiABoKEggKBBICCAQiABoKEggKBBICCAUiABoKEggKBBICCAYiABoKEggKBBICCAciABoKEggKBBICCAgiABoKEggKBBICCAkiABoKEggKBBICCAoiABoKEggKBBICCAsiABoKEggKBBICCAwiABoKEggKBBICCA0iABoKEggKBBICCA4iABoKEggKBBICCA8iABoKEggKBBICCBAiABoKEggKBBICCBEiABoKEggKBBICCBIiABgAIAUSC2VtcGxveWVlX2lkEglpc19hY3RpdmUSDWVtcGxveWVlX25hbWUSCWpvYl90aXRsZRIHYWRkcmVzcxIJam9pbl9kYXRlEgl0aW1lc3RhbXASCGR1cmF0aW9uEgZzYWxhcnkSBWJvbnVzEgVoYXNoMRIFaGFzaDISBGNvZGUSBWRlYml0EgVjb3VudBIGYW1vdW50EgdiYWxhbmNlEgRyYXRlEgpkaWZmZXJlbmNl");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");
        String expectedQuery =
                "SELECT \"employee_id\", \"is_active\", \"employee_name\", \"job_title\", \"address\", \"join_date\", \"timestamp\", \"duration\", \"salary\", \"bonus\", \"hash1\", \"hash2\", \"code\", \"debit\", \"count\", \"amount\", \"balance\", \"rate\", \"difference\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "LIMIT 5";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_WithOrderBy() {
        // SQL: SELECT employee_id, salary FROM basic_write_nonexist ORDER BY salary DESC - Done
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "GrADEq0DCpUDKpIDCgIKABL7Ajr4AgoGEgQKAhMUEtcCCtQCCgIKABK1AgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2USgwEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBDoCEAEKBDoCEAEKBDoCEAEKCcIBBggCEAogAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoIEgYKAhIAIgAaChIICgQSAggIIgAaDgoKEggKBBICCAEiABADEgtFTVBMT1lFRV9JRBIGU0FMQVJZ");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");

        String expectedQuery =
                "SELECT \"employee_id\", \"salary\"\n" +
                "FROM \"public\".\"basic_write_nonexist\"\n" +
                "ORDER BY \"salary\" DESC";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectWithStringFilter() {
        // SQL: SELECT employee_id FROM basic_write_nonexist WHERE employee_id = 'EMP001'
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSExoRCAEaDWVxdWFsOmFueV9hbnkaqQMSpgMKlgM6kwMKBRIDCgETEv8CEvwCCgIKABLRAgrOAgoCCgASrwIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEn4KBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYoCAhgBCgRiAhABCgRiAhABCgRiAhABCgQqAhABCgQqAhABCgQqAhABCgfCAQQQEyABCgQqAhABCgfCAQQQEyABCgQqAhABCgfCAQQQEyABCgQqAhABGAI6FgoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaIhogGgQKAhABIgoaCBIGCgISACIAIgwaCgoIYgZFTVAwMDEaCBIGCgISACIAEgtlbXBsb3llZV9pZA==");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");
        String expectedQuery =
                "SELECT \"employee_id\"\n" +
                "FROM \"public\".\"basic_write_nonexist\"\n" +
                "WHERE \"employee_id\" = 'EMP001'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectWithNumericFilter() {
        // SQL: SELECT employee_id FROM basic_write_nonexist WHERE hash1 > 1000
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSEBoOCAEaCmd0OmFueV9hbnkapQMSogMKkgM6jwMKBRIDCgETEvsCEvgCCgIKABLQAgrNAgoCCgASrgIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEn0KBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBCoCEAEKBCoCEAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBofGh0aBAoCEAEiDBoKEggKBBICCAoiACIHGgUKAyjoBxoIEgYKAhIAIgASC0VNUExPWUVFX0lE");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");
        String expectedQuery =
                "SELECT \"employee_id\"\n" +
                "FROM \"public\".\"basic_write_nonexist\"\n" +
                "WHERE \"hash1\" > 1000";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectWithLimitAndOffset() {
        // SQL: SELECT employee_id FROM basic_write_nonexist LIMIT 5 OFFSET 10
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Go8DEowDCvwCGvkCCgIKABLuAjrrAgoFEgMKARMS1wIK1AIKAgoAErUCCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoEcmF0ZQoKZGlmZmVyZW5jZRKDAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEOgIQAQoJwgEGCAIQCiABCgQ6AhABCgnCAQYIAhAKIAEKBDoCEAEKCcIBBggCEAogAQoEOgIQARgCOhYKFGJhc2ljX3dyaXRlX25vbmV4aXN0GggSBgoCEgAiABgKIAUSC0VNUExPWUVFX0lE");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");
        String expectedQuery =
                "SELECT \"employee_id\"\n" +
                "FROM \"public\".\"basic_write_nonexist\"\n" +
                "OFFSET 10\n" +
                "LIMIT 5";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectHavingTimestampzField() {
        // SQL : SELECT "timestamp" FROM "basic_write_nonexist"
        Schema schema = new SchemaBuilder()
                .addField(FieldBuilder.newBuilder("timestampz", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")).build())
                .build();
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "GnIScApjOmEKBRIDCgEDEkwKSgoCCgASLAoCaWQKCXRpbWVzdGFtcAoEbmFtZRIVCgQqAhABCgWKAgIYAQoEYgIQARgCOhYKFGJhc2ljX3dyaXRlX25vbmV4aXN0GgoSCAoEEgIIASIAEgl0aW1lc3RhbXA=");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");
        String expectedQuery =
                "SELECT CAST(\"timestamp\" AS VARCHAR) AS \"timestamp\"\n" +
                "FROM \"public\".\"basic_write_nonexist\"";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectWithDecimalFilter() {
        // SQL: SELECT employee_id FROM basic_write_nonexist WHERE debit = 500
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSExoRCAEaDWVxdWFsOmFueV9hbnkalwQSlAQKhAQ6gQQKBRIDCgEYEu0DEuoDCgIKABKuAwqrAwoCCgASjAMKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgpkaWZmZXJlbmNlCgpiaWdfbnVtYmVyCgxzbWFsbF9udW1iZXIKC3RpbnlfbnVtYmVyCgpieXRlX3ZhbHVlCgVwcmljZQoLZmxvYXRfdmFsdWUSmQEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYoCAhgBCgRiAhABCgRiAhABCgRiAhABCgQqAhABCgQqAhABCgQqAhABCgfCAQQQEyABCgQqAhABCgfCAQQQEyABCgQqAhABCgQqAhABCgQ6AhABCgQaAhABCgQSAhABCgQSAhABCgRaAhABCgRaAhABGAI6FgoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaMxoxGgQKAhABIgwaChIICgQSAggNIgAiGxoZChfCARQKEPQBAAAAAAAAAAAAAAAAAAAQExoIEgYKAhIAIgASC0VNUExPWUVFX0lE");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");
        String expectedQuery =
                "SELECT \"employee_id\"\n" +
                "FROM \"public\".\"basic_write_nonexist\"\n" +
                "WHERE \"debit\" = 500";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectAllWithAndClause() {
        // SQL: SELECT * FROM basic_write_nonexist WHERE employee_id = 'EMP001' AND salary > 5000
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIOGgwIARoIYW5kOmJvb2wSFRoTCAIQARoNZXF1YWw6YW55X2FueRISGhAIAhACGgpndDphbnlfYW55GtMIEtAICt0GOtoGChwSGgoYGBkaGxwdHh8gISIjJCUmJygpKissLS4vEpsEEpgECgIKABKuAwqrAwoCCgASjAMKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgpkaWZmZXJlbmNlCgpiaWdfbnVtYmVyCgxzbWFsbF9udW1iZXIKC3RpbnlfbnVtYmVyCgpieXRlX3ZhbHVlCgVwcmljZQoLZmxvYXRfdmFsdWUSmQEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYoCAhgBCgRiAhABCgRiAhABCgRiAhABCgQqAhABCgQqAhABCgQqAhABCgfCAQQQEyABCgQqAhABCgfCAQQQEyABCgQqAhABCgQqAhABCgQ6AhABCgQaAhABCgQSAhABCgQSAhABCgRaAhABCgRaAhABGAI6FgoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaYRpfGgQKAhABIiYaJBoiCAEaBAoCEAEiChoIEgYKAhIAIgAiDBoKCghiBkVNUDAwMSIvGi0aKwgCGgQKAhABIhgaFloUCgQqAhABEgoSCAoEEgIICCIAGAIiBxoFCgMoiCcaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAGgoSCAoEEgIIAyIAGgoSCAoEEgIIBCIAGgoSCAoEEgIIBSIAGgoSCAoEEgIIBiIAGgoSCAoEEgIIByIAGgoSCAoEEgIICCIAGgoSCAoEEgIICSIAGgoSCAoEEgIICiIAGgoSCAoEEgIICyIAGgoSCAoEEgIIDCIAGgoSCAoEEgIIDSIAGgoSCAoEEgIIDiIAGgoSCAoEEgIIDyIAGgoSCAoEEgIIECIAGgoSCAoEEgIIESIAGgoSCAoEEgIIEiIAGgoSCAoEEgIIEyIAGgoSCAoEEgIIFCIAGgoSCAoEEgIIFSIAGgoSCAoEEgIIFiIAGgoSCAoEEgIIFyIAEgtlbXBsb3llZV9pZBIJaXNfYWN0aXZlEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSB2FkZHJlc3MSCWpvaW5fZGF0ZRIJdGltZXN0YW1wEghkdXJhdGlvbhIGc2FsYXJ5EgVib251cxIFaGFzaDESBWhhc2gyEgRjb2RlEgVkZWJpdBIFY291bnQSBmFtb3VudBIHYmFsYW5jZRIKZGlmZmVyZW5jZRIKYmlnX251bWJlchIMc21hbGxfbnVtYmVyEgt0aW55X251bWJlchIKYnl0ZV92YWx1ZRIFcHJpY2USC2Zsb2F0X3ZhbHVl");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");
        String expectedQuery =
                "SELECT \"employee_id\", \"is_active\", \"employee_name\", \"job_title\", \"address\", \"join_date\", CAST(\"timestamp\" AS VARCHAR) AS \"timestamp\", \"duration\", \"salary\", \"bonus\", \"hash1\", \"hash2\", \"code\", \"debit\", \"count\", \"amount\", \"balance\", \"difference\", \"big_number\", \"small_number\", \"tiny_number\", \"byte_value\", \"price\", \"float_value\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE \"employee_id\" = 'EMP001'AND \"salary\" > '5000'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectAllWithOrClause() {
        // SQL: SELECT * FROM basic_write_nonexist WHERE employee_id = 'EMP001' OR employee_id = 'EMP002'
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBINGgsIARoHb3I6Ym9vbBIVGhMIAhABGg1lcXVhbDphbnlfYW55GsoIEscICtQGOtEGChwSGgoYGBkaGxwdHh8gISIjJCUmJygpKissLS4vEpIEEo8ECgIKABKuAwqrAwoCCgASjAMKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgpkaWZmZXJlbmNlCgpiaWdfbnVtYmVyCgxzbWFsbF9udW1iZXIKC3RpbnlfbnVtYmVyCgpieXRlX3ZhbHVlCgVwcmljZQoLZmxvYXRfdmFsdWUSmQEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYoCAhgBCgRiAhABCgRiAhABCgRiAhABCgQqAhABCgQqAhABCgQqAhABCgfCAQQQEyABCgQqAhABCgfCAQQQEyABCgQqAhABCgQqAhABCgQ6AhABCgQaAhABCgQSAhABCgQSAhABCgRaAhABCgRaAhABGAI6FgoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaWBpWGgQKAhABIiYaJBoiCAEaBAoCEAEiChoIEgYKAhIAIgAiDBoKCghiBkVNUDAwMSImGiQaIggBGgQKAhABIgoaCBIGCgISACIAIgwaCgoIYgZFTVAwMDIaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAGgoSCAoEEgIIAyIAGgoSCAoEEgIIBCIAGgoSCAoEEgIIBSIAGgoSCAoEEgIIBiIAGgoSCAoEEgIIByIAGgoSCAoEEgIICCIAGgoSCAoEEgIICSIAGgoSCAoEEgIICiIAGgoSCAoEEgIICyIAGgoSCAoEEgIIDCIAGgoSCAoEEgIIDSIAGgoSCAoEEgIIDiIAGgoSCAoEEgIIDyIAGgoSCAoEEgIIECIAGgoSCAoEEgIIESIAGgoSCAoEEgIIEiIAGgoSCAoEEgIIEyIAGgoSCAoEEgIIFCIAGgoSCAoEEgIIFSIAGgoSCAoEEgIIFiIAGgoSCAoEEgIIFyIAEgtlbXBsb3llZV9pZBIJaXNfYWN0aXZlEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSB2FkZHJlc3MSCWpvaW5fZGF0ZRIJdGltZXN0YW1wEghkdXJhdGlvbhIGc2FsYXJ5EgVib251cxIFaGFzaDESBWhhc2gyEgRjb2RlEgVkZWJpdBIFY291bnQSBmFtb3VudBIHYmFsYW5jZRIKZGlmZmVyZW5jZRIKYmlnX251bWJlchIMc21hbGxfbnVtYmVyEgt0aW55X251bWJlchIKYnl0ZV92YWx1ZRIFcHJpY2USC2Zsb2F0X3ZhbHVl");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");
        String expectedQuery =
                "SELECT \"employee_id\", \"is_active\", \"employee_name\", \"job_title\", \"address\", \"join_date\", CAST(\"timestamp\" AS VARCHAR) AS \"timestamp\", \"duration\", \"salary\", \"bonus\", \"hash1\", \"hash2\", \"code\", \"debit\", \"count\", \"amount\", \"balance\", \"difference\", \"big_number\", \"small_number\", \"tiny_number\", \"byte_value\", \"price\", \"float_value\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE \"employee_id\" IN ('EMP001', 'EMP002')";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectTimestampField() {
        // SQL: SELECT * FROM basic_write_nonexist WHERE employee_id = 'EMP001' OR employee_id = 'EMP002'
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "GsUBEsIBCp8BOpwBCgcSBQoDBAUGEm8KbQoCCgASTwoLZW1wbG95ZWVfaWQKDWVtcGxveWVlX25hbWUKCXRpbWVzdGFtcAoGc2FsYXJ5Eh4KBGICEAEKBGICEAEKBYoCAhgBCgfCAQQQEyABGAI6FgoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaCBIGCgISACIAGgoSCAoEEgIIAiIAGgoSCAoEEgIIAyIAEgtlbXBsb3llZV9pZBIJdGltZXN0YW1wEgZzYWxhcnk=");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");
        String expectedQuery =
                "SELECT \"employee_id\", CAST(\"timestamp\" AS VARCHAR) AS \"timestamp\", \"salary\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectAllWithComplexFilter() {
        // SQL: SELECT * FROM basic_write_nonexist WHERE (employee_id = 'EMP001' AND salary > 5000) OR (employee_id = 'EMP002' AND salary < 3000)
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBINGgsIARoHb3I6Ym9vbBIQGg4IARABGghhbmQ6Ym9vbBIVGhMIAhACGg1lcXVhbDphbnlfYW55EhIaEAgCEAMaCmd0OmFueV9hbnkSEhoQCAIQBBoKbHQ6YW55X2FueRrKCRLHCQrUBzrRBwocEhoKGBgZGhscHR4fICEiIyQlJicoKSorLC0uLxKSBRKPBQoCCgASrgMKqwMKAgoAEowDCgtlbXBsb3llZV9pZAoJaXNfYWN0aXZlCg1lbXBsb3llZV9uYW1lCglqb2JfdGl0bGUKB2FkZHJlc3MKCWpvaW5fZGF0ZQoJdGltZXN0YW1wCghkdXJhdGlvbgoGc2FsYXJ5CgVib251cwoFaGFzaDEKBWhhc2gyCgRjb2RlCgVkZWJpdAoFY291bnQKBmFtb3VudAoHYmFsYW5jZQoKZGlmZmVyZW5jZQoKYmlnX251bWJlcgoMc21hbGxfbnVtYmVyCgt0aW55X251bWJlcgoKYnl0ZV92YWx1ZQoFcHJpY2UKC2Zsb2F0X3ZhbHVlEpkBCgRiAhABCgRiAhABCgRiAhABCgRiAhABCgRiAhABCgRiAhABCgWKAgIYAQoEYgIQAQoEYgIQAQoEYgIQAQoEKgIQAQoEKgIQAQoEKgIQAQoHwgEEEBMgAQoEKgIQAQoHwgEEEBMgAQoEKgIQAQoEKgIQAQoEOgIQAQoEGgIQAQoEEgIQAQoEEgIQAQoEWgIQAQoEWgIQARgCOhYKFGJhc2ljX3dyaXRlX25vbmV4aXN0GtcBGtQBGgQKAhABImUaYxphCAEaBAoCEAEiJhokGiIIAhoECgIQASIKGggSBgoCEgAiACIMGgoKCGIGRU1QMDAxIi8aLRorCAMaBAoCEAEiGBoWWhQKBCoCEAESChIICgQSAggIIgAYAiIHGgUKAyiIJyJlGmMaYQgBGgQKAhABIiYaJBoiCAIaBAoCEAEiChoIEgYKAhIAIgAiDBoKCghiBkVNUDAwMiIvGi0aKwgEGgQKAhABIhgaFloUCgQqAhABEgoSCAoEEgIICCIAGAIiBxoFCgMouBcaCBIGCgISACIAGgoSCAoEEgIIASIAGgoSCAoEEgIIAiIAGgoSCAoEEgIIAyIAGgoSCAoEEgIIBCIAGgoSCAoEEgIIBSIAGgoSCAoEEgIIBiIAGgoSCAoEEgIIByIAGgoSCAoEEgIICCIAGgoSCAoEEgIICSIAGgoSCAoEEgIICiIAGgoSCAoEEgIICyIAGgoSCAoEEgIIDCIAGgoSCAoEEgIIDSIAGgoSCAoEEgIIDiIAGgoSCAoEEgIIDyIAGgoSCAoEEgIIECIAGgoSCAoEEgIIESIAGgoSCAoEEgIIEiIAGgoSCAoEEgIIEyIAGgoSCAoEEgIIFCIAGgoSCAoEEgIIFSIAGgoSCAoEEgIIFiIAGgoSCAoEEgIIFyIAEgtlbXBsb3llZV9pZBIJaXNfYWN0aXZlEg1lbXBsb3llZV9uYW1lEglqb2JfdGl0bGUSB2FkZHJlc3MSCWpvaW5fZGF0ZRIJdGltZXN0YW1wEghkdXJhdGlvbhIGc2FsYXJ5EgVib251cxIFaGFzaDESBWhhc2gyEgRjb2RlEgVkZWJpdBIFY291bnQSBmFtb3VudBIHYmFsYW5jZRIKZGlmZmVyZW5jZRIKYmlnX251bWJlchIMc21hbGxfbnVtYmVyEgt0aW55X251bWJlchIKYnl0ZV92YWx1ZRIFcHJpY2USC2Zsb2F0X3ZhbHVl");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");
        String expectedQuery =
                "SELECT \"employee_id\", \"is_active\", \"employee_name\", \"job_title\", \"address\", \"join_date\", CAST(\"timestamp\" AS VARCHAR) AS \"timestamp\", \"duration\", \"salary\", \"bonus\", \"hash1\", \"hash2\", \"code\", \"debit\", \"count\", \"amount\", \"balance\", \"difference\", \"big_number\", \"small_number\", \"tiny_number\", \"byte_value\", \"price\", \"float_value\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE \"employee_id\" = 'EMP001' AND \"salary\" > '5000'OR \"employee_id\" = 'EMP002' AND \"salary\" < '3000'";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_SelectAllWithCount() {
        // SQL: SELECT COUNT(*) OVER() AS TotalCount, * FROM "basic_write_nonexist"
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "CiUIARIhL2Z1bmN0aW9uc19hZ2dyZWdhdGVfZ2VuZXJpYy55YW1sEgwaCggBGgZjb3VudDoaiwgSiAgKiQY6hgYKHRIbChkYGRobHB0eHyAhIiMkJSYnKCkqKywtLi8wEq4DCqsDCgIKABKMAwoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKCmRpZmZlcmVuY2UKCmJpZ19udW1iZXIKDHNtYWxsX251bWJlcgoLdGlueV9udW1iZXIKCmJ5dGVfdmFsdWUKBXByaWNlCgtmbG9hdF92YWx1ZRKZAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoFigICGAEKBGICEAEKBGICEAEKBGICEAEKBCoCEAEKBCoCEAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKB8IBBBATIAEKBCoCEAEKBCoCEAEKBDoCEAEKBBoCEAEKBBICEAEKBBICEAEKBFoCEAEKBFoCEAEYAjoWChRiYXNpY193cml0ZV9ub25leGlzdBoWKhQiAiIAKgIiADADOgQ6AhACUAFgAhoIEgYKAhIAIgAaChIICgQSAggBIgAaChIICgQSAggCIgAaChIICgQSAggDIgAaChIICgQSAggEIgAaChIICgQSAggFIgAaChIICgQSAggGIgAaChIICgQSAggHIgAaChIICgQSAggIIgAaChIICgQSAggJIgAaChIICgQSAggKIgAaChIICgQSAggLIgAaChIICgQSAggMIgAaChIICgQSAggNIgAaChIICgQSAggOIgAaChIICgQSAggPIgAaChIICgQSAggQIgAaChIICgQSAggRIgAaChIICgQSAggSIgAaChIICgQSAggTIgAaChIICgQSAggUIgAaChIICgQSAggVIgAaChIICgQSAggWIgAaChIICgQSAggXIgASClRPVEFMQ09VTlQSC2VtcGxveWVlX2lkEglpc19hY3RpdmUSDWVtcGxveWVlX25hbWUSCWpvYl90aXRsZRIHYWRkcmVzcxIJam9pbl9kYXRlEgl0aW1lc3RhbXASCGR1cmF0aW9uEgZzYWxhcnkSBWJvbnVzEgVoYXNoMRIFaGFzaDISBGNvZGUSBWRlYml0EgVjb3VudBIGYW1vdW50EgdiYWxhbmNlEgpkaWZmZXJlbmNlEgpiaWdfbnVtYmVyEgxzbWFsbF9udW1iZXISC3RpbnlfbnVtYmVyEgpieXRlX3ZhbHVlEgVwcmljZRILZmxvYXRfdmFsdWU=");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");
        String expectedQuery =
                "SELECT COUNT(*) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS \"$f24\", \"employee_id\", \"is_active\", \"employee_name\", \"job_title\", \"address\", \"join_date\", CAST(\"timestamp\" AS VARCHAR) AS \"timestamp\", \"duration\", \"salary\", \"bonus\", \"hash1\", \"hash2\", \"code\", \"debit\", \"count\", \"amount\", \"balance\", \"difference\", \"big_number\", \"small_number\", \"tiny_number\", \"byte_value\", \"price\", \"float_value\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_ComplexExpression() {
        // SQL : SELECT COUNT(*) FROM basic_write_nonexist
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "CiUIARIhL2Z1bmN0aW9uc19hZ2dyZWdhdGVfZ2VuZXJpYy55YW1sEgwaCggBGgZjb3VudDoa+gIS9wIK7AIi6QIKAgoAEtICCs8CCgIKABKwAgoLZW1wbG95ZWVfaWQKCWlzX2FjdGl2ZQoNZW1wbG95ZWVfbmFtZQoJam9iX3RpdGxlCgdhZGRyZXNzCglqb2luX2RhdGUKCXRpbWVzdGFtcAoIZHVyYXRpb24KBnNhbGFyeQoFYm9udXMKBWhhc2gxCgVoYXNoMgoEY29kZQoFZGViaXQKBWNvdW50CgZhbW91bnQKB2JhbGFuY2UKBHJhdGUKCmRpZmZlcmVuY2USfwoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEKgIQAQoEKgIQAQoEKgIQAQoJwgEGCAIQCiABCgQqAhABCgfCAQQQEyABCgQqAhABCgfCAQQQEyABCgQqAhABGAI6FgoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaACIMCgogAyoEOgIQAjABEgZFWFBSJDA=");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");
        String expectedQuery =
                "SELECT COUNT(*) AS \"$f0\"\n" +
                "FROM \"public\".\"basic_write_nonexist\"";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_DoubleFilter() {
        // SQL : SELECT price FROM basic_write_nonexist WHERE price >= 10000
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSERoPCAEaC2d0ZTphbnlfYW55Gp0BEpoBCpABOo0BCgUSAwoBARJ6EngKAgoAEjEKLwoCCgASEQoFcHJpY2USCAoEWgIQARgCOhYKFGJhc2ljX3dyaXRlX25vbmV4aXN0Gj8aPRoECgIQASIKGggSBgoCEgAiACIpGidaJQoEWgIQAhIbChnCARYKEA8nAAAAAAAAAAAAAAAAAAAQBBgCGAIaCBIGCgISACIAEgVQUklDRQ==");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");
        String expectedQuery =
                "SELECT \"price\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE \"price\" >= 99.99";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_FloatFilter() {
        // SQL : SELECT * FROM basic_write_nonexist WHERE float_value < 500
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSEBoOCAEaCmx0OmFueV9hbnkakQESjgEKfzp9CgUSAwoBARJqEmgKAgoAEjcKNQoCCgASFwoLZmxvYXRfdmFsdWUSCAoEUgIQARgCOhYKFGJhc2ljX3dyaXRlX25vbmV4aXN0GikaJxoECgIQASIKGggSBgoCEgAiACITGhFaDwoEUgIQAhIFCgMo9AMYAhoIEgYKAhIAIgASC2Zsb2F0X3ZhbHVl");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");
        String expectedQuery =
                "SELECT \"float_value\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE \"float_value\" < 500.0";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_DecimalFilter() {
        // SQL : SELECT debit FROM basic_write_nonexist WHERE debit > 10000
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSEBoOCAEaCmd0OmFueV9hbnkakgESjwEKhQE6ggEKBRIDCgEBEm8SbQoCCgASNAoyCgIKABIUCgVkZWJpdBILCgfCAQQQEyABGAI6FgoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaMRovGgQKAhABIgoaCBIGCgISACIAIhsaGQoXwgEUChAQJwAAAAAAAAAAAAAAAAAAEBMaCBIGCgISACIAEgVkZWJpdA==");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");
        String expectedQuery =
                "SELECT \"debit\"\n" +
                        "FROM \"public\".\"basic_write_nonexist\"\n" +
                        "WHERE \"debit\" > 10000";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_DateDayFilter() {
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSExoRCAEaDWVxdWFsOmFueV9hbnkakQESjgEKhQE6ggEKBRIDCgEBEm8SbQoCCgASMQovCgIKABIRCgRkYXRlEgkKBYIBAhABGAI6FgoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaNBoyGgQKAhABIgoaCBIGCgISACIAIh4aHFoaCgWCAQIQAhIPCg2qAQoyMDIzLTAyLTAxGAIaCBIGCgISACIAEgRkYXRl");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");
        String expectedQuery = "SELECT \"date\"\n" +
                "FROM \"public\".\"basic_write_nonexist\"\n" +
                "WHERE \"date\" = 19389";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }

    @Test
    public void withQueryPlan_DateMillisecondFilter() {
        VerticaExportQueryBuilder builder = new VerticaExportQueryBuilder(createValidTemplate());
        QueryPlan queryPlan = new QueryPlan("1.0", "Ch4IARIaL2Z1bmN0aW9uc19jb21wYXJpc29uLnlhbWwSExoRCAEaDWVxdWFsOmFueV9hbnka0gYSzwYKnQU6mgUKFxIVChMTFBUWFxgZGhscHR4fICEiIyQlEpwDEpkDCgIKABLRAgrOAgoCCgASrwIKC2VtcGxveWVlX2lkCglpc19hY3RpdmUKDWVtcGxveWVlX25hbWUKCWpvYl90aXRsZQoHYWRkcmVzcwoJam9pbl9kYXRlCgl0aW1lc3RhbXAKCGR1cmF0aW9uCgZzYWxhcnkKBWJvbnVzCgVoYXNoMQoFaGFzaDIKBGNvZGUKBWRlYml0CgVjb3VudAoGYW1vdW50CgdiYWxhbmNlCgRyYXRlCgpkaWZmZXJlbmNlEn4KBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBGICEAEKBYoCAhgBCgRiAhABCgRiAhABCgRiAhABCgQqAhABCgQqAhABCgQqAhABCgfCAQQQEyABCgQqAhABCgfCAQQQEyABCgQqAhABCgfCAQQQEyABCgQqAhABGAI6FgoUYmFzaWNfd3JpdGVfbm9uZXhpc3QaPxo9GgQKAhABIgwaChIICgQSAggGIgAiJxolWiMKBYoCAhgCEhgKFqoBEzIwMjMtMDEtMDFUMDA6MDA6MDAYAhoIEgYKAhIAIgAaChIICgQSAggBIgAaChIICgQSAggCIgAaChIICgQSAggDIgAaChIICgQSAggEIgAaChIICgQSAggFIgAaChIICgQSAggGIgAaChIICgQSAggHIgAaChIICgQSAggIIgAaChIICgQSAggJIgAaChIICgQSAggKIgAaChIICgQSAggLIgAaChIICgQSAggMIgAaChIICgQSAggNIgAaChIICgQSAggOIgAaChIICgQSAggPIgAaChIICgQSAggQIgAaChIICgQSAggRIgAaChIICgQSAggSIgASC2VtcGxveWVlX2lkEglpc19hY3RpdmUSDWVtcGxveWVlX25hbWUSCWpvYl90aXRsZRIHYWRkcmVzcxIJam9pbl9kYXRlEgl0aW1lc3RhbXASCGR1cmF0aW9uEgZzYWxhcnkSBWJvbnVzEgVoYXNoMRIFaGFzaDISBGNvZGUSBWRlYml0EgVjb3VudBIGYW1vdW50EgdiYWxhbmNlEgRyYXRlEgpkaWZmZXJlbmNl");
        VerticaExportQueryBuilder queryBuilder = builder.withQueryPlan(queryPlan, VerticaSqlDialect.DEFAULT, "public", "basic_write_nonexist");
        String expectedQuery = "SELECT \"employee_id\", \"is_active\", \"employee_name\", \"job_title\", \"address\", \"join_date\", CAST(\"timestamp\" AS VARCHAR) AS \"timestamp\", \"duration\", \"salary\", \"bonus\", \"hash1\", \"hash2\", \"code\", \"debit\", \"count\", \"amount\", \"balance\", \"rate\", \"difference\"\n" +
                "FROM \"public\".\"basic_write_nonexist\"\n" +
                "WHERE \"timestamp\" = 1672531200000";
        assertEquals(expectedQuery, queryBuilder.getQueryFromPlan());
    }


    /**
     * Utility method for mocking ResultSet behavior.
     */
    private void mockResultSet(String[] columnNames, String[] typeNames) throws Exception {
        when(resultSet.next()).thenReturn(true, true, false);
        when(resultSet.getString("COLUMN_NAME")).thenReturn(columnNames[0], columnNames[1]);
        when(resultSet.getString("TYPE_NAME")).thenReturn(typeNames[0], typeNames[1]);
    }

    /**
     * Utility method for mocking Schema behavior.
     */
    private void mockSchema(String[] fieldNames) {
        Field field1 = mock(Field.class);
        Field field2 = mock(Field.class);
        when(field1.getName()).thenReturn(fieldNames[0]);
        when(field2.getName()).thenReturn(fieldNames[1]);
        when(schema.getFields()).thenReturn(Arrays.asList(field1, field2));
    }

    private ST createValidTemplate() {
        STGroup stGroup = new STGroupFile("Vertica.stg");
        return stGroup.getInstanceOf("templateVerticaExportSubstraitQuery");
    }
}