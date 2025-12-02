/*-
 * #%L
 * athena-vertica
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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

import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.substrait.SubstraitSqlUtils;
import com.amazonaws.athena.connectors.jdbc.manager.SubstraitTypeAndValue;
import com.amazonaws.athena.connectors.jdbc.visitor.SubstraitAccumulatorVisitor;
import com.google.common.base.Joiner;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class VerticaExportQueryBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(VerticaExportQueryBuilder.class);
    private static final String TEMPLATE_NAME = "templateVerticaExportQuery";
    private static final String QPT_TEMPLATE_NAME = "templateVerticaExportQPTQuery";
    private static final String SUBSTRAIT_TEMPLATE_NAME = "templateVerticaExportSubstraitQuery";
    private static final String TEMPLATE_FIELD = "builder";
    private static final String QUOTE_CHARS = "\"";
    private final ST query;
    private String s3ExportBucket;
    private String table;
    private String queryID;
    private String colNames;
    private String constraintValues;
    private String queryFromPlan;
    private String preparedStatementSQL;

    public VerticaExportQueryBuilder(ST template)
    {
        this.query = Validate.notNull(template, "The StringTemplate for " + TEMPLATE_NAME + " can not be null!");
    }

    static String getTemplateName()
    {
        return TEMPLATE_NAME;
    }

    static String getQptTemplateName()
    {
        return QPT_TEMPLATE_NAME;
    }

    static String getSubstraitTemplateName() {
        return SUBSTRAIT_TEMPLATE_NAME;
    }

    public String getTable(){return table;}

    public String getQueryFromPlan() {
        return queryFromPlan;
    }

    public VerticaExportQueryBuilder fromTable(String schemaName, String tableName)
    {
        this.table = PredicateBuilder.getFromClauseWithSplit(schemaName, tableName);
        return this;
    }

    public String getColNames() {return colNames;}

    public VerticaExportQueryBuilder withPreparedStatementSQL(String preparedStatementSQL)
    {
        this.preparedStatementSQL = preparedStatementSQL;
        return this;
    }

    public String getPreparedStatementSQL()
    {
        return preparedStatementSQL;
    }

    // get the column names from user issued query in Athena
    public VerticaExportQueryBuilder withColumns(ResultSet definition, Schema tableSchema) throws SQLException {
        //get column name and type from the Schema in a hashmap for future use
        HashMap<String, String> mapOfNamesAndTypes = new HashMap<>();

        while(definition.next())
        {
            String colName = definition.getString("COLUMN_NAME").toLowerCase();
            String colType = definition.getString("TYPE_NAME").toLowerCase();
            mapOfNamesAndTypes.put(colName, colType);
        }

        // get the column names from the table schema
        StringBuilder colN = new StringBuilder();
        List<Field> fields = tableSchema.getFields();
        for(Field f : fields)
        {
            /*
            Vertica exports timestamp/timestamptz field as a INT 96 (26 digit number). The solution implemented here adds a 'cast as varchar' statement
            to the timestamp column to export the field as a VARCHAR.
             */
            String col_type = mapOfNamesAndTypes.get(f.getName().toLowerCase());
            if(col_type.equals("timestamp") || col_type.equals("timestamptz"))
            {
                String castedField = castTimestamp(f.getName());
                colN.append(castedField).append(",");
            }
            else {
                colN.append(f.getName()).append(",");
            }
        }
        this.colNames = colN.deleteCharAt(colN.length() - 1).toString();
        return this;
    }

    public String getConstraintValues() {
        return constraintValues;}

    //get the constraints from user issued query in Athena
    public VerticaExportQueryBuilder withConstraints(Constraints constraints, Schema tableSchema)
    {

        StringBuilder stringBuilder = new StringBuilder();
        //Get the constraints
        HashMap<String, PredicateBuilder.TypeAndValue> accumulator = new HashMap<>();
        List<String> clauses =  PredicateBuilder.toConjuncts(tableSchema.getFields(), constraints, accumulator);

        // if clauses is not empty, add it to the templates
        if (!clauses.isEmpty())
        {
            stringBuilder.append("WHERE ")
                    .append(Joiner.on(" AND ").join(clauses));
        }

        ST sqlTemplate = new ST(stringBuilder.toString());

        for (Map.Entry<String, PredicateBuilder.TypeAndValue> entry : accumulator.entrySet())
        {
            PredicateBuilder.TypeAndValue typeAndValue = entry.getValue();
            Types.MinorType minorTypeForArrowType = Types.getMinorTypeForArrowType(typeAndValue.getType());
            String colName = entry.getKey();

            switch (minorTypeForArrowType)
            {
                case BIT:
                    int value = ((boolean) typeAndValue.getValue()) ? 1 : 0;
                    sqlTemplate.add(colName, value);
                    break;
                case TINYINT:
                    sqlTemplate.add(colName, Byte.parseByte(typeAndValue.getValue().toString()));
                    break;
                case SMALLINT:
                    sqlTemplate.add(colName,Short.parseShort(typeAndValue.getValue().toString()));
                    break;
                case INT:
                    sqlTemplate.add(colName, Integer.parseInt(typeAndValue.getValue().toString()));
                    break;
                case BIGINT:
                    sqlTemplate.add(colName,Long.parseLong(typeAndValue.getValue().toString()));
                    break;
                case FLOAT4:
                    sqlTemplate.add(colName,Float.parseFloat(typeAndValue.getValue().toString()));
                    break;
                case FLOAT8:
                    sqlTemplate.add(colName,Double.parseDouble(typeAndValue.getValue().toString()));
                    break;
                case DECIMAL:
                    sqlTemplate.add(colName, new BigDecimal(typeAndValue.getValue().toString()));
                    break;
                case DATEDAY:
                    int epochDays = (int) LocalDate.parse(typeAndValue.getValue().toString()).toEpochDay();
                    sqlTemplate.add(colName, epochDays);
                    break;
                case DATEMILLI:
                    sqlTemplate.add(colName, LocalDateTime.parse(
                            typeAndValue.getValue().toString()).atZone(BlockUtils.UTC_ZONE_ID)
                            .toInstant().toEpochMilli());
                    break;
                case VARCHAR:
                    String val = "'" + typeAndValue.getValue() + "'";
                    sqlTemplate.add(colName, val);
                    break;
                case VARBINARY:
                    sqlTemplate.add(colName, typeAndValue.toString().getBytes());
                    break;

                default:
                    throw new UnsupportedOperationException(String.format("Can't handle type: %s, %s", typeAndValue.getType(), minorTypeForArrowType));
            }
        }
        this.constraintValues =  sqlTemplate.render();
        return this;

    }

    protected String castTimestamp(String name)
    {
        ST castFieldST = new ST("CAST(<name> AS VARCHAR) AS <name>");
        castFieldST.add("name", name);
        return castFieldST.render();
    }

    //build the Vertica SQL to set the AWS Region
    public String buildSetAwsRegionSql(String awsRegion)
    {
        if (awsRegion == null || awsRegion.equals("")) { 
            awsRegion = "us-east-1"; 
        }
        ST regionST=  new ST("ALTER SESSION SET AWSRegion='<defaultRegion>'") ;
        regionST.add("defaultRegion", awsRegion);
        return regionST.render();
    }

    public String getS3ExportBucket(){return s3ExportBucket;}

    public VerticaExportQueryBuilder withS3ExportBucket(String s3ExportBucket)
    {
        this.s3ExportBucket = s3ExportBucket;
        return this;
    }

    public String getQueryID(){return queryID;}

    public VerticaExportQueryBuilder withQueryID(String queryID)
    {
        this.queryID = queryID;
        return this;
    }

    public VerticaExportQueryBuilder withQueryPlan(final QueryPlan queryPlan, final SqlDialect sqlDialect,
                                                   final String schemaName, final String tableName) {
        String base64EncodedPlan = queryPlan.getSubstraitPlan();

        SqlNode sqlNode = SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(base64EncodedPlan, sqlDialect);
        Schema tableSchema = SubstraitSqlUtils.getTableSchemaFromSubstraitPlan(base64EncodedPlan, sqlDialect);

        if (!(sqlNode instanceof SqlSelect)) {
            throw new RuntimeException("Unsupported Query Type. Only SELECT Query is supported.");
        }
        SqlSelect select = (SqlSelect) sqlNode;

        // Remove auto-generated columnName aliases and cast timestamp/timestampz type field
        select = processSelectList(select, tableSchema);
        // Qualifies the table name by prefixing it with its schema name
        qualifyTableNames(select, schemaName, tableName);

        Map<String, String> aliasToOriginalMap = createAliasMapping(sqlNode);
        Schema adaptedSchema = createSchemaWithOriginalNames(tableSchema, aliasToOriginalMap);

        List<SubstraitTypeAndValue> accumulator = new ArrayList<>();
        SubstraitAccumulatorVisitor visitor = new SubstraitAccumulatorVisitor(accumulator, Collections.emptyMap(), adaptedSchema);
        select.accept(visitor);

        select = createParameterizedQuery(select);

        // Converts SQL Standard FETCH syntax to Vertica LIMIT syntax.
        String sqlString = convertFetchToVerticaLimit(select);

        /**
         * Uses custom delimiters in StringTemplate to avoid conflicts with SQL operators like `<` and `>`.
         * Fixes rendering issues by replacing default `< >` delimiters with `{ }`.
         */
        ST sqlTemplate = new ST(sqlString, '{', '}');
        handleDataTypesForSqlTemplate(accumulator, sqlTemplate);
        String renderedSql = sqlTemplate.render();
        this.queryFromPlan = renderedSql.replace("`", "\"");
        return this;
    }

    public String build()
    {
        Validate.notNull(s3ExportBucket, "s3ExportBucket can not be null.");
        Validate.notNull(table != null ? table : (preparedStatementSQL != null ? preparedStatementSQL : queryFromPlan), "table can not be null.");
        Validate.notNull(queryID, "queryID can not be null.");

        query.add(TEMPLATE_FIELD, this);
        return query.render().trim();
    }

    /**
     * Converts SQL Standard FETCH syntax to Vertica LIMIT syntax.
     *
     * Why programmatic approach:
     *  - Calcite's VerticaSqlDialect incorrectly generates FETCH NEXT syntax instead of LIMIT
     */
    private String convertFetchToVerticaLimit(final SqlSelect select) {
        String sql = select.toString();
        if (select.getOffset() != null) {
            // Convert "OFFSET n ROWS" to "OFFSET n"
            sql = sql.replaceAll("OFFSET (\\d+) ROWS", "OFFSET $1");
        }

        if (select.getFetch() != null) {
            // Convert "FETCH NEXT n ROWS ONLY" to "LIMIT n"
            sql = sql.replaceAll("FETCH NEXT (\\d+) ROWS ONLY", "LIMIT $1");
        }

        return sql;
    }

    /**
     * 1. Removes auto-generated aliases added when Substrait plans are converted to SQL, Calcite automatically adds sequential aliases
     * - SELECT name becomes SELECT name AS name0
     * - SELECT age becomes SELECT age AS age0
     * Example:
     *  - Input:  SELECT name AS name0, COUNT(*) AS count0 FROM table
     *  - Output: SELECT name, COUNT(*) AS count0 FROM table
     *
     * 2. Applies CAST(column AS VARCHAR) for timestamp fields
     * Vertica exports timestamp/timestamptz fields as INT 96 (26 digit number) which causes
     * data corruption. Casting to VARCHAR ensures proper timestamp export format.
     * Example:
     *  - Input:  SELECT created_date AS created_date0 FROM table
     *  - Output: SELECT CAST(created_date AS VARCHAR) AS created_date FROM table
     *
     * @param select The SQL SELECT statement from Substrait deserialization
     * @param tableSchema Schema containing field types for casting decisions
     * @return Modified SELECT with cleaned aliases and timestamp casting applied
     */
    private SqlSelect processSelectList(final SqlSelect select, final Schema tableSchema) {

        SqlNodeList selectList = expandSelectList(select, tableSchema);
        SqlNodeList newSelectList = new SqlNodeList(SqlParserPos.ZERO);

        for (int i = 0; i < selectList.size(); i++) {
            SqlNode selectItem = selectList.get(i);
            Field schemaField = tableSchema.getFields().get(i);
            if (selectItem instanceof SqlIdentifier) {
                // Case 1: Simple column reference without alias - SELECT name FROM users
                SqlNode processedItem = processColumnWithCasting(selectItem, schemaField);
                newSelectList.add(processedItem);
            } else if (selectItem instanceof SqlBasicCall && selectItem.getKind() == SqlKind.AS) {
                // Case 2 & 3: Aliased expressions - SELECT name AS name0 or SELECT COUNT(*) AS count0
                SqlBasicCall asCall = (SqlBasicCall) selectItem;
                SqlNode sourceExpr = asCall.operand(0);

                if (sourceExpr instanceof SqlIdentifier) {
                    // Case 2: Simple column with auto-generated alias - SELECT name AS name0 → SELECT name
                    newSelectList.add(processColumnWithCasting(sourceExpr, schemaField));
                } else {
                    // Case 3: Complex expression with alias - SELECT COUNT(*) AS count0 (keep alias)
                    newSelectList.add(selectItem);
                }
            } else {
                // Case 4: Complex expressions without alias - SELECT COUNT(*), name + 1 (keep as is)
                newSelectList.add(selectItem);
            }
        }

        // Clone and replace select list
        SqlSelect newSelect = (SqlSelect) select.clone(select.getParserPosition());
        newSelect.setSelectList(newSelectList);
        return newSelect;
    }

    // Expands the SELECT list by replacing * with explicit column references.
    private SqlNodeList expandSelectList(final SqlSelect select, final Schema tableSchema) {
        SqlNodeList expandedList = new SqlNodeList(SqlParserPos.ZERO);
        for (SqlNode selectItem : select.getSelectList()) {
            if (selectItem instanceof SqlIdentifier && ((SqlIdentifier) selectItem).isStar()) {
                // Expand * to all columns
                for (Field field : tableSchema.getFields()) {
                    expandedList.add(new SqlIdentifier(field.getName(), SqlParserPos.ZERO));
                }
            } else {
                expandedList.add(selectItem);
            }
        }
    return expandedList;
    }

    // Process column and apply timestamp casting if needed
    private SqlNode processColumnWithCasting(SqlNode columnNode, Field schemaField) {
        if (columnNode instanceof SqlIdentifier && schemaField != null &&
                schemaField.getType() instanceof ArrowType.Timestamp) {

            String columnName = ((SqlIdentifier) columnNode).getSimple();
            String quotedColumn = QUOTE_CHARS + columnName + QUOTE_CHARS;

            // Parse only the CAST expression, not the alias
            String castExpression = "CAST(" + quotedColumn + " AS VARCHAR)";

            try {
                SqlNode castNode = SqlParser.create(castExpression).parseExpression();

                // Add alias separately using Calcite's AS operator
                return SqlStdOperatorTable.AS.createCall(
                        SqlParserPos.ZERO,
                        castNode,
                        new SqlIdentifier(columnName, SqlParserPos.ZERO)
                );
            } catch (Exception e) {
                return columnNode; // Fallback to original
            }
        }
        return columnNode;
    }

    /**
     * Qualifies table names with schema to resolve naming conflicts across multiple schemas.
     * This prevents ambiguity when different schemas contain tables with identical names.
     * Example transformation:
     * - Input:  SELECT * FROM employees WHERE id = 1
     * - Output: SELECT * FROM "hr"."employees" WHERE id = 1
     *
     * @param select     The SQL SELECT statement to modify
     * @param schemaName The schema name to prepend (e.g., "hr", "public")
     * @param tableName  The table name to qualify (e.g., "employees")
     */
    private void qualifyTableNames(SqlSelect select, final String schemaName, final String tableName) {
        SqlNode from = select.getFrom();
        if (from instanceof SqlIdentifier) {
            SqlIdentifier table = (SqlIdentifier) from;
            if (table.isSimple() && table.getSimple().equals(tableName)) {
                // Create qualified identifier: schema.table
                SqlIdentifier qualified = new SqlIdentifier(
                        Arrays.asList(schemaName, tableName),
                        table.getParserPosition()
                );
                select.setFrom(qualified);
            }
        }
    }

    private void handleDataTypesForSqlTemplate(final List<SubstraitTypeAndValue> accumulator, ST sqlTemplate) {
        for (int i = 0; i < accumulator.size(); i++) {
            SubstraitTypeAndValue typeAndValue = accumulator.get(i);
            String paramName = "param" + i;
            SqlTypeName sqlTypeName = typeAndValue.getType();
            switch (sqlTypeName) {
                case BOOLEAN:
                    sqlTemplate.add(paramName, (boolean) typeAndValue.getValue() ? 1 : 0);
                    break;
                case TINYINT:
                    sqlTemplate.add(paramName, Byte.parseByte(typeAndValue.getValue().toString()));
                    break;
                case SMALLINT:
                    sqlTemplate.add(paramName, Short.parseShort(typeAndValue.getValue().toString()));
                    break;
                case INTEGER:
                    sqlTemplate.add(paramName, Integer.parseInt(typeAndValue.getValue().toString()));
                    break;
                case BIGINT:
                    sqlTemplate.add(paramName, Long.parseLong(typeAndValue.getValue().toString()));
                    break;
                case FLOAT:
                    sqlTemplate.add(paramName, Float.parseFloat(typeAndValue.getValue().toString()));
                    break;
                case DOUBLE:
                    sqlTemplate.add(paramName, Double.parseDouble(typeAndValue.getValue().toString()));
                    break;
                case DECIMAL:
                    sqlTemplate.add(paramName, new BigDecimal(typeAndValue.getValue().toString()));
                    break;
                case DATE:
                    int epochDays = (int) LocalDate.parse(typeAndValue.getValue().toString()).toEpochDay();
                    sqlTemplate.add(paramName, epochDays);
                    break;
                case TIMESTAMP:
                    long value;
                    try {
                        value = LocalDateTime.parse(typeAndValue.getValue().toString()).atZone(BlockUtils.UTC_ZONE_ID).toInstant().toEpochMilli();
                    } catch (DateTimeParseException e) {
                        throw new AthenaConnectorException(String.format("Can't handle timestamp format: %s, value class: %s", typeAndValue.getType(), typeAndValue.getValue().getClass().getName()),
                                ErrorDetails.builder()
                                        .errorCode(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION.toString())
                                        .build());
                    }
                    sqlTemplate.add(paramName, value);
                    break;
                case VARCHAR:
                    String val = "'" + typeAndValue.getValue() + "'";
                    sqlTemplate.add(paramName, val);
                    break;
                case VARBINARY:
                    sqlTemplate.add(paramName, typeAndValue.toString().getBytes());
                    break;
                default:
                    throw new AthenaConnectorException(String.format("Can't handle type: %s, %s", typeAndValue.getType(), typeAndValue.getType()),
                            ErrorDetails.builder().errorCode(FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION.toString()).build());
            }
        }
    }

    private Map<String, String> createAliasMapping(final SqlNode sqlNode) {
        Map<String, String> mapping = new HashMap<>();
        if (sqlNode instanceof SqlSelect) {
            SqlSelect select = (SqlSelect) sqlNode;
            SqlNodeList selectList = select.getSelectList();
            for (SqlNode node : selectList) {
                if (node instanceof SqlBasicCall && node.getKind() == SqlKind.AS) {
                    // Handle "column AS alias"
                    SqlBasicCall asCall = (SqlBasicCall) node;
                    String originalName = asCall.operand(0).toString().replace("`", "");
                    String aliasName = asCall.operand(1).toString().replace("`", "");
                    mapping.put(aliasName, originalName);
                }
            }
        }
        return mapping;
    }

    // CHANGE: Add helper method to create schema with original column names

    private Schema createSchemaWithOriginalNames(Schema substraitSchema, Map<String, String> aliasToOriginal) {
        List<Field> adaptedFields = new ArrayList<>();
        for (Field field : substraitSchema.getFields()) {
            String aliasName = field.getName();
            String originalName = aliasToOriginal.getOrDefault(aliasName, aliasName);
            // Create new field with original name but keep the same type and metadata
            Field adaptedField = new Field(originalName, field.getFieldType(), field.getChildren());
            adaptedFields.add(adaptedField);
        }
        return new Schema(adaptedFields);
    }

    private SqlSelect createParameterizedQuery(SqlSelect select) {
        AtomicInteger paramIndex = new AtomicInteger(0);

        if (select.getWhere() != null) {
            SqlShuttle parameterize = new SqlShuttle() {
                @Override
                public SqlNode visit(SqlLiteral literal) {
                    String paramName = "param" + paramIndex.getAndIncrement();
                    return new SqlDynamicParam(paramIndex.get() - 1, literal.getParserPosition()) {
                        @Override
                        public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
                            writer.print("{" + paramName + "}");
                        }
                    };
                }
            };
            return(SqlSelect)select.accept(parameterize);
            }
            return select;
        }
}