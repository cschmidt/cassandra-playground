import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;


/**
 * Creates a column definition from the meta data describing a JDBC
 * result set.
 * @author cschmidt
 */
public class JDBCToCassandraMapper {
    
    private static StringSerializer stringSerializer = StringSerializer.get();
    private static Map<Integer, ComparatorType> jdbcToCassandraTypes;
    private static Logger log = Logger.getLogger(JDBCToCassandraMapper.class.toString());
    
    static {
        jdbcToCassandraTypes = new HashMap<Integer,ComparatorType>();
        // jdbcToCassandraTypes.put(Types.ARRAY, null);
        jdbcToCassandraTypes.put(Types.BIGINT, ComparatorType.DECIMALTYPE);
        jdbcToCassandraTypes.put(Types.BINARY, ComparatorType.BYTESTYPE);
        jdbcToCassandraTypes.put(Types.BIT, ComparatorType.BOOLEANTYPE);
        jdbcToCassandraTypes.put(Types.BLOB, ComparatorType.BYTESTYPE);
        jdbcToCassandraTypes.put(Types.BOOLEAN, ComparatorType.BOOLEANTYPE);
        jdbcToCassandraTypes.put(Types.CHAR, ComparatorType.UTF8TYPE);
        jdbcToCassandraTypes.put(Types.CLOB, ComparatorType.UTF8TYPE);
        jdbcToCassandraTypes.put(Types.DATALINK, ComparatorType.UTF8TYPE);
        jdbcToCassandraTypes.put(Types.DATE, ComparatorType.DATETYPE);
        jdbcToCassandraTypes.put(Types.DECIMAL, ComparatorType.DECIMALTYPE);
        // jdbcToCassandraTypes.put(Types.DISTINCT, null);
        jdbcToCassandraTypes.put(Types.DOUBLE, ComparatorType.FLOATTYPE);
        jdbcToCassandraTypes.put(Types.FLOAT, ComparatorType.FLOATTYPE);
        jdbcToCassandraTypes.put(Types.INTEGER, ComparatorType.INTEGERTYPE);
        // jdbcToCassandraTypes.put(Types.JAVA_OBJECT, null);
        // jdbcToCassandraTypes.put(Types.LONGNVARCHAR, null);
        // jdbcToCassandraTypes.put(Types.LONGVARBINARY, null);
        jdbcToCassandraTypes.put(Types.LONGVARCHAR, ComparatorType.UTF8TYPE);
        // jdbcToCassandraTypes.put(Types.NCHAR, null);
//        jdbcToCassandraTypes.put(Types.NCLOB, null);
//        jdbcToCassandraTypes.put(Types.NULL, null);
//        jdbcToCassandraTypes.put(Types.NUMERIC, null);
//        jdbcToCassandraTypes.put(Types.NVARCHAR, null);
//        jdbcToCassandraTypes.put(Types.OTHER, null);
//        jdbcToCassandraTypes.put(Types.REAL, null);
//        jdbcToCassandraTypes.put(Types.REF, null);
//        jdbcToCassandraTypes.put(Types.ROWID, null);
        jdbcToCassandraTypes.put(Types.SMALLINT, ComparatorType.INTEGERTYPE);
        jdbcToCassandraTypes.put(Types.SQLXML, null);
        jdbcToCassandraTypes.put(Types.STRUCT, null);
        jdbcToCassandraTypes.put(Types.TIME, null);
        jdbcToCassandraTypes.put(Types.TIMESTAMP, ComparatorType.DATETYPE);
        jdbcToCassandraTypes.put(Types.TINYINT, null);
        jdbcToCassandraTypes.put(Types.VARBINARY, null);
        jdbcToCassandraTypes.put(Types.VARCHAR, ComparatorType.UTF8TYPE);
    }
    
    /**
     * The target Cassandra cluster
     */
    private Cluster cluster;
    private Connection connection;
    private BasicColumnFamilyDefinition columnFamilyDefinition;
    
    /**
     * The target Cassandra keyspace
     */
    private Keyspace keyspace;
    
    private KeyspaceDefinition keyspaceDefinition;
    
    private ResultSet results;
    
    /**
     * The name of the table we're pulling into Cassandra from the JDBC source.
     * We'll use this name for the Cassandra keyspace as well.
     */
    private String tableName;
    
    
    
    public JDBCToCassandraMapper(Connection connection, 
                                 String tableName,
                                 Cluster cluster,
                                 Keyspace keyspace) {
        this.connection = connection;
        this.tableName = tableName;
        this.cluster = cluster;
        this.keyspace = keyspace;
        this.keyspaceDefinition = 
            cluster.describeKeyspace(keyspace.getKeyspaceName());
        getOrCreateColumnFamilyDefinition();
    }
    
    
    /**
     * Gets the Cassandra column definition for the associated SQL table.  Be
     * sure to call {@link #mapColumns()} first.
     */
    public ColumnFamilyDefinition getColumnFamilyDefinition() {
        return columnFamilyDefinition;
    }
    

    public void mapColumns() throws SQLException {
        try {
            lazyExecQuery();
            ResultSetMetaData meta = results.getMetaData();
            for(int col = 1; col <= meta.getColumnCount(); col++) {
                BasicColumnDefinition columnDef = new BasicColumnDefinition();
                columnDef.setName(stringSerializer.toByteBuffer(meta.getColumnName(col)));
                columnDef.setValidationClass(getComparatorType(meta.getColumnType(col)).getClassName());
                columnFamilyDefinition.addColumnDefinition(columnDef);
            }
        } finally {
            if (results != null) {
                results.close();
            }
        }
    }


    public void transferData() throws SQLException {
        Statement stmt = connection.createStatement();
        ResultSet results = 
            stmt.executeQuery("select * from " + tableName + " limit 5");
        ResultSetMetaData meta = results.getMetaData();
        while (results.next()) {
            for  (int col = 1; col <= meta.getColumnCount(); col++) {
                Object columnValue = results.getObject(col);
                String columnName = meta.getColumnName(col);
                String columnType = columnValue != null ? columnValue.getClass().getName() : null;
                System.out.println(columnName + "=" + columnValue + "[" + columnType + "]");
            }
            System.out.println("-");
        }
    }
    
    
    /**
     * Gets the Cassandra column type for the corresponding SQL column type.
     * 
     * @exception IllegalArgumentException if columnType isn't a recognized
     *                                     java.sql.Types constant, or we just
     *                                     haven't yet figured out how to map
     *                                     it quite yet
     */
    private ComparatorType getComparatorType(int sqlColumnType) {
        ComparatorType mappedType = jdbcToCassandraTypes.get(sqlColumnType);
        if (mappedType != null) {
            return mappedType;
        } else {
            throw new IllegalArgumentException("Unknown JDBC type: " + sqlColumnType);
        }
    }

    
    private void getOrCreateColumnFamilyDefinition() {
        if (tableName == null) {
            throw new IllegalStateException("tableName not set");
        }
        for (ColumnFamilyDefinition cfd : keyspaceDefinition.getCfDefs()) {
            if (cfd.getName().equals(tableName)) {
                this.columnFamilyDefinition = new BasicColumnFamilyDefinition(cfd);
                break;
            }
        }
        if (this.columnFamilyDefinition == null) {
            this.columnFamilyDefinition = new BasicColumnFamilyDefinition();
            this.columnFamilyDefinition.setName(tableName);
            this.columnFamilyDefinition.setKeyspaceName(keyspace.getKeyspaceName());
        }
    }

    
    private void lazyExecQuery() throws SQLException {
        if (results == null) {
            Statement stmt = connection.createStatement();
            this.results = stmt.executeQuery("select * from " + tableName + " limit 10");                       
        }
    }
}
