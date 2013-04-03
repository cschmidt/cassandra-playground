import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import columns.ColumnMapper;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
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
    private static Map<Integer, ComparatorType> sqlToCassandraTypes;
    private static Map<Integer, ColumnMapper> sqlTypeToColumnMapper;
    private static Logger log = Logger.getLogger(JDBCToCassandraMapper.class.toString());
    
    static {
        sqlToCassandraTypes = new HashMap<Integer,ComparatorType>();
        sqlTypeToColumnMapper = new HashMap<Integer,ColumnMapper>();
        try {
            addMapping(Types.BIGINT, "getInt", ComparatorType.INTEGERTYPE, "setInteger", Integer.class);
            addMapping(Types.INTEGER, "getInt", ComparatorType.INTEGERTYPE, "setInteger", Integer.class);
            addMapping(Types.VARCHAR, "getString", ComparatorType.UTF8TYPE, "setString", String.class);
            addMapping(Types.LONGVARCHAR, "getString", ComparatorType.UTF8TYPE, "setString", String.class);
            addMapping(Types.TIMESTAMP, "getTimestamp", ComparatorType.DATETYPE, "setDate", Date.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }        
    }
    
    
    private static void addMapping(int sqlType, 
                                   String resultSetGetter,
                                   ComparatorType cassandraType,
                                   String columnFamilySetter,
                                   Class<?> setterParamType) throws Exception {
        Method getter = ResultSet.class.getDeclaredMethod(resultSetGetter, int.class);
        Class<?>[] paramTypes = {Object.class, setterParamType};
        Method setter = ColumnFamilyUpdater.class.getDeclaredMethod(columnFamilySetter, paramTypes);
        ColumnMapper columnMapper = new ColumnMapper(sqlType, cassandraType, getter, setter);
        sqlToCassandraTypes.put(sqlType, cassandraType);
        sqlTypeToColumnMapper.put(sqlType, columnMapper);
    }
    
    
    /**
     * The target Cassandra cluster
     */
    private Cluster cluster;
    private Connection connection;
    private BasicColumnFamilyDefinition columnFamilyDefinition;
    
    private ColumnFamilyTemplate columnFamilyTemplate;
    
    
    /**
     * The target Cassandra keyspace
     */
    private Keyspace keyspace;
    
    private KeyspaceDefinition keyspaceDefinition;
    
    private ResultSet results;
    
    /**
     * The number of rows converted from SQL to Cassandra
     */
    private int rowCount;
    
    /**
     * The name of the table we're pulling into Cassandra from the JDBC source.
     * We'll use this name for the Cassandra keyspace as well.
     */
    private String tableName;
    
    private Map<Integer, ColumnMapper> columnMappers;
    private ColumnFamilyUpdater columnFamilyUpdater;
    
    
    
    public JDBCToCassandraMapper(Connection connection, 
                                 String tableName,
                                 Cluster cluster,
                                 Keyspace keyspace) throws Exception {
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
     * sure to call {@link #mapColumnTypes()} first.
     */
    public ColumnFamilyDefinition getColumnFamilyDefinition() {
        return columnFamilyDefinition;
    }
    

    public void mapColumnTypes() throws Exception {
        try {
            this.columnMappers = new HashMap<Integer,ColumnMapper>();
            lazyExecQuery();
            ResultSetMetaData meta = results.getMetaData();
            for(int col = 1; col <= meta.getColumnCount(); col++) {
                int sqlType = meta.getColumnType(col);
                BasicColumnDefinition columnDef = new BasicColumnDefinition();
                System.out.println(meta.getColumnName(col));
                columnDef.setName(stringSerializer.toByteBuffer(meta.getColumnName(col)));
                columnDef.setValidationClass(getComparatorType(sqlType).getClassName());
                columnFamilyDefinition.addColumnDefinition(columnDef);
                ColumnMapper columnMapper = sqlTypeToColumnMapper.get(sqlType);
                columnMappers.put(col, columnMapper);
            }
            this.columnFamilyTemplate = 
                new ThriftColumnFamilyTemplate<String, String>(keyspace, 
                                                               tableName, 
                                                               StringSerializer.get(), 
                                                               StringSerializer.get());
            this.columnFamilyUpdater = columnFamilyTemplate.createUpdater();
        } finally {
            if (results != null) {
                results.close();
            }
        }
    }


    public void transferData() throws Exception {
        this.rowCount = 0;
        Statement stmt = connection.createStatement();
        ResultSet results = 
            stmt.executeQuery("select * from " + tableName + " limit 50000");
        ResultSetMetaData meta = results.getMetaData();
        while (results.next()) {
            this.columnFamilyUpdater = 
                this.columnFamilyTemplate.createUpdater(results.getObject(1).toString());            
            for  (int col = 2; col <= meta.getColumnCount(); col++) {
                Object columnValue = results.getObject(col);
                String columnName = meta.getColumnName(col);
                String columnType = columnValue != null ? columnValue.getClass().getName() : null;
                // System.out.println(columnName + "=" + columnValue + "[" + columnType + "]");
                if (columnValue != null) {
                    ColumnMapper mapper = getColumnMapper(col);
                    mapper.map(results, col, columnFamilyUpdater, columnName);
                }
            }
            rowCount++;
            columnFamilyTemplate.update(columnFamilyUpdater);
            if (rowCount % 500 == 0) {
                System.out.println(rowCount);
            }
        }
    }
    
    
    private ColumnMapper getColumnMapper(int col) {
        return columnMappers.get(col);
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
        ComparatorType mappedType = sqlToCassandraTypes.get(sqlColumnType);
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
