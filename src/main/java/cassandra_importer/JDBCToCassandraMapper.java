package cassandra_importer;
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


import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;


/**
 * Creates a column definition from the meta data describing a JDBC
 * result set.
 * @author cschmidt
 */
public class JDBCToCassandraMapper {
    
    private static StringSerializer stringSerializer = StringSerializer.get();
    private static Map<Integer, ComparatorType> sqlToCassandraTypes;
    
    static {
        sqlToCassandraTypes = new HashMap<Integer,ComparatorType>();
        try {
            addMapping(Types.BIGINT, ComparatorType.INTEGERTYPE);
            addMapping(Types.INTEGER, ComparatorType.INTEGERTYPE);
            addMapping(Types.VARCHAR, ComparatorType.UTF8TYPE);
            addMapping(Types.LONGVARCHAR, ComparatorType.UTF8TYPE);
            addMapping(Types.TIMESTAMP, ComparatorType.DATETYPE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }        
    }
    
    
    private static void addMapping(int sqlType, 
                                   ComparatorType cassandraType) throws Exception {
        sqlToCassandraTypes.put(sqlType, cassandraType);
    }
    
    
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
    
    private ColumnFamilyUpdater columnFamilyUpdater;
    
    
    
    public JDBCToCassandraMapper(Connection connection, 
                                 String tableName,
                                 Cluster cluster,
                                 Keyspace keyspace) throws Exception {
        this.connection = connection;
        this.tableName = tableName;
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
            lazyExecQuery();
            ResultSetMetaData meta = results.getMetaData();
            for(int col = 1; col <= meta.getColumnCount(); col++) {
                int sqlType = meta.getColumnType(col);
                BasicColumnDefinition columnDef = new BasicColumnDefinition();
                System.out.println(meta.getColumnName(col));
                columnDef.setName(stringSerializer.toByteBuffer(meta.getColumnName(col)));
                columnDef.setValidationClass(getComparatorType(sqlType).getClassName());
                columnFamilyDefinition.addColumnDefinition(columnDef);
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


    public void transferDataViaMutator() throws Exception {
        this.rowCount = 0;
        int batchSize = 1000;
        Statement stmt = connection.createStatement();
        // otherwise the MySQL driver attempts to read *all* results into
        // memory at once...
        stmt.setFetchSize(Integer.MIN_VALUE);
        ResultSet results = 
            stmt.executeQuery("select * from " + tableName + " limit 500000");
        ResultSetMetaData meta = results.getMetaData();
        Mutator<Object> mutator = this.columnFamilyTemplate.createMutator();
        while (results.next()) {
            Object key = results.getObject(1).toString();            
            for  (int col = 2; col <= meta.getColumnCount(); col++) {
                Object columnValue = results.getObject(col);
                String columnName = meta.getColumnName(col);
                if (columnValue != null) {
                    HColumn<String,Object> column = 
                        HFactory.createColumn(columnName, 
                                              columnValue, 
                                              StringSerializer.get(), 
                                              SerializerTypeInferer.getSerializer(columnValue));
                    mutator.addInsertion(key, tableName, column);
                }
            }
            rowCount++;
            if (rowCount % batchSize == 0) {
                System.out.println(rowCount);
                columnFamilyTemplate.executeBatch(mutator);
            }
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
