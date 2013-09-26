package cassandra_importer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

public class CassandraImporter {

    private Logger log = Logger.getLogger(getClass());    
    private Cluster targetCluster;
    
    private Queue<ImportTask> taskQueue;
    
    public CassandraImporter() {
        this.taskQueue = new LinkedBlockingQueue<ImportTask>();
    }
    
    public void setTargetCluster(Cluster cassandraCluster) {
        this.targetCluster = cassandraCluster;
    }

    
    /**
     * Queues an {@link ImportTask} to run.
     *  
     * @param importTask a task that's ready to run (in the Ready state)
     * @exception IllegalStateException if importTask is not in the Ready state
     */
    public void queueImportTask(ImportTask importTask) {
        if (ImportTask.State.Ready.equals(importTask.getCurrentState())) {
            this.taskQueue.add(importTask);
        } else {
            throw new IllegalStateException("Cannot add " + 
                                            importTask.getCurrentState() + "task");
        }        
    }

    
    public void run() {
        while (!taskQueue.isEmpty()) {
            ImportTask task = taskQueue.remove();
            Connection connection = null;
            try {
                connection = task.getDataSource().getConnection();
                mapColumnFamily(connection, task);
                transferData(connection, task);
            } catch (Exception e) {
                // FIXME: error handling
                e.printStackTrace();
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch(SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    
    private void mapColumnFamily(Connection connection, ImportTask importTask) {
        TableToColumnFamilyMapper mapper = 
                new TableToColumnFamilyMapper(targetCluster, connection);
        ColumnFamilyDefinition cfd =
                mapper.getColumnFamilyDefinition(importTask.getSourceTable(), 
                        importTask.getTargetKeyspace(), importTask.getTargetColumnFamily());
        log.info("Adding " + cfd.getName() + " to " + targetCluster.describeClusterName());
        log.debug(cfd.getColumnMetadata());
        if (isColumnFamilyDefined(importTask.getTargetKeyspace(), importTask.getTargetColumnFamily())) {
            targetCluster.updateColumnFamily(cfd, true);
        } else {
            targetCluster.addColumnFamily(cfd, true);
        }
    }

    
    private boolean isColumnFamilyDefined(String targetKeyspace, 
                                          String targetColumnFamilyName) {
        KeyspaceDefinition keyspace = targetCluster.describeKeyspace(targetKeyspace);
        List<ColumnFamilyDefinition> cfDefs = keyspace.getCfDefs();
        for (ColumnFamilyDefinition cfDef : cfDefs) {
            if (cfDef.getName().equals(targetColumnFamilyName)) {
                return true;
            }
        }
        return false;
    }
    
    
    private void transferData(Connection connection, ImportTask importTask) throws Exception {
        log.info("Transferring data for " + importTask);
        Keyspace keyspace = HFactory.createKeyspace(importTask.getTargetKeyspace(), targetCluster);
        ThriftColumnFamilyTemplate<Object,String> columnFamilyTemplate = 
                new ThriftColumnFamilyTemplate<Object, String>(keyspace, 
                                                               importTask.getTargetColumnFamily(), 
                                                               ObjectSerializer.get(), 
                                                               StringSerializer.get());

        int batchSize = 1000;
        int rowCount = 0;
        Statement stmt = connection.createStatement();
        // otherwise the MySQL driver attempts to read *all* results into
        // memory at once...
        stmt.setFetchSize(Integer.MIN_VALUE);
        connection.setCatalog(importTask.getSourceSchema());
        ResultSet results = 
            stmt.executeQuery("select * from " + importTask.getSourceTable());
        ResultSetMetaData meta = results.getMetaData();
        Mutator<Object> mutator = columnFamilyTemplate.createMutator();
        String columnFamily = importTask.getTargetColumnFamily();
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
                    log.debug(columnFamily + "," + key + "," + columnName + "," + columnValue);
                    mutator.addInsertion(key, columnFamily, column);
                }
            }
            rowCount++;
            if (rowCount % batchSize == 0) {
                System.out.println(rowCount);
                columnFamilyTemplate.executeBatch(mutator);
            }
        }
        columnFamilyTemplate.executeBatch(mutator);
    }
    
}
