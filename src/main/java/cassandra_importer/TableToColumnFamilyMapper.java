package cassandra_importer;

import java.sql.Connection;

import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

public class TableToColumnFamilyMapper {

    private Connection sourceConnection;
    
    private Cluster targetCluster;
    
    public TableToColumnFamilyMapper(Cluster cluster, Connection connection) {
        this.targetCluster = cluster;
        this.sourceConnection = connection;
    }
    
    public ColumnFamilyDefinition getColumnFamilyDefinition(String sourceTableName,
                                                            String targetKeyspaceName,
                                                            String targetColumnFamilyName) {
        KeyspaceDefinition keyspaceDef = createKeyspace(targetKeyspaceName);
        ColumnFamilyDefinition columnFamilyDefinition = 
                getOrCreateColumnFamilyDefinition(keyspaceDef, targetColumnFamilyName);
        return columnFamilyDefinition;
    }

    
    private KeyspaceDefinition createKeyspace(String keyspaceName) {
        KeyspaceDefinition keyspaceDef = targetCluster.describeKeyspace(keyspaceName);
        if (keyspaceDef == null) {
            KeyspaceDefinition newKeyspace =
                    HFactory.createKeyspaceDefinition(keyspaceName);
            targetCluster.addKeyspace(newKeyspace, true);
        }
        HFactory.createKeyspace(keyspaceName, targetCluster);
        return keyspaceDef;
    }
    
    
    private ColumnFamilyDefinition getOrCreateColumnFamilyDefinition(KeyspaceDefinition keyspaceDef,
            String targetColumnFamilyName) {
        BasicColumnFamilyDefinition columnFamilyDefinition = null;
        for (ColumnFamilyDefinition cfd : keyspaceDef.getCfDefs()) {
            if (cfd.getName().equals(targetColumnFamilyName)) {
                columnFamilyDefinition = new BasicColumnFamilyDefinition(cfd);
                break;
            }
        }
        if (columnFamilyDefinition == null) {
            columnFamilyDefinition = new BasicColumnFamilyDefinition();
            columnFamilyDefinition.setName(targetColumnFamilyName);
            columnFamilyDefinition.setKeyspaceName(keyspaceDef.getName());
        }
        return columnFamilyDefinition;
    }
}
