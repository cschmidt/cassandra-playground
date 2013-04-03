import java.sql.Connection;
import java.sql.DriverManager;

import me.prettyprint.hector.api.*;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

public class CassandraDemo {
    

    public static Keyspace createKeyspace(final Cluster cluster, 
                                          final String keyspaceName) {
        KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(keyspaceName);
        if (keyspaceDef == null) {
            KeyspaceDefinition newKeyspace = 
                HFactory.createKeyspaceDefinition(keyspaceName);
            cluster.addKeyspace(newKeyspace, true);
        }
        return HFactory.createKeyspace(keyspaceName, cluster);
    }

    
    public static void main(String[] args) throws Exception {
        Connection con = 
            DriverManager.getConnection("jdbc:mysql://localhost/lp_webapp?" +
                                        "user=root");
        Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "cassandra01.local:9160");
        Keyspace keyspace = createKeyspace(cluster, "lp_webapp");
        JDBCToCassandraMapper mapper = 
            new JDBCToCassandraMapper(con, "form_submissions", cluster, keyspace);
        mapper.mapColumnTypes();        
        System.out.println(mapper.getColumnFamilyDefinition());
        cluster.addColumnFamily(mapper.getColumnFamilyDefinition(), true);
        
        mapper.transferData();
        System.out.println("Done");
    }
}
