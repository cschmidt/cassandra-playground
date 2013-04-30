package cassandra_importer;

import static org.junit.Assert.*;

import javax.sql.DataSource;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;


public class CassandraImporterTest {

    private static Cluster cluster;
    
    private static ClassPathXmlApplicationContext context;
    
    private static DataSource dataSource;
    
    private static Keyspace keyspace;
    
    private static String keyspaceName = "Cassandra_Importer_Test";
    
    
    @BeforeClass
    public static void setup() throws Exception {
        context = new ClassPathXmlApplicationContext("beans.xml");
        // cluster = HFactory.getOrCreateCluster("Test Cluster", "cassandra01.local:9160");
        dataSource = context.getBean(DataSource.class);
        cluster = context.getBean(Cluster.class);
        if (cluster.describeKeyspace(keyspaceName) == null) {
            KeyspaceDefinition keyspaceDef = 
                    HFactory.createKeyspaceDefinition(keyspaceName);
            cluster.addKeyspace(keyspaceDef, true);
        }
        keyspace = HFactory.createKeyspace(keyspaceName, cluster);
    }

    @AfterClass
    public static void shutdown() throws Exception {
        context.close();
    }
    
    @Test
    public void basic() throws Exception {
        CassandraImporter importer = new CassandraImporter();
        importer.setTargetCluster(cluster);
        
        ImportTask importTask = new ImportTask();
        importTask.setDataSource(dataSource);
        importTask.setSourceTable("T1");
        
        importer.addImportTask(importTask);        
        importer.run();
        
        
        ColumnFamilyTemplate<String, String> template =
                new ThriftColumnFamilyTemplate<String, String>(keyspace,
                                                               "T1",
                                                               StringSerializer.get(),
                                                               StringSerializer.get());        
        ColumnFamilyResult<String, String> res = template.queryColumns("1");
        String value = res.getString("description");
        assertEquals("Retrieved description", "description0", value);
    }
    
    
    @Test
    public void identifiesSimplePrimaryKey() throws Exception {
        
    }
    
    
    @Test
    public void identifiesDataTypes() throws Exception {
        
    }
    

    @Test
    public void compoundPrimaryKey() throws Exception {
        
    }
    
    
    @Test
    public void identifiesIndices() throws Exception {
        
    }
}
