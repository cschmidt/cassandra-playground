package cassandra_importer;

import java.sql.Connection;

import javax.sql.DataSource;

import me.prettyprint.hector.api.Cluster;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration( )
public class TableToColumnFamilyMapperTest {

    @Autowired
    private Cluster cluster;
    
    private Connection connection;
    
    @Autowired
    private DataSource dataSource;
    
    @Test
    public void basic() throws Exception {
        TableToColumnFamilyMapper mapper = 
                new TableToColumnFamilyMapper(cluster, connection);
    }
}
