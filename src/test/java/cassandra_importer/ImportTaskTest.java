package cassandra_importer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;

public class ImportTaskTest {

    private ImportTask importTask;
    
    @Before
    public void setup() {
        this.importTask = new ImportTask();
    }
    
    @Test
    public void initialState() {
        assertEquals("initial state", ImportTask.State.Pending, importTask.getCurrentState());
    }
    
    /**
     * When an {@link ImportTask} has all the required information, it should
     * transition to {@link ImportTask.State#Ready} so that it can be queued
     * via {@link CassandraImporter#queueImportTask(ImportTask)}.
     */
    @Test
    public void taskBecomesReady() {
        assertEquals("current state", ImportTask.State.Pending, importTask.getCurrentState());
        
        this.importTask.setDataSource(mock(DataSource.class));
        assertEquals("current state", ImportTask.State.Pending, importTask.getCurrentState());
        
        this.importTask.setSourceTable("T1");
        assertEquals("current state", ImportTask.State.Pending, importTask.getCurrentState());
        
        this.importTask.setSourceSchema("schema");
        assertEquals("current state", ImportTask.State.Pending, importTask.getCurrentState());
        
        this.importTask.setTargetKeyspace("ks");
        assertEquals("current state", ImportTask.State.Pending, importTask.getCurrentState());
        
        this.importTask.setTargetColumnFamily("cf");
        
        assertEquals("current state", ImportTask.State.Ready, importTask.getCurrentState());
    }
    
    
    @Test
    public void firesStateChangeEvent() {
        // TODO
    }
    
    
    @Test
    public void providesProgressEvents() {
        
    }
    
}
