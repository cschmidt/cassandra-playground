package cassandra_importer;

import static org.junit.Assert.*;

import org.junit.Test;

public class ImportTaskTest {

    @Test
    public void testInitialState() {
        ImportTask importTask = new ImportTask();
        assertEquals("initial state", ImportTask.State.Pending, importTask.getCurrentState());
    }
    
}
