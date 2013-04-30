package cassandra_importer;

import javax.sql.DataSource;

/**
 * States -> Pending, Ready, Running, Done
 * @author cschmidt
 *
 */
public class ImportTask {

    public enum State {Pending};
    
    private State currentState = State.Pending;
    
    public void setDataSource(DataSource dataSource) {
        // TODO Auto-generated method stub
        
    }

    public void setSourceTable(String string) {
        // TODO Auto-generated method stub
        
    }

    public State getCurrentState() {
        return currentState;
    }

}
