package cassandra_importer;

import javax.sql.DataSource;

/**
 * States -> Pending, Ready, Running, Done
 * @author cschmidt
 *
 */
public class ImportTask {

    public enum State {Pending, Ready, Done};
    
    private State currentState = State.Pending;
    
    private DataSource dataSource;
    
    private String sourceTable;

    private String sourceSchema;

    private String targetColumnFamily;
    
    private String targetKeyspace;
    
    public State getCurrentState() {
        return currentState;
    }

    
    public DataSource getDataSource() {
        return dataSource;
    }
    
    public void setDataSource(DataSource dataSource) {        
        this.dataSource = dataSource;
        checkReady();
    }

    
    public String getSourceSchema() {
        return this.sourceSchema;
    }
    
    public void setSourceSchema(String sourceSchema) {
        this.sourceSchema = sourceSchema;
    }


    public String getSourceTable() {
        return this.sourceTable;
    }
    
    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
        checkReady();
    }

    
    public String getTargetColumnFamily() {
        return this.targetColumnFamily;
    }
    
    public void setTargetColumnFamily(String targetColumnFamily) {
        this.targetColumnFamily = targetColumnFamily;
        checkReady();
    }
    
    
    public String getTargetKeyspace() {
        return this.targetKeyspace;
    }
    
    public void setTargetKeyspace(String targetKeyspace) {
        this.targetKeyspace = targetKeyspace;
        checkReady();
    }

    
    private void checkReady() {
        // FIXME: having to call checkReady on all setXXX methods is pretty
        // error prone
        if (this.dataSource != null &&
            notEmpty(getSourceTable()) &&
            notEmpty(getSourceSchema()) &&
            notEmpty(getTargetKeyspace()) &&
            notEmpty(getTargetColumnFamily()) ) {
            
            this.currentState = State.Ready;
        }
    }
    
    
    private boolean notEmpty(String s) {
        return s != null && !s.trim().isEmpty();
    }
}
