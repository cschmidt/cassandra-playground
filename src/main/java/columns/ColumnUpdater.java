package columns;

import java.sql.ResultSet;

import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;


public abstract class ColumnUpdater {

    String columnName;
    int columnNum;
    
    public ColumnUpdater(String columnName, 
                         int columnNum) {
        this.columnName = columnName;
        this.columnNum = columnNum;
    }
    
    
    public abstract void map(ResultSet results, ColumnFamilyUpdater<String,String> updater)
        throws Exception;
}
