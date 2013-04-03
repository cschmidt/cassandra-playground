package columns;

import java.sql.ResultSet;
import java.sql.SQLException;

import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;

public class IntegerColumnUpdater extends ColumnUpdater {

    public IntegerColumnUpdater(String columnName, int columnNum) {
        super(columnName, columnNum);
    }
    
    @Override
    public void map(ResultSet results, ColumnFamilyUpdater<String, String> updater)
            throws SQLException {
        updater.setInteger(columnName, results.getInt(columnNum));
    }
    
}
