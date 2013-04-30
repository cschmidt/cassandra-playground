package cassandra_importer;

import java.lang.reflect.Method;
import java.sql.ResultSet;

import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;


class ColumnMapper {

    private Method columnFamilySetter;
    private Method resultSetGetter;
    
    ColumnMapper(Method resultSetGetter, 
                 Method columnFamilySetter) {
        
        this.resultSetGetter = resultSetGetter;
        this.columnFamilySetter = columnFamilySetter;
    }

    void map(ResultSet results,
             int columnNum,
             ColumnFamilyUpdater<String, String> updater,
             String columnName) throws Exception {
        Object[] rsgArgs = {columnNum};
        Object[] setterArgs = {columnName, resultSetGetter.invoke(results, rsgArgs)};
        columnFamilySetter.invoke(updater, setterArgs);
    }    
}
