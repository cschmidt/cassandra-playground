package columns;

import java.lang.reflect.Method;
import java.sql.ResultSet;

import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ComparatorType;


public class ColumnMapper {

    private ComparatorType cassandraType;
    private Method columnFamilySetter;
    private Method resultSetGetter;
    private int sqlType;
    
    public ColumnMapper(int sqlType,
                        ComparatorType cassandraType, 
                        Method resultSetGetter, 
                        Method columnFamilySetter) {
        
        this.resultSetGetter = resultSetGetter;
        this.columnFamilySetter = columnFamilySetter;
        this.cassandraType = cassandraType;
        this.sqlType = sqlType;
    }

    public void map(ResultSet results,
                    int columnNum,
                    ColumnFamilyUpdater<String, String> updater,
                    String columnName) throws Exception {
        Object[] rsgArgs = {columnNum};
        Object[] setterArgs = {columnName, resultSetGetter.invoke(results, rsgArgs)};
        columnFamilySetter.invoke(updater, setterArgs);
    }
    
    public ComparatorType getCassandraType() {
        return cassandraType;
    }
    
}
