import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCQuery {

    private Connection connection;
    private String query;
    private ResultSet results;
    
    
    public JDBCQuery(Connection connection, String query) {
        this.connection = connection;
        this.query = query;
    }
    
    
    
    public ResultSetMetaData getMetaDeta() throws SQLException {
        return results.getMetaData();
    }
    
    
    public ResultSet getResults() throws SQLException {
        if (results == null) {
            Statement stmt = connection.createStatement();
            this.results = stmt.executeQuery(query);
        }
        return results;
    }
    
}
