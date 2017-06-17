import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Easy to use database connection class
 *
 * @author Rudy Mas
 * @version 1.3.0
 */
public class DBconnect {
    public int rows;
    public ResultSet data;

    private Connection connection;
    private Statement statement;
    private ResultSet result;
    private ResultSet generatedKeys;

    public DBconnect(String host, String database, String username, String password) {
        String databaseType = "mysql";
        String charset = "utf-8";
        String dbUrl = "jdbc:" + databaseType + "://" + host + "/" + database + "?useUnicode=true&characterEncoding=" + charset;
        this.makeDbConnection(dbUrl, username, password);
    }

    public DBconnect(String host, String database, String username, String password, String databaseType) {
        String charset = "utf-8";
        String dbUrl = "jdbc:" + databaseType + "://" + host + "/" + database + "?useUnicode=true&characterEncoding=" + charset;
        this.makeDbConnection(dbUrl, username, password);
    }

    public DBconnect(String host, String database, String username, String password, String databaseType, String charset) {
        String dbUrl = "jdbc:" + databaseType + "://" + host + "/" + database + "?useUnicode=true&characterEncoding=" + charset;
        this.makeDbConnection(dbUrl, username, password);
    }

    private void makeDbConnection(String dbUrl, String username, String password) {
        try {
            this.connection = DriverManager.getConnection(dbUrl, username, password);
        } catch (SQLException ex) {
            System.out.println("[ERROR: makeDbConnection] There was an error connection to the database.");
            ex.printStackTrace(System.err);
        }
    }

    public void close() {
        try {
            if (this.result != null) {
                this.result.close();
            }
            if (this.statement != null) {
                this.statement.close();
            }
            this.connection.close();
        } catch (SQLException ex) {
            System.out.println("[ERROR: close] There was an error closing down the database connection.");
            ex.printStackTrace(System.err);
        }
    }

    public void query(String strQuery) {
        try {
            this.statement = this.connection.createStatement();
            this.result = this.statement.executeQuery(strQuery);
            this.getRows();
        } catch (SQLException ex) {
            System.out.println("[ERROR: query] There was an error executing your query.");
            ex.printStackTrace(System.err);
        }
    }

    private void exec(String strQuery) {
        try {
            this.statement = this.connection.createStatement();
            this.rows = this.statement.executeUpdate(strQuery);
        } catch (SQLException ex) {
            System.out.println("[ERROR: exec] There was an error executing your query.");
            ex.printStackTrace(System.err);
        }
    }

    private void exec(String strQuery, int statement) {
        try {
            this.statement = this.connection.createStatement();
            this.rows = this.statement.executeUpdate(strQuery, statement);
            this.generatedKeys = this.statement.getGeneratedKeys();
        } catch (SQLException ex) {
            System.out.println("[ERROR: exec] There was an error executing your query.");
            ex.printStackTrace(System.err);
        }
    }

    public ResultSet getGeneratedKeys() {
        return this.generatedKeys;
    }

    public void fetchAll() {
        try {
            this.result.beforeFirst();
            this.data = this.result;
        } catch (SQLException ex) {
            System.out.println("[ERROR: fetchAll] There was an error fetching your data.");
            ex.printStackTrace(System.err);
        }
    }

    public void fetch() {
        try {
            this.result.next();
            this.data = this.result;
        } catch (SQLException ex) {
            System.out.println("[ERROR: fetch] There was an error fetching your data.");
            ex.printStackTrace(System.err);
        }
    }

    public void fetch(int row) {
        try {
            this.result.absolute(row + 1);
            this.data = this.result;
        } catch (SQLException ex) {
            System.out.println("[ERROR: fetch] There was an error fetching your data.");
            ex.printStackTrace(System.err);
        }
    }

    public String queryItem(String strQuery, String field) {
        this.query(strQuery);
        this.fetch(0);
        try {
            if (this.rows > 0) {
                return this.result.getString(field);
            }
            return null;
        } catch (SQLException ex) {
            System.out.println("[ERROR: queryItem] There was an error executing your query.");
            ex.printStackTrace();
        }
        return null;
    }

    public void queryRow(String strQuery) {
        this.query(strQuery);
        this.fetch(0);
        this.data = this.result;
    }

    public void insert(String strQuery) {
        this.exec(strQuery);
    }

    public void insert(String strQuery, int statement) {
        this.exec(strQuery, statement);
    }

    public void update(String strQuery) {
        this.exec(strQuery);
    }

    public void delete(String strQuery) {
        this.exec(strQuery);
    }

    public PreparedStatement prepare(String statement) {
        try {
            PreparedStatement preparedStatement = this.connection.prepareStatement(statement);
            return preparedStatement;
        } catch (SQLException ex) {
            System.out.println("[ERROR: prepare] There was an error with your prepared statement.");
            ex.printStackTrace();
            return null;
        }
    }

    public void bindParam(PreparedStatement preparedStatement, int paramno, byte param) {
        try {
            preparedStatement.setByte(paramno, param);
        } catch (SQLException ex) {
            System.out.println("[ERROR: bindParam(byte)] There was an error with binding your data to the prepared statement.");
            ex.printStackTrace();
        }
    }

    public void bindParam(PreparedStatement preparedStatement, int paramno, Array param) {
        try {
            preparedStatement.setArray(paramno, param);
        } catch (SQLException ex) {
            System.out.println("[ERROR: bindParam(Array)] There was an error with binding your data to the prepared statement.");
            ex.printStackTrace();
        }
    }

    public void bindParam(PreparedStatement preparedStatement, int paramno, short param) {
        try {
            preparedStatement.setShort(paramno, param);
        } catch (SQLException ex) {
            System.out.println("[ERROR: bindParam(short)] There was an error with binding your data to the prepared statement.");
            ex.printStackTrace();
        }
    }

    public void bindParam(PreparedStatement preparedStatement, int paramno, int param) {
        try {
            preparedStatement.setInt(paramno, param);
        } catch (SQLException ex) {
            System.out.println("[ERROR: bindParam(int)] There was an error with binding your data to the prepared statement.");
            ex.printStackTrace();
        }
    }

    public void bindParam(PreparedStatement preparedStatement, int paramno, long param) {
        try {
            preparedStatement.setLong(paramno, param);
        } catch (SQLException ex) {
            System.out.println("[ERROR: bindParam(long)] There was an error with binding your data to the prepared statement.");
            ex.printStackTrace();
        }
    }

    public void bindParam(PreparedStatement preparedStatement, int paramno, float param) {
        try {
            preparedStatement.setFloat(paramno, param);
        } catch (SQLException ex) {
            System.out.println("[ERROR: bindParam(float)] There was an error with binding your data to the prepared statement.");
            ex.printStackTrace();
        }
    }

    public void bindParam(PreparedStatement preparedStatement, int paramno, double param) {
        try {
            preparedStatement.setDouble(paramno, param);
        } catch (SQLException ex) {
            System.out.println("[ERROR: bindParam(double)] There was an error with binding your data to the prepared statement.");
            ex.printStackTrace();
        }
    }

    public void bindParam(PreparedStatement preparedStatement, int paramno, BigDecimal param) {
        try {
            preparedStatement.setBigDecimal(paramno, param);
        } catch (SQLException ex) {
            System.out.println("[ERROR: bindParam(BigDecimal)] There was an error with binding your data to the prepared statement.");
            ex.printStackTrace();
        }
    }

    public void bindParam(PreparedStatement preparedStatement, int paramno, boolean param) {
        try {
            preparedStatement.setBoolean(paramno, param);
        } catch (SQLException ex) {
            System.out.println("[ERROR: bindParam(boolean)] There was an error with binding your data to the prepared statement.");
            ex.printStackTrace();
        }
    }

    public void bindParam(PreparedStatement preparedStatement, int paramno, String param) {
        try {
            preparedStatement.setString(paramno, param);
        } catch (SQLException ex) {
            System.out.println("[ERROR: bindParam(string)] There was an error with binding your data to the prepared statement.");
            ex.printStackTrace();
        }
    }

    public void bindParam(PreparedStatement preparedStatement, int paramno, byte[] param) {
        try {
            preparedStatement.setBytes(paramno, param);
        } catch (SQLException ex) {
            System.out.println("[ERROR: bindParam(byte[])] There was an error with binding your data to the prepared statement.");
            ex.printStackTrace();
        }
    }

    public void bindParam(PreparedStatement preparedStatement, int paramno, Date param) {
        try {
            preparedStatement.setDate(paramno, param);
        } catch (SQLException ex) {
            System.out.println("[ERROR: bindParam(Date)] There was an error with binding your data to the prepared statement.");
            ex.printStackTrace();
        }
    }

    public void bindParam(PreparedStatement preparedStatement, int paramno, Time param) {
        try {
            preparedStatement.setTime(paramno, param);
        } catch (SQLException ex) {
            System.out.println("[ERROR: bindParam(Time)] There was an error with binding your data to the prepared statement.");
            ex.printStackTrace();
        }
    }

    public void bindParam(PreparedStatement preparedStatement, int paramno, Timestamp param) {
        try {
            preparedStatement.setTimestamp(paramno, param);
        } catch (SQLException ex) {
            System.out.println("[ERROR: bindParam(Timestamp)] There was an error with binding your data to the prepared statement.");
            ex.printStackTrace();
        }
    }

    public void bindParam(PreparedStatement preparedStatement, int paramno, InputStream param, String type) {
        try {
            switch (type.toLowerCase()) {
                case "ascii":
                    preparedStatement.setAsciiStream(paramno, param);
                    break;
                case "binary":
                    preparedStatement.setBinaryStream(paramno, param);
                    break;
            }
        } catch (SQLException ex) {
            System.out.println("[ERROR: bindParam(InputStream)] There was an error with binding your data to the prepared statement.");
            ex.printStackTrace();
        }
    }

    public void bindParam(PreparedStatement preparedStatement, int paramno, Reader param) {
        try {
            preparedStatement.setCharacterStream(paramno, param);
        } catch (SQLException ex) {
            System.out.println("[ERROR: bindParam(Reader)] There was an error with binding your data to the prepared statement.");
            ex.printStackTrace();
        }
    }

    public void bindParam(PreparedStatement preparedStatement, int paramno, Clob param) {
        try {
            preparedStatement.setClob(paramno, param);
        } catch (SQLException ex) {
            System.out.println("[ERROR: bindParam(Clob)] There was an error with binding your data to the prepared statement.");
            ex.printStackTrace();
        }
    }

    public void bindParam(PreparedStatement preparedStatement, int paramno, Blob param) {
        try {
            preparedStatement.setBlob(paramno, param);
        } catch (SQLException ex) {
            System.out.println("[ERROR: bindParam(Blob)] There was an error with binding your data to the prepared statement.");
            ex.printStackTrace();
        }
    }

    public void execute(PreparedStatement preparedStatement) {
        try {
            this.result = preparedStatement.executeQuery();
            this.getRows();
        } catch (SQLException ex) {
            System.out.println("[ERROR: execute] There was an error executing your query.");
            ex.printStackTrace();
        }
    }

    public void execute(PreparedStatement preparedStatement, String type) {
        try {
            switch (type.toLowerCase()) {
                case "query":
                    this.result = preparedStatement.executeQuery();
                    this.getRows();
                    break;
                case "update":
                    this.rows = preparedStatement.executeUpdate();
                    break;
                default:
                    this.result = preparedStatement.executeQuery();
                    this.getRows();
            }
        } catch (SQLException ex) {
            System.out.println("[ERROR: execute(type)] There was an error executing your query.");
            ex.printStackTrace();
        }
    }

    public void setAutoCommit(boolean autoCommit) {
        try {
            this.connection.setAutoCommit(autoCommit);
        } catch (SQLException ex) {
            System.out.println("[ERROR: setAutoCommit] There was an error with setting your auto commit.");
            ex.printStackTrace();
        }
    }

    public void setSavePoint(String nameSavepoint) {
        try {
            this.connection.setSavepoint(nameSavepoint);
        } catch (SQLException ex) {
            System.out.println("[ERROR: setSavePoint] There was an error with setting your save point.");
            ex.printStackTrace();
        }
    }

    public void rollback() {
        try {
            this.connection.rollback();
        } catch (SQLException ex) {
            System.out.println("[ERROR: rollbacl] There was an error with rolling back your queries.");
            ex.printStackTrace();
        }
    }

    public void rollback(Savepoint nameSavepoint) {
        try {
            this.connection.rollback(nameSavepoint);
            ;
        } catch (SQLException ex) {
            System.out.println("[ERROR: rollback(Savepoint)] There was an error with rolling back your queries to a save point.");
            ex.printStackTrace();
        }
    }

    public void commit() {
        try {
            this.connection.commit();
        } catch (SQLException ex) {
            System.out.println("[ERROR: commit] There was an error committing your queries.");
            ex.printStackTrace();
        }
    }

    public int getTransactionIsolation() {
        try {
            return this.connection.getTransactionIsolation();
        } catch (SQLException ex) {
            System.out.println("[ERROR: getTransactionIsolation] There was an error getting your transaction isolation settings.");
            ex.printStackTrace();
        }
        return 0;
    }

    public void setTransactionIsolation(int level) {
        try {
            this.connection.setTransactionIsolation(level);
        } catch (SQLException ex) {
            System.out.println("[ERROR: setTransactionIsolation] There was an error setting your transaction isolation settings.");
            ex.printStackTrace();
        }
    }

    private void getRows() {
        try {
            this.rows = this.result.last() ? this.result.getRow() : 0;
            this.result.beforeFirst();
        } catch (SQLException ex) {
            System.out.println("[ERROR: getRows] There was an error with determining the rows effected by your query.");
            ex.printStackTrace(System.err);
        }
    }
}