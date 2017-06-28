package io.kyligence.benchmark.loadtest.client;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.kylin.jdbc.Driver;

import io.kyligence.benchmark.loadtest.job.TestCase;

public class JdbcClient {

    protected String host;

    protected int port;

    protected String userName;

    protected String password;
    
    protected String projectName;

    protected Connection conn;

    public JdbcClient(TestCase testCase) throws Exception {
    	
        String user = testCase.getUserName();
        String pwd = testCase.getPassword();
        String host = testCase.getHost();
        int port = testCase.getPort();
        String projectName = testCase.getLoadTestProjectName();
        init(host, port, user, pwd, projectName);
        
    }
    
    public void close() throws SQLException {
    	this.conn.close();
    }

    private void init(String host, int port, String userName, String password, String projectName) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
    	
        this.host = host;
        this.port = port;
        this.userName = userName;
        this.password = password;
        this.projectName = projectName;
			
		Driver driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
		Properties info = new Properties();
		info.put("user", this.userName);
		info.put("password", this.password);
		this.conn = driver.connect("jdbc:kylin://master:7070/" + this.projectName, info); 
        
    }

	public ResultSet query(String sql) throws IOException, SQLException {
    	
    	Statement state = conn.createStatement();
    	return state.executeQuery(sql);
        
    }
	
}
