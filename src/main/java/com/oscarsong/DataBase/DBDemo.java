package com.oscarsong.DataBase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.DatabaseMetaData;
import java.util.Properties;
import java.util.HashMap;
/**
 * This class demonstrates how to connect to MySQL and run some basic commands.
 * 
 * In order to use this, you have to download the Connector/J driver and add
 * its .jar file to your build path.  You can find it here:
 * 
 * http://dev.mysql.com/downloads/connector/j/
 * 
 * You will see the following exception if it's not in your class path:
 * 
 * java.sql.SQLException: No suitable driver found for jdbc:mysql://localhost:3306/
 * 
 * To add it to your class path:
 * 1. Right click on your project
 * 2. Go to Build Path -> Add External Archives...
 * 3. Select the file mysql-connector-java-5.1.24-bin.jar
 *    NOTE: If you have a different version of the .jar file, the name may be
 *    a little different.
 *    
 * The user name and password are both "root", which should be correct if you followed
 * the advice in the MySQL tutorial. If you want to use different credentials, you can
 * change them below. 
 * 
 * You will get the following exception if the credentials are wrong:
 * 
 * java.sql.SQLException: Access denied for user 'userName'@'localhost' (using password: YES)
 * 
 * You will instead get the following exception if MySQL isn't installed, isn't
 * running, or if your serverName or portNumber are wrong:
 * 
 * java.net.ConnectException: Connection refused
 */
public class DBDemo {

	/** The name of the MySQL account to use (or empty for anonymous) */
	private final String userName = "root";

	/** The password for the MySQL account (or empty for anonymous) */
	private final String password = "root";

	/** The name of the computer running MySQL */
	private final String serverName = "localhost";

	/** The port of the MySQL server (default is 3306) */
	private final int portNumber = 3306;

	/** The name of the database we are testing with (this default is installed with MySQL) */
	private final String dbName = "test";
	
	/** The name of the table we are testing with */
	private final String tableName = "JDBC_TEST";
	
	/**
	 * Get a new database connection
	 * 
	 * @return
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException {
		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", this.userName);
		connectionProps.put("password", this.password);
		connectionProps.setProperty("useSSL", "false");
		conn = DriverManager.getConnection("jdbc:mysql://"
				+ this.serverName + ":" + this.portNumber + "/" + this.dbName,
				connectionProps);

		return conn;
	}

	/**
	 * Run a SQL command which does not return a recordset:
	 * CREATE/INSERT/UPDATE/DELETE/DROP/etc.
	 * 
	 * @throws SQLException If something goes wrong
	 */
	public boolean executeUpdate(Connection conn, String command) throws SQLException {
	    Statement stmt = null;
	    try {
	        stmt = conn.createStatement();
	        stmt.executeUpdate(command); // This will throw a SQLException if it fails
	        return true;
	    } finally {

	    	// This will run whether we throw an exception or not
	        if (stmt != null) { stmt.close(); }
	    }
	}
	
	/**
	 * Connect to MySQL and do some stuff.
	 */
	public void run() {

		// Connect to MySQL
		Connection conn = null;
		try {
			conn = this.getConnection();
			System.out.println("Connected to database");
		} catch (SQLException e) {
			System.out.println("ERROR: Could not connect to the database");
			e.printStackTrace();
			return;
		}

		// Create a table
		
		try {
			DatabaseMetaData dbm = conn.getMetaData();
			ResultSet rs = dbm.getTables(null, null, this.tableName, null);
			//If table doesn't exist
			if(!rs.next()) {
				String createString =
				        "CREATE TABLE " + this.tableName + " ( " +
				        "NAME varchar(40) NOT NULL, " +
				        "AGE INTEGER NOT NULL, " +
				        "PRIMARY KEY (NAME))";
				this.executeUpdate(conn, createString);
				System.out.println("Created a table");
			}	    
	    } catch (SQLException e) {
			System.out.println("ERROR: Could not create the table");
			e.printStackTrace();
			return;
		}
		
		//Insert to the table
		String insertStr = "INSERT INTO " + this.tableName + " (NAME, AGE)" + 
				"VALUES (?,?)";
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		map.put("Tom", 36);
		map.put("Jerry", 32);
		map.put("Tony", 40);
		map.put("Steve", 35);
		try {
			PreparedStatement statement = conn.prepareStatement(insertStr);
			for(String name : map.keySet()) {
				if(hasRecord(conn,name)) {
					continue;
				}
				statement.setString(1, name);
				statement.setInt(2, map.get(name));
				statement.addBatch();
			}
			statement.executeBatch();
			System.out.println("Inserted a table");
		} catch(SQLException e) {
			System.out.println("ERROR: Could not insert into the table");
			e.printStackTrace();
			return;
		}
		
		//Select from the table
		try {
			Statement stmt = conn.createStatement();
			String queryString = 
					"SELECT NAME " +
					"FROM " + this.tableName + " " +
					"WHERE AGE = " +
					"	(SELECT MAX(AGE) " +
					"	FROM " + this.tableName+")";
			System.out.println(queryString);
			ResultSet rs = stmt.executeQuery(queryString);
			System.out.println("Query the table");
			while(rs.next()) {
				String name = rs.getString("NAME");
				System.out.println("Name: " + name);
			}
		} catch(SQLException e) {
			System.out.println("ERROR: Could not query the table");
			e.printStackTrace();
			return;
		}
		
		// Drop the table
		try {
		    String dropString = "DROP TABLE " + this.tableName;
			this.executeUpdate(conn, dropString);
			System.out.println("Dropped the table");
	    } catch (SQLException e) {
			System.out.println("ERROR: Could not drop the table");
			e.printStackTrace();
			return;
		}
	}
	
	private boolean hasRecord(Connection conn, String id) throws SQLException{
		String sql = "Select 1 from "+this.tableName + " where NAME = ?";
		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setString(1, id);
		ResultSet rs = ps.executeQuery();
		return rs.next();
	}
	/**
	 * Connect to the DB and do some stuff
	 */
	public static void main(String[] args) {
		DBDemo app = new DBDemo();
		app.run();
	}
}
