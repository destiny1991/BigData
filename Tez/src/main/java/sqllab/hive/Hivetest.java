package sqllab.hive;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class Hivetest {
	 private static String driverName = 
             "org.apache.hive.jdbc.HiveDriver";
	
	public static void main(String[] args) throws Throwable {
        Class.forName(driverName);
		Connection con = DriverManager.getConnection(
                "jdbc:hive2://master:10000/default", "", "");
		Statement stmt = con.createStatement();
		String querySQL = "select * from default.stu";
		ResultSet res = stmt.executeQuery(querySQL);
		
		while(res.next()) {
			System.out.println(res.getInt(1));
		}
		
		res.close();
		stmt.close();
		con.close();
	}

}