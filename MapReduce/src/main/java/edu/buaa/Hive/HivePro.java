package edu.buaa.Hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import javax.net.ssl.SSLContext;

import java.sql.DriverManager;

public class HivePro {
	private static String driverName =
             "org.apache.hive.jdbc.HiveDriver";
 
	public static void main(String[] args) throws Throwable {
		
		
		
		Class.forName(driverName);
		Connection con = DriverManager.getConnection(
                "jdbc:hive2://parallels:10000/hive", "root", "chen2015");
		Statement stmt = con.createStatement();
		String querySQL = "show create table default.stu";
//		querySQL = "show tables";
		
		
//		String querySQL = "desc default.stu";
		
		//String querySQL = "select * from default.stu";
		//querySQL = "select * from default.TBLS";
		ResultSet res = stmt.executeQuery(querySQL);
 
		while(res.next()) {
//			System.out.println(res.getFetchSize());
		
			System.out.println(res.getString(1));
		}
		
//		while(res.next()) {
//			System.out.println(res.getInt(1));
//			System.out.println(res.getString(1));
//			System.out.println(res.toString());
//		}
 

//		res.close();
//		stmt.close();
//		con.close();
	}
}
