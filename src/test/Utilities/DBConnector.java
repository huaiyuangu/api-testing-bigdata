package Utilities;

import java.sql.*;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import oracle.jdbc.driver.OracleDriver;

import java.sql.Connection;
import java.util.Properties;


public class DBConnector {
    /**
     * HiveServer2 JDBC driver name
     */
    //private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static int lport = 5656;

    private static Session setupSSH(int rport, String rhost){

        String host="106.52.241.248";
        String user="root";
        String password="Tbds@2021!~";
        String dbuserName = "root";
        String dbpassword = "123456";
        //String driverName="com.mysql.cj.jdbc.Driver";
        Session session= null;
        CallableStatement stmt = null;
        try {
            //Set StrictHostKeyChecking property to no to avoid UnknownHostKey issue
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            JSch jsch = new JSch();
            session = jsch.getSession(user, host, 22);
            session.setPassword(password);
            session.setConfig(config);
            session.connect();
            System.out.println("Connected");
            int assinged_port = session.setPortForwardingL(lport, rhost, rport);
            System.out.println("localhost:" + assinged_port + " -> " + rhost + ":" + rport);
            System.out.println("Port Forwarded");
        }catch(Exception e){
            e.printStackTrace();
        }
        return session;
    }

    private static void mysql() throws SQLException {

        String rhost = "172.16.16.29";
        String host = "106.52.241.248";
        int rport = 3306;

        String dbuserName = "root";
        String dbpassword = "123456";
        String driverName = "com.mysql.cj.jdbc.Driver";
        Connection conn = null;
//        Session session= null;
        CallableStatement stmt = null;
        Session session = setupSSH(rport, rhost);

        try {
            String url = "jdbc:mysql://localhost:" + lport + "/coolmeta";
            //mysql database connectivity
            Class.forName(driverName).newInstance();
            //conn = DriverManager.getConnection (url, dbuserName, dbpassword);
            conn = DriverManager.getConnection(url, dbuserName, dbpassword);
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT VERSION();");
            if (rs.next()) {
                System.out.println(rs.getString(1));
            }
            System.out.println("DONE");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null && !conn.isClosed()) {
                System.out.println("Closing Database Connection");
                conn.close();
            }
            if (session != null && session.isConnected()) {
                System.out.println("Closing SSH Connection");
                session.disconnect();
            }
        }
    }

    private static void HiveServer2 ()throws SQLException{
        try {
            Class.forName("driverName");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection conn = DriverManager.getConnection("jdbc:hive2://localhost:10000", "hive", "");
        Statement stmt = conn.createStatement();
        // show tables
        String sql = "SHOW databases";
        System.out.println("Running: " + sql);
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        rs.close();
        String sql2 = "select * from test_db.test_table";
        System.out.println("Running: " + sql2);
        ResultSet rs2 = stmt.executeQuery(sql2);
        ResultSetMetaData rsmd = rs2.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        while (rs2.next()) {
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1)
                    System.out.print(",  ");
                String columnValue = rs2.getString(i);
                System.out.print(rsmd.getColumnName(i) + " " + columnValue);
            }
            System.out.println("");
        }
        rs2.close();
        conn.close();
    }


    private static void oracle() throws SQLException{
        String rhost = "172.16.16.18";
        int rport = 1521;

        String dbuserName = "TEST";
        String dbpassword = "123456";
        Connection conn = null;
        CallableStatement stmt = null;
        Session session = setupSSH(rport, rhost);

        Connection connect = null;
        Statement statement = null;
        ResultSet resultSet = null;

        try {
            Driver driver = new OracleDriver();
            DriverManager.deregisterDriver(driver);
            Properties pro = new Properties();
            pro.put("user", dbuserName);
            pro.put("password", dbpassword);
            connect = driver.connect(String.format("jdbc:oracle:thin:@localhost:%s:orcl",lport), pro);
            System.out.println(connect);
            statement = connect.createStatement();
            resultSet = statement.executeQuery("select table_name from user_tables");
            while (resultSet.next())
            {
                System.out.println(resultSet.getString("table_name"));  //打印输出结果集
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                if (resultSet!=null) resultSet.close();
                if (statement!=null) statement.close();
                if (connect!=null) connect.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            if (session != null && session.isConnected()) {
                System.out.println( "Closing SSH Connection");
                session.disconnect();
            }

        }
    }



    public static void main(String[] args){
        DBConnector conn = new DBConnector();
        try{
            conn.oracle();
        }
        catch(Exception e){
            e.printStackTrace();
        }

    }
}