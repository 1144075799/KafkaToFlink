package com.flinkFirst.demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MySQLSink extends RichSinkFunction<Tuple2<String,Integer>> {
    private static final long serialVersionUID = 1L;
    private Connection connection;
    private PreparedStatement preparedStatement;
    String username = "root";
    String password = "root";
    String drivername = "com.mysql.jdbc.Driver";
    String dburl = "jdbc:mysql://localhost:3306/flink";

    @Override
    public void invoke(Tuple2<String,Integer> value) throws Exception {
        Class.forName(drivername);
        connection = DriverManager.getConnection(dburl, username, password);
        String sql = "INSERT into wordcount(name,number) values(?,?)";
        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, value.f0);
        preparedStatement.setInt(2, value.f1);
        preparedStatement.executeUpdate();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }

    }

}
