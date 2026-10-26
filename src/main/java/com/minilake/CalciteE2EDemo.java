
package com.minilake;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.adapter.java.ReflectiveSchema;

import java.sql.*;
import java.util.Properties;

public class CalciteE2EDemo {

    public static class Employee {
        public final int id;
        public final String name;
        public final int salary;

        public Employee(int id, String name, int salary) {
            this.id = id;
            this.name = name;
            this.salary = salary;
        }
    }

    public static class EmployeeSchema {
        public final Employee[] employees = {
                new Employee(1, "Alice", 7000),
                new Employee(2, "Bob", 4000),
                new Employee(3, "Charlie", 9000)
        };
    }

    public static void main(String[] args)
            throws Exception {

        Class.forName("org.apache.calcite.jdbc.Driver");

        Properties properties = new Properties();
        properties.setProperty("caseSensitive", "false");

        try (Connection connection =
                     DriverManager.getConnection(
                             "jdbc:calcite:", properties)) {

            CalciteConnection calcite =
                    connection.unwrap(CalciteConnection.class);

            calcite.getRootSchema().add(
                    "APP",
                    new ReflectiveSchema(new EmployeeSchema())
            );

            calcite.setSchema("APP");

            String sql = "SELECT name, salary FROM employees WHERE salary > 5000 ORDER BY salary DESC";
            String descSql = "describe table APP.employees" ;

            try (Statement statement =
                         calcite.createStatement();
                 ResultSet rs =
                         statement.executeQuery(sql)) {

                while (rs.next()) {
                    System.out.println(
                            rs.getString(1) + " : "
                                    + rs.getInt(2)
                    );
                }
            }


            DatabaseMetaData metadata = connection.getMetaData();

            try (ResultSet columns =
                         metadata.getColumns(
                                 null, "APP", "employees", null)) {

                while (columns.next()) {
                    System.out.printf(
                            "%s | %s | %s%n",
                            columns.getString("COLUMN_NAME"),
                            columns.getString("TYPE_NAME"),
                            columns.getString("IS_NULLABLE")
                    );
                }
            }


        }
    }
}

