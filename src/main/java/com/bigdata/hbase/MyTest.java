package com.geek.hbase;

import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

public class MyTest {
    public static void main(String[] args) throws IOException {
        Connection connection = HbaseInit.createConnection();
        // create namespace saliu
        CreateNameSpace.createNamespace(connection, "saliu");
        // create table saliu:student
        CreateTable.create(connection, "saliu:student", "name", "info", "score");
        // insert data to table
        InsertRow.insert(connection, "saliu:student", "stu1", "name", "", "Tom");
        InsertRow.insert(connection, "saliu:student", "stu1", "info", "student_id", "20210000000001");
        InsertRow.insert(connection, "saliu:student", "stu1", "info", "class", "1");
        InsertRow.insert(connection, "saliu:student", "stu1", "score", "understanding", "75");
        InsertRow.insert(connection, "saliu:student", "stu1", "score", "programming", "82");

        InsertRow.insert(connection, "saliu:student", "stu2", "name", "", "Jerry");
        InsertRow.insert(connection, "saliu:student", "stu2", "info", "student_id", "20210000000002");
        InsertRow.insert(connection, "saliu:student", "stu2", "info", "class", "1");
        InsertRow.insert(connection, "saliu:student", "stu2", "score", "understanding", "85");
        InsertRow.insert(connection, "saliu:student", "stu2", "score", "programming", "67");


        InsertRow.insert(connection, "saliu:student", "stu3", "name", "", "Jack");
        InsertRow.insert(connection, "saliu:student", "stu3", "info", "student_id", "20210000000003");
        InsertRow.insert(connection, "saliu:student", "stu3", "info", "class", "2");
        InsertRow.insert(connection, "saliu:student", "stu3", "score", "understanding", "80");
        InsertRow.insert(connection, "saliu:student", "stu3", "score", "programming", "80");

        InsertRow.insert(connection, "saliu:student", "stu4", "name", "", "Rose");
        InsertRow.insert(connection, "saliu:student", "stu4", "info", "student_id", "20210000000004");
        InsertRow.insert(connection, "saliu:student", "stu4", "info", "class", "2");
        InsertRow.insert(connection, "saliu:student", "stu4", "score", "understanding", "60");
        InsertRow.insert(connection, "saliu:student", "stu4", "score", "programming", "61");

        InsertRow.insert(connection, "saliu:student", "stu5", "name", "", "saliu");
        InsertRow.insert(connection, "saliu:student", "stu5", "info", "student_id", "G20220735040058");
        InsertRow.insert(connection, "saliu:student", "stu5", "info", "class", "3");
        InsertRow.insert(connection, "saliu:student", "stu5", "score", "understanding", "100");
        InsertRow.insert(connection, "saliu:student", "stu5", "score", "programming", "100");

        // select table
        SelectTable.scan(connection, "saliu:student");
        // get record by row key
        SelectTable.getRecordByRowKey(connection, "saliu:student", "stu1");

        // delete data by row key
        DeleteRecord.deleteRecord(connection, "saliu:student", "stu1");


    }
}
