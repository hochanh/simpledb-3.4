package simpleclient;

import simpledb.jdbc.embedded.EmbeddedDriver;

import java.sql.*;

public class CreateStudentDb {
    public static final String[] statements = new String[]{
            "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)",
            "insert into STUDENT(SId, SName, MajorId, GradYear) values (1, 'joe', 10, 2021)",
            "insert into STUDENT(SId, SName, MajorId, GradYear) values  (2, 'amy', 20, 2020)",
            "insert into STUDENT(SId, SName, MajorId, GradYear) values  (3, 'max', 10, 2022)",
            "insert into STUDENT(SId, SName, MajorId, GradYear) values  (4, 'sue', 20, 2022)",
            "insert into STUDENT(SId, SName, MajorId, GradYear) values  (5, 'bob', 30, 2020)",
            "insert into STUDENT(SId, SName, MajorId, GradYear) values  (6, 'kim', 20, 2020)",
            "insert into STUDENT(SId, SName, MajorId, GradYear) values  (7, 'art', 30, 2021)",
            "insert into STUDENT(SId, SName, MajorId, GradYear) values  (8, 'pat', 20, 2019)",
            "insert into STUDENT(SId, SName, MajorId, GradYear) values  (9, 'lee', 10, 2021)",
            "create table DEPT(DId int, DName varchar(8))",
            "insert into DEPT(DId, DName) values (10, 'compsci')",
            "insert into DEPT(DId, DName) values (20, 'math')",
            "insert into DEPT(DId, DName) values (30, 'drama')",
            "create table COURSE(CId int, Title varchar(20), DeptId int)",
            "insert into COURSE(CId, Title, DeptId) values (12, 'db systems', 10)",
            "insert into COURSE(CId, Title, DeptId) values (22, 'compilers', 10)",
            "insert into COURSE(CId, Title, DeptId) values (32, 'calculus', 20)",
            "insert into COURSE(CId, Title, DeptId) values (42, 'algebra', 20)",
            "insert into COURSE(CId, Title, DeptId) values (52, 'acting', 30)",
            "insert into COURSE(CId, Title, DeptId) values (62, 'elocution', 30)",
            "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)",
            "insert into SECTION(SectId, CourseId, Prof, YearOffered) values (13, 12, 'turing', 2018)",
            "insert into SECTION(SectId, CourseId, Prof, YearOffered) values  (23, 12, 'turing', 2019)",
            "insert into SECTION(SectId, CourseId, Prof, YearOffered) values  (33, 32, 'newton', 2019)",
            "insert into SECTION(SectId, CourseId, Prof, YearOffered) values  (43, 32, 'einstein', 2017)",
            "insert into SECTION(SectId, CourseId, Prof, YearOffered) values  (53, 62, 'brando', 2018)",
            "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))",
            "insert into ENROLL(EId, StudentId, SectionId, Grade) values (14, 1, 13, 'A')",
            "insert into ENROLL(EId, StudentId, SectionId, Grade) values  (34, 2, 43, 'B+')",
            "insert into ENROLL(EId, StudentId, SectionId, Grade) values  (24, 1, 43, 'C' )",
            "insert into ENROLL(EId, StudentId, SectionId, Grade) values  (44, 4, 33, 'B' )",
            "insert into ENROLL(EId, StudentId, SectionId, Grade) values  (54, 4, 53, 'A' )",
            "insert into ENROLL(EId, StudentId, SectionId, Grade) values  (64, 6, 53, 'A' )"
    };

    public static void main(String[] args) {
        String s = args.length > 0 ? args[1] : "studentdb";
        Driver d = new EmbeddedDriver();

        try (Connection conn = d.connect(s, null);
             Statement stmt = conn.createStatement()) {
            System.out.println("Init " + s + "...");
            for (String cmd : statements) {
                doUpdate(stmt, cmd);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void doUpdate(Statement stmt, String cmd) {
        try {
            int howmany = stmt.executeUpdate(cmd);
            System.out.println(howmany + " records processed");
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
        }
    }
}