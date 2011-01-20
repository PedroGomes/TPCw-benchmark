/*
 * *********************************************************************
 * Copyright (c) 2010 Pedro Gomes and Universidade do Minho.
 * All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************
 */

package org.uminho.gsd.benchmarks.TPCW_MySQL.executor;

import java.sql.*;
import java.util.Map;

public class DatabaseCreation {

    static Connection con;

    public DatabaseCreation() throws Exception {

        //Register the JDBC driver for MySQL.
        Class.forName("com.mysql.jdbc.Driver");
        String url = "jdbc:mysql://192.168.82.20:3306/TPCW";
 //     String url = "jdbc:mysql://192.168.82.20:3306/TPCW";
        con = DriverManager.getConnection(url, "root", "");

        con.setAutoCommit(false);

    }

    public void createTables() {
        int i;

        try {
            PreparedStatement statement = con.prepareStatement
                    ("CREATE TABLE ADDRESS ( addr_id int not null, addr_street1 varchar(40), addr_street2 varchar(40), addr_city varchar(30), addr_state varchar(20), addr_zip varchar(10), addr_co_id int, primary key(addr_id)) engine=innodb") ;
            // ("CREATE TABLE ADDRESS ( ADDR_ID VARCHAR(10) not null, ADDR_STREET1 varchar(40), ADDR_STREET2 varchar(40), ADDR_CITY varchar(30), ADDR_STATE varchar(20), ADDR_ZIP varchar(10), ADDR_CO_ID int, PRIMARY KEY(ADDR_ID))");

            statement.executeUpdate();
            con.commit();
            System.out.println("Created table ADDRESS");
        } catch (java.lang.Exception ex) {
            System.out.println("Unable to create table: ADDRESS");
            ex.printStackTrace();
            System.exit(1);
        }

        try {
            PreparedStatement statement = con.prepareStatement
                    ("CREATE TABLE AUTHOR ( a_id int not null, a_fname varchar(20), a_lname varchar(20), a_mname varchar(20), a_dob date, a_bio varchar(500), primary key(a_id)) ENGINE=innodb");


            statement.executeUpdate();
            con.commit();
            System.out.println("Created table AUTHOR");
        } catch (java.lang.Exception ex) {
            System.out.println("Unable to create table: AUTHOR");
            ex.printStackTrace();
            System.exit(1);
        }

        try {
            PreparedStatement statement = con.prepareStatement
                    ("CREATE TABLE CC_XACTS ( cx_o_id int not null, cx_type varchar(10), cx_num varchar(20), cx_name varchar(30), cx_expire date, cx_auth_id char(15), cx_xact_amt double, cx_xact_date date, cx_co_id int, primary key(cx_o_id)) ENGINE=innodb");

            statement.executeUpdate();
            con.commit();
            System.out.println("Created table CC_XACTS");
        } catch (java.lang.Exception ex) {
            System.out.println("Unable to create table: CC_XACTS");
            ex.printStackTrace();
            System.exit(1);
        }

        try {
            PreparedStatement statement = con.prepareStatement
                    ("CREATE TABLE COUNTRY ( co_id int not null, co_name varchar(50), co_exchange double, co_currency varchar(18), primary key(co_id)) ENGINE=innodb");

            statement.executeUpdate();
            con.commit();
            System.out.println("Created table COUNTRY");
        } catch (java.lang.Exception ex) {
            System.out.println("Unable to create table: COUNTRY");
            ex.printStackTrace();
            System.exit(1);
        }
        try {
            PreparedStatement statement = con.prepareStatement
                    ("CREATE TABLE CUSTOMER ( c_id int not null, c_uname varchar(20), c_passwd varchar(20), c_fname varchar(17), c_lname varchar(17), c_addr_id int, c_phone varchar(18), c_email varchar(50), c_since date, c_last_visit date, c_login timestamp, c_expiration timestamp, c_discount real, c_balance double, c_ytd_pmt double, c_birthdate date, c_data varchar(510), primary key(c_id)) ENGINE=innodb");
//            ("CREATE TABLE CUSTOMER ( C_ID VARCHAR(10) not null, C_UNAME varchar(20), C_PASSWD varchar(20), C_FNAME varchar(17), C_LNAME varchar(17), C_ADDR_ID VARCHAR(10), C_PHONE varchar(18), C_EMAIL varchar(50), C_SINCE date, C_LAST_VISIT date, C_LOGIN timestamp, C_EXPIRATION timestamp, C_DISCOUNT real, C_BALANCE double, C_YTD_PMT double, C_BIRTHDATE date, C_DATA varchar(510), PRIMARY KEY(C_ID))");
            statement.executeUpdate();
            con.commit();
            System.out.println("Created table CUSTOMER");
        } catch (java.lang.Exception ex) {
            System.out.println("Unable to create table: CUSTOMER");
            ex.printStackTrace();
            System.exit(1);
        }

        try {
            PreparedStatement statement = con.prepareStatement
                    ("CREATE TABLE ITEM ( i_id int not null, i_title varchar(60), i_a_id int, i_pub_date date, i_publisher varchar(60), i_subject varchar(60), i_desc varchar(500), i_related1 int, i_related2 int, i_related3 int, i_related4 int, i_related5 int, i_thumbnail varchar(40), i_image varchar(40), i_srp double, i_cost double, i_avail date, i_stock int, i_isbn char(13), i_page int, i_backing varchar(15), i_dimension varchar(25), primary key(i_id)) ENGINE=innodb");
            statement.executeUpdate();
            con.commit();
            System.out.println("Created table ITEM");
        } catch (java.lang.Exception ex) {
            System.out.println("Unable to create table: ITEM");
            ex.printStackTrace();
            System.exit(1);
        }

        try {
            PreparedStatement statement = con.prepareStatement
                    ("CREATE TABLE ORDER_LINE ( ol_id int not null, ol_o_id int not null, ol_i_id int, ol_qty int, ol_discount double, ol_comments varchar(110), primary key(ol_id, ol_o_id)) ENGINE=innodb");
            statement.executeUpdate();
            con.commit();
            System.out.println("Created table ORDER_LINE");
        } catch (java.lang.Exception ex) {
            System.out.println("Unable to create table: ORDER_LINE");
            ex.printStackTrace();
            System.exit(1);
        }

        try {
            PreparedStatement statement = con.prepareStatement
                    ("CREATE TABLE ORDERS ( o_id int not null, o_c_id int, o_date date, o_sub_total double, o_tax double, o_total double, o_ship_type varchar(10), o_ship_date date, o_bill_addr_id int, o_ship_addr_id int, o_status varchar(15), primary key(o_id)) ENGINE=innodb");
            //         ("CREATE TABLE ORDERS ( O_ID varchar(10) not null, O_C_ID varchar(10), O_DATE date, O_SUB_TOTAL double, O_TAX double, O_TOTAL double, O_SHIP_TYPE varchar(10), O_SHIP_DATE date, O_BILL_ADDR_ID varchar(10), O_SHIP_ADDR_ID varchar(10), O_STATUS varchar(15), PRIMARY KEY(O_ID))");
            statement.executeUpdate();
            con.commit();
            System.out.println("Created table ORDERS");
        } catch (java.lang.Exception ex) {
            System.out.println("Unable to create table: ORDERS");
            ex.printStackTrace();
            System.exit(1);
        }


        try {
            PreparedStatement statement = con.prepareStatement
                    ("CREATE TABLE SHOPPING_CART ( sc_id int not null, sc_time timestamp, sc_sub_total float default null, sc_tax float default 0 , sc_ship_cost float default 0 , sc_total float default 0 , primary key(sc_id)) ENGINE=innodb");
            statement.executeUpdate();
            con.commit();
            System.out.println("Created table SHOPPING_CART");
        } catch (java.lang.Exception ex) {
            System.out.println("Unable to create table: SHOPPING_CART");
            ex.printStackTrace();
            System.exit(1);
        }
        try {
            PreparedStatement statement = con.prepareStatement
("CREATE TABLE SHOPPING_CART_LINE ( scl_sc_id int not null, scl_qty int, scl_i_id int not null,scl_cost float default 0 , scl_srp double default 0 ,  scl_title mediumtext , scl_backing mediumtext , primary key(scl_sc_id, scl_i_id)) ENGINE=innodb");
            statement.executeUpdate();
            con.commit();
            System.out.println("Created table SHOPPING_CART_LINE");
        } catch (java.lang.Exception ex) {
            System.out.println("Unable to create table: SHOPPING_CART_LINE");
            ex.printStackTrace();
            System.exit(1);
        }


        try {
            PreparedStatement statement = con.prepareStatement
                    ("CREATE TABLE RESULTS ( client_id int not null, item_id int not null, stock int not null, bought int not null, primary key( item_id,client_id)) engine=innodb") ;
            statement.executeUpdate();
            con.commit();
            System.out.println("Created table RESULTS");
        } catch (java.lang.Exception ex) {
            System.out.println("Unable to create table: RESULTS");
            ex.printStackTrace();
            System.exit(1);
        }


        System.out.println("Done creating tables!");

    }

    public void test() {

        Object result = null;
        try {
            PreparedStatement statement = con.prepareStatement(" SELECT * FROM ITEM");
            //logger.debug("[READ STATEMENT]:" + statement.toString());

            ResultSet set = statement.executeQuery();
            Map<String, Class<?>> classes = set.getStatement().getConnection().getTypeMap();


            if (set.next()) {
                result = set.getObject(1);

                java.sql.ResultSetMetaData metaData = set.getMetaData();
                System.out.println("CC:" + metaData.getColumnCount());
                for (int i = 1; i < metaData.getColumnCount(); i++) {
                    System.out.println("CN:" + metaData.getColumnName(i));


                }
            }

            statement.close();

            System.out.println("CL:" + result.getClass().getName());
            System.out.println("C:" + classes.toString());

        } catch (SQLException e) {
            e.printStackTrace();
            // logger.error("[READ]: ",e);
        }
    }


    public static void main(String[] args) {


        try {
            DatabaseCreation dbc = new DatabaseCreation();
           // dbc.test();
              dbc.createTables();

        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

}
