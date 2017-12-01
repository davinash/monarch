/*
 * Copyright (c) 2017 Ampool, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */

package io.ampool.examples.spark;

import java.io.Serializable;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * An example demonstrating how to save an existing data-frame to Ampool
 * and load an existing Ampool table into Spark as a data-frame.
 * Once an Ampool table is loaded as a data-frame in Spark, one can
 * perform all operations that are supported on a data-frame. It includes,
 * for example, the filter operations, column selection, and aggregation
 * operations. These are now transparent to the user and these are
 * redirected to Ampool wherever required.
 * <p>
 * The options can be provided for writing/loading a data-frame to/from
 * Ampool as a map of Strings (i.e. Map<String,String>):
 *   ampool.locator.host -- the Ampool locator hostanme
 *   ampool.locator.port -- the Ampool locator port number
 *   ampool.batch.size   -- the batch-size to be used during read/write
 *
 *   By Default FTable is used to save an existing data-frame to Ampool and
 *   load an existing Ampool FTable into spark as a data-frame.
 *   Set following options to work with Ampool MTable.
 *
 *   ampool.table.type   -- "ordered" for ORDERED MTable
 *   ampool.table.type   --  "unordered" for UNORDERED MTable.
 *
 *   To work with kerberised ampool cluster, set the following options,
 *   security.enable.kerberos.authc       --  "true" to enable kerberos authentication
 *   security.kerberos.service.principal  --  "Ampool locator service principal"
 *   security.kerberos.user.principal     --  "user principal"
 *   security.kerberos.user.keytab        --  "user keytab"
 *
 * <p>
 * Compile and run the example:
 * <code>
 *   bash$ mvn clean install
 *   bash$ mvn exec:java -Dspark.ampool.jar=<spark-ampool-jar-path> -Dexec.mainClass="io.ampool.examples.spark.SparkExampleML" -Dexec.args="localhost 10334"
 * </code>
 *
 * <p>
 * Created on: 2016-04-18
 * Since version: 0.3.2.0
 */
public class SparkExampleDF {
  /**
   * The sample JavaBean class representing the data-frame. The members will be the
   * respective columns in the data-frame.
   */
  public static final class Employee implements Serializable {
    private String name;
    private Long id;
    private Integer age;
    private Double salary;
    private String department;
    private Date dateOfJoining;

    /** Constructor.. **/
    public Employee(final Object[] values) {
      this.name = (String)values[0];
      this.id = (Long)values[1];
      this.age = (Integer)values[2];
      this.salary = (Double)values[3];
      this.department = (String)values[4];
      this.dateOfJoining = (Date)values[5];
    }

    /** getters and setters **/
    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Long getId() {
      return id;
    }

    public void setId(Long id) {
      this.id = id;
    }

    public Integer getAge() {
      return age;
    }

    public void setAge(Integer age) {
      this.age = age;
    }

    public Double getSalary() {
      return salary;
    }

    public void setSalary(Double salary) {
      this.salary = salary;
    }

    public String getDepartment() {
      return department;
    }

    public void setDepartment(String department) {
      this.department = department;
    }

    public Date getDateOfJoining() {
      return dateOfJoining;
    }

    public void setDateOfJoining(Date dateOfJoining) {
      this.dateOfJoining = dateOfJoining;
    }
  }

  /**
   * sample data
   **/
  private static final List<Employee> SAMPLE_DATA = new ArrayList<Employee>(5) {{
    add(new Employee(new Object[]{"Name_1", 1L, 11, 11.111, "ABC", Date.valueOf("2016-01-01")}));
    add(new Employee(new Object[]{"Name_2", 2L, 22, 22.222, "ABC", Date.valueOf("2016-02-02")}));
    add(new Employee(new Object[]{"Name_3", 3L, 33, 33.333, "PQR", Date.valueOf("2016-03-03")}));
    add(new Employee(new Object[]{"Name_4", 4L, 44, 44.444, "PQR", Date.valueOf("2016-04-04")}));
    add(new Employee(new Object[]{"Name_5", 5L, 55, 55.555, "XYZ", Date.valueOf("2016-05-05")}));
  }};

  /**
   * Main method..
   *
   * @param args the arguments
   */
  public static void main(final String[] args) {
    final String tableName = "SparkExampleDF";

    /** get the locator host/port from arguments, if specified.. **/
    final String locatorHost = args.length > 0 ? args[0] : "localhost";
    final int locatorPort = args.length > 1 ? Integer.valueOf(args[1]) : 10334;

    /** create SparkContext **/
    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkExampleDF");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    /** create data-frame from existing data.. **/
    Dataset df = sqlContext.createDataFrame(jsc.parallelize(SAMPLE_DATA), Employee.class);

    /** print schema of the data-frame **/
    df.printSchema();

    df.show();

    Map<String, String> options = new HashMap<>(3);
    options.put("ampool.locator.host", locatorHost);
    options.put("ampool.locator.port", String.valueOf(locatorPort));

    /** overwrite existing table, if specified.. **/
    SaveMode saveMode = Boolean.getBoolean("overwrite") ? SaveMode.Overwrite : SaveMode.ErrorIfExists;

    /** save the dataFrame to Ampool as `tableName' **/
    df.write().format("io.ampool").options(options).mode(saveMode).save(tableName);

    System.out.println("########## DATA FROM AMPOOL ############");

    /** load the data-frame from Ampool `tableName' **/
    Dataset df1 = sqlContext.read().format("io.ampool").options(options).load(tableName);

    /** show the contents of loaded data-frame **/
    df1.show();

    /** show the total number of rows in data-frame **/
    System.out.println("# NumberOfRowsInDataFrame= " + df1.count());

    /** data-frame with filter **/
    df1.filter("id > 2").show();

    /** data-frame with selected columns **/
    df1.select("name", "id", "department").show();

    df1.registerTempTable("temp_table");

    sqlContext.sql("select * from temp_table order by id").show();
  }
}
