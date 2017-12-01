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

package io.ampool.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.FileInputStream;
import java.io.IOException;

public class HDFSClient {
  public static void main(String[] args) throws IOException {
    System.setProperty("java.security.krb5.conf", args[0]);
    Configuration conf = new Configuration();
    conf.addResource(new FileInputStream("/tmp/testCluster/hdfs.xml"));
    System.out.println("default fs :- " + conf.get("fs.defaultFS"));
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab("hdfs/localhost@EXAMPLE.COM",
        "/tmp/testCluster/kdc/keytabs/hdfs.keytab");

    FileSystem fs = FileSystem.get(conf);

    // Path path = new Path("/hello.txt");
    // FSDataOutputStream fin = fs.create(path);
    // fin.writeUTF("hello");
    // fin.close();
    //
    Path hbaseDir = new Path("/user");
    fs.mkdirs(hbaseDir);
    fs.setOwner(hbaseDir, "hdfs", "supergroup");


    /*
     * RemoteIterator<LocatedFileStatus> fileList = listHDFSFiles(fs, new Path("/user"));
     * System.out.println(fileList.hasNext()); if(fileList!=null){ while (fileList.hasNext()){
     * LocatedFileStatus file = fileList.next();
     * System.out.println("--------------------------------------------------------");
     * System.out.println(file.getPath()); System.out.println(file.getPermission());
     * System.out.println(file.getOwner());
     * System.out.println("--------------------------------------------------------"); } }
     */

    FileStatus[] stats = fs.listStatus(new Path("/"));
    for (FileStatus fst : stats) {
      System.out.println(fst.getPath());
    }
  }

  public static RemoteIterator<LocatedFileStatus> listHDFSFiles(FileSystem fs, Path path)
      throws IOException {
    return fs.listFiles(path, true);
  }
}
