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

package io.ampool.monarch.common;

public final class Version implements  Comparable<Version>{
  private int MajorVersion;
  private int MinorVersion;
  private int PatchVersion;
  private int BuildNumber;

  private Version(){
    this.MajorVersion = 0;
    this.MinorVersion = 0;
    this.PatchVersion = 0;
    this.BuildNumber = 0;
  }


  private Version(int MajorVersion, int MinorVersion, int PatchVersion, int BuildNumber){
    this.MajorVersion = MajorVersion;
    this.MinorVersion = MinorVersion;
    this.PatchVersion = PatchVersion;
    this.BuildNumber = BuildNumber;
  }

  public static final String ALPHA_VERSION = new Version(0,1,0,0).toString();

  @Override
  public int compareTo(Version other) {
    //check major version
    if(this.MajorVersion > other.MajorVersion){
      return 1;
    }else if(other.MajorVersion > this.MajorVersion){
      return -1;
    }

    //check minor version
    if(this.MinorVersion > other.MinorVersion){
      return 1;
    }else if(other.MinorVersion > this.MinorVersion){
      return -1;
    }

    //Check patch version
    if(this.PatchVersion > other.PatchVersion){
      return 1;
    }else if(other.PatchVersion > this.PatchVersion){
      return -1;
    }

    //check build number
    if(this.BuildNumber > other.BuildNumber){
      return 1;
    }else if(other.BuildNumber > this.BuildNumber){
      return -1;
    }
    return 0;
  }

  public String toString(){
    return this.MajorVersion + "." + this.MinorVersion + "." + this.PatchVersion + "." + this.BuildNumber;
  }
}
