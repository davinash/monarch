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
package org.apache.hadoop.hive.ql.io.orc;


public class FTableOrcStruct {
  private OrcStruct orcStruct;

  public FTableOrcStruct(int children) {
    orcStruct = new OrcStruct(children);
  }

  public FTableOrcStruct(OrcStruct orcStruct) {
    this.orcStruct = orcStruct;
  }

  public OrcStruct getOrcStruct() {
    return orcStruct;
  }

  public Object getFieldValue(int fieldIndex) {
    return orcStruct.getFieldValue(fieldIndex);
  }

  public int getNumFields() {
    return orcStruct.getNumFields();
  }



  public void setFieldValue(int fieldIndex, Object value) {
    orcStruct.setFieldValue(fieldIndex, value);
  }

}
