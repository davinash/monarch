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

import java.io.Serializable;
import java.util.Arrays;

public class MonarchCdapKeyObject implements Serializable {
    private byte[] data;

    public MonarchCdapKeyObject(byte[] inputByteArray) {
        if (inputByteArray != null) {
            this.data = Arrays.copyOf(inputByteArray, inputByteArray.length);
        } else {
            this.data = null;
        }
    }

    public byte[] getKeyBytes() {
        return data;
    }

    public int hashCode() {
        return Arrays.hashCode(data);
    }

    public boolean equals(Object other) {
        return other instanceof MonarchCdapKeyObject
                && Arrays.equals(data, ((MonarchCdapKeyObject) other).data);
    }
}
