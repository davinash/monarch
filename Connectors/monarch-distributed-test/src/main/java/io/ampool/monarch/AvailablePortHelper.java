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

package io.ampool.monarch;

import org.apache.geode.internal.AvailablePort;

import java.util.*;

public class AvailablePortHelper {


  /**
   * Returns array of unique randomly available tcp ports of specified count.
   */
  public static int[] getRandomAvailableTCPPorts(int count) {
    List<AvailablePort.Keeper> list = getRandomAvailableTCPPortKeepers(count);
    int[] ports = new int[list.size()];
    int i = 0;
    for (AvailablePort.Keeper k: list) {
      ports[i] = k.getPort();
      k.release();
      i++;
    }
    return ports;
  }
  public static List<AvailablePort.Keeper> getRandomAvailableTCPPortKeepers(int count) {
    List<AvailablePort.Keeper> result = new ArrayList<AvailablePort.Keeper>();
    while (result.size() < count) {
      result.add(AvailablePort.getRandomAvailablePortKeeper(AvailablePort.SOCKET));
    }
    return result;
  }

  /**
   * Returns array of unique randomly available tcp ports of specified count.
   */
  public static int[] getRandomAvailableTCPPortsForDUnitSite(int count) {
    int site = 1;
    String hostName = System.getProperty("hostName");
    if(hostName != null && hostName.startsWith("host") && hostName.length() > 4) {
      site = Integer.parseInt(hostName.substring(4));
    }

    Set set = new HashSet();
    while (set.size() < count) {
      int port = AvailablePort.getRandomAvailablePortWithMod(AvailablePort.SOCKET,site);
      set.add(new Integer(port));
    }
    int[] ports = new int[set.size()];
    int i = 0;
    for (Iterator iter = set.iterator(); iter.hasNext();) {
      ports[i] = ((Integer) iter.next()).intValue();
      i++;
    }
    return ports;
  }


  /**
   * Returns array of unique randomly available tcp ports of specified count.
   */
  public static int getRandomAvailablePortForDUnitSite() {
    int site = 1;
    String hostName = System.getProperty("hostName");
    if(hostName != null && hostName.startsWith("host")) {
      if (hostName.length() > 4) {
        site = Integer.parseInt(hostName.substring(4));
      }
    }
    int port = AvailablePort.getRandomAvailablePortWithMod(AvailablePort.SOCKET,site);
    return port;
  }


  /**
   * Returns randomly available tcp port.
   */
  public static int getRandomAvailableTCPPort() {
    return getRandomAvailableTCPPorts(1)[0];
  }

  /**
   * Returns array of unique randomly available udp ports of specified count.
   */
  public static int[] getRandomAvailableUDPPorts(int count) {
    Set set = new HashSet();
    while (set.size() < count) {
      int port = AvailablePort.getRandomAvailablePort(AvailablePort.MULTICAST);
      set.add(new Integer(port));
    }
    int[] ports = new int[set.size()];
    int i = 0;
    for (Iterator iter = set.iterator(); iter.hasNext();) {
      ports[i] = ((Integer) iter.next()).intValue();
      i++;
    }
    return ports;
  }

  /**
   * Returns randomly available udp port.
   */
  public static int getRandomAvailableUDPPort() {
    return getRandomAvailableUDPPorts(1)[0];
  }


}
