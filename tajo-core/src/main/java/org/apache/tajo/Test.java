package org.apache.tajo;

import com.sun.tools.attach.VirtualMachine;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.File;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by babokim on 2014. 7. 16..
 */
public class Test {
  static final String CONNECTOR_ADDRESS =
      "com.sun.management.jmxremote.localConnectorAddress";

  public static void main(String args[]) throws Exception {
    // attach to target VM to get connector address
    VirtualMachine vm = VirtualMachine.attach(args[0]);
    String connectorAddress = vm.getAgentProperties().getProperty(CONNECTOR_ADDRESS);
    if (connectorAddress == null) {
      // start management agent
      String agent = vm.getSystemProperties().getProperty("java.home") +
          File.separator + "lib" + File.separator + "management-agent.jar";
      vm.loadAgent(agent);
      connectorAddress = vm.getAgentProperties().getProperty(CONNECTOR_ADDRESS);
      assert connectorAddress != null;
    }

    // connect to agent
    JMXServiceURL url = new JMXServiceURL(connectorAddress);
    JMXConnector c = JMXConnectorFactory.connect(url);
    MBeanServerConnection server = c.getMBeanServerConnection();

    // get the list of pools
    Set<ObjectName> mbeans = server.queryNames(
        new ObjectName("java.nio:type=BufferPool,*"), null);
    List<BufferPoolMXBean> pools = new ArrayList<BufferPoolMXBean>();
    for (ObjectName name: mbeans) {
      BufferPoolMXBean pool = ManagementFactory
          .newPlatformMXBeanProxy(server, name.toString(), BufferPoolMXBean.class);
      pools.add(pool);
    }

    // print headers
    for (BufferPoolMXBean pool: pools)
      System.out.format("         %8s             ", pool.getName());
    System.out.println();
    for (int i=0; i<pools.size(); i++)
      System.out.format("%6s %10s %10s  ",  "Count", "Capacity", "Memory");
    System.out.println();

    // poll and print usage
    for (;;) {
      for (BufferPoolMXBean pool: pools) {
        System.out.format("%6d %10d %10d  ",
            pool.getCount(), pool.getTotalCapacity(), pool.getMemoryUsed());
      }
      System.out.println();
      Thread.sleep(2000);
    }
  }
}
