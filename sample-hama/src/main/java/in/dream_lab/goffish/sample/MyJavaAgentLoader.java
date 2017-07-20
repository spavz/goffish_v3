package in.dream_lab.goffish.sample;

import com.sun.tools.attach.VirtualMachine;

import java.lang.management.ManagementFactory;

/**
 * Created by vis on 20/7/17.
 */
public class MyJavaAgentLoader {

    private static final String jarFilePath = "/home/vis/Documents/hama-0.7.1/lib/goffish-sample-3.1.jar";

    public static void loadAgent() {
        String nameOfRunningVM = ManagementFactory.getRuntimeMXBean().getName();
        int p = nameOfRunningVM.indexOf('@');
        String pid = nameOfRunningVM.substring(0, p);

        try {
            VirtualMachine vm = VirtualMachine.attach(pid);
            vm.loadAgent(jarFilePath, "");
            vm.detach();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
