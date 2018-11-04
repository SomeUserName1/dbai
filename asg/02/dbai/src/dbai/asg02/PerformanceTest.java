package dbai.asg02;

import java.lang.management.*;

public class PerformanceTest {
	public static void main(String[] args) {
		ThreadMXBean bean = ManagementFactory.getThreadMXBean();
		if(!bean.isCurrentThreadCpuTimeSupported()) {System.out.println("Not supported"); return;}

		for (int i = 0; i < 100; i++) {
			final long time = System.nanoTime();//bean.getCurrentThreadCpuTime();
            double sum = 0;
			for (int j = 0; j < 100_000_000; j++) {
				sum += Math.sqrt(j);
			}
			System.out.println(System.nanoTime() - time);//bean.getCurrentThreadCpuTime() - time);
		}
	}
}


