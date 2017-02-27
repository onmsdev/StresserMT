
// Originally from David, OpenNMS, Pradeep Cerner

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.opennms.netmgt.snmp.InetAddrUtils;

public class EventdSyslogStresser {
	private static InetAddress m_agentAddress;
	private static Integer m_trapPort = Integer.valueOf(514);
	private static Double m_syslogRate = Double.valueOf(100); // seconds
	private static Integer m_syslogCount = Integer.valueOf(500);
	private static int m_batchCount = 1;
	private static Integer m_nConcurrentSenders = Integer.valueOf(1);
	private static ExecutorService m_executorService;

	public static void main(String[] args) throws UnknownHostException {

		m_syslogRate = Double.valueOf(args[0]); // seconds
		m_syslogCount = Integer.valueOf(args[1]);
		m_agentAddress = InetAddrUtils.addr(args[2]);
		m_batchCount = Integer.valueOf(args[3]);
		m_nConcurrentSenders = Integer.valueOf(args[4]);

		System.out.println("Number of logs: " + m_syslogCount * m_batchCount);
		System.out.println("To : " + m_agentAddress);
		System.out.println("Number of concurrent senders : " + m_nConcurrentSenders);
		System.out.println("Number of batches : " + m_batchCount);

		System.out.println("Commencing Stress Test....");
		executeStressTest();

	}

	private static void executeStressTest() {
		try {
			stressEventd();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public static void sendConcurrentSyslogs(long startTimeInMillis)
			throws IllegalStateException, InterruptedException, SQLException, ClassNotFoundException {
		m_executorService = Executors.newFixedThreadPool(m_nConcurrentSenders);
		System.out.println(
				"Sending " + m_syslogCount + " syslogs @"+ m_syslogRate.intValue() +" per second per sender with " + m_nConcurrentSenders + " senders...");
		int mySyslogCount[] = new int[m_nConcurrentSenders];
		Set<Callable<Integer>> callables = new HashSet<Callable<Integer>>();
		List<Future<Integer>> futures;
		for (int i = 0; i < m_nConcurrentSenders; ++i) {
			mySyslogCount[i] = (m_syslogCount / m_nConcurrentSenders)
					+ ((((m_syslogCount % m_nConcurrentSenders) / (i + 1)) > 0) ? 1 : 0);
			final int iFinal = mySyslogCount[i];
			callables.add(new Callable<Integer>() {
				public Integer call() throws Exception {
					return Integer.valueOf(sendMySyslogs(iFinal));
				}
			});
		}

		int totalSyslogsSent = 0;
		for (int j = 0; j < m_batchCount; j++) {
			futures = m_executorService.invokeAll(callables);
			for (Future<Integer> future : futures) {
				try {
					int mt = future.get();
					totalSyslogsSent += mt;

				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
		}
		systemReport(startTimeInMillis, totalSyslogsSent);
		m_executorService.shutdown();
	}

	public static int sendMySyslogs(int mySyslogCount)
			throws IllegalStateException, InterruptedException, SQLException, ClassNotFoundException, SocketException {
		int syslogsSent = 0;
		DatagramSocket datagramSocket = new DatagramSocket();
		for (int remaining = mySyslogCount, toSendThisRound = Math.min(m_syslogRate.intValue(),remaining); 
				toSendThisRound > 0; 
				syslogsSent += toSendThisRound, remaining = mySyslogCount - syslogsSent, toSendThisRound = Math.min(m_syslogRate.intValue(), remaining)) {
			long start = Calendar.getInstance().getTimeInMillis();
			sendSyslog(datagramSocket);
			long end = Calendar.getInstance().getTimeInMillis();
			long toSleep = 1000 /* 1 Second, unit of time in rate measurement */ - end + start;
			Thread.sleep(toSleep);
		}
		datagramSocket.close();
		return syslogsSent;
	}

	public static void stressEventd()
			throws ClassNotFoundException, SQLException, IllegalStateException, InterruptedException {
		if (m_batchCount < 1) {
			throw new IllegalArgumentException("Batch count of < 1 is not allowed.");
		} else if (m_batchCount > m_syslogCount) {
			throw new IllegalArgumentException("Batch count is > than syslog count.");
		}

		long startTimeInMillis = Calendar.getInstance().getTimeInMillis();
		sendConcurrentSyslogs(startTimeInMillis);
	}

	private static void systemReport(long beginMillis, int trapsSent) {
		System.out.println("Syslogs sent: " + trapsSent);
		long totalMillis = Calendar.getInstance().getTimeInMillis() - beginMillis;
		Long totalSeconds = totalMillis / 1000L;
		System.out.println("Total Elapsed time (secs): " + totalSeconds);
		System.out.println();
	}

	private static int sendSyslog(DatagramSocket datagramSocket) {
		int syslogsSent = 0;
		try {
			Date myDate = new Date();
			String date = new SimpleDateFormat("yyyy-MM-dd").format(myDate);
			String syslog = "<34> " + date + " 10.181.230.67 foo10000: load test 10000 on abc";
			byte[] bytes = syslog.getBytes();
			DatagramPacket pkt = new DatagramPacket(bytes, bytes.length, m_agentAddress, m_trapPort);
			datagramSocket.send(pkt);
			syslogsSent++;
		} catch (Exception e) {
			throw new IllegalStateException("Caught Exception sending syslog.", e);
		}
		return syslogsSent;
	}

}