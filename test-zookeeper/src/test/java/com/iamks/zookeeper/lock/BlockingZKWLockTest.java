package com.iamks.zookeeper.lock;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;
import com.nearinfinity.examples.zookeeper.util.EmbeddedZooKeeperServer;

public class BlockingZKWLockTest {

	private static EmbeddedZooKeeperServer _embeddedServer;
	private ZooKeeper _zooKeeper;
	private String _testLockPath;

	private static final int ZK_PORT = 53181;
	private static final String ZK_CONNECTION_STRING = "localhost:" + ZK_PORT;

	private static final int testSize = 1000;
	CountDownLatch startLatch;
	CountDownLatch endLatch;
	int number = 0;

	@BeforeClass
	public static void beforeAll() throws IOException, InterruptedException {
		_embeddedServer = new EmbeddedZooKeeperServer(ZK_PORT);
		_embeddedServer.start();
	}

	@AfterClass
	public static void afterAll() {
		_embeddedServer.shutdown();
	}

	@Before
	public void setUp() throws IOException, InterruptedException {
		_zooKeeper = new ConnectionHelper().connect(ZK_CONNECTION_STRING);
		_testLockPath = "/test-write-lock-" + System.currentTimeMillis();
	}

	@After
	public void tearDown() throws InterruptedException, KeeperException {
		if (_zooKeeper.exists(_testLockPath, false) == null) {
			return;
		}

		List<String> children = _zooKeeper.getChildren(_testLockPath, false);
		for (String child : children) {
			_zooKeeper.delete(_testLockPath + "/" + child, -1);
		}
		_zooKeeper.delete(_testLockPath, -1);
	}

	@Test
	public void test() throws IOException, InterruptedException {

		_zooKeeper = new ConnectionHelper().connect(ZK_CONNECTION_STRING, 50000);

		startLatch = new CountDownLatch(testSize);
		endLatch = new CountDownLatch(testSize);

		ExecutorService exeService = Executors.newFixedThreadPool(testSize);

		for (int i = 0; i < testSize; i++) {
			exeService.execute(new TestThread());
		}

		startLatch.await();
		endLatch.await();

		System.out.println("====================================");
		System.out.println("number: " + number);

		Thread.sleep(2000);

	}

	public class TestThread implements Runnable {

		@Override
		public void run() {

			startLatch.countDown();

			try {
				startLatch.await();
			} catch (InterruptedException e) {
			}

			BlockingZKWLock lock = new BlockingZKWLock(_zooKeeper, _testLockPath, null);

			try {
				lock.lock();
				System.out.println(lock.getId());
				int temp = number;
				try {
					Thread.sleep(25);
				} catch (InterruptedException e) {
				}
				number = temp + 1;
				System.out.println("Current: " + number);
			} catch (KeeperException e1) {
				e1.printStackTrace();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			} finally {
				lock.unlock();
				lock.close();
			}

			endLatch.countDown();

		}

	}

}
