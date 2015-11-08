package com.iamks.zookeeper.lock;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.recipes.lock.LockListener;

public class BlockingZKWLock {

	private ZKWLock writeLock;
	private CountDownLatch signal = new CountDownLatch(1);

	public BlockingZKWLock(ZooKeeper zookeeper, String path, List<ACL> acls) {
		this.writeLock = new ZKWLock(zookeeper, path, acls, new SyncLockListener());
	}

	public String getId() {
		return this.writeLock.getId();
	}

	public void lock() throws InterruptedException, KeeperException {
		writeLock.lock();
		signal.await();
	}

	public boolean lock(long timeout, TimeUnit unit) throws InterruptedException, KeeperException {
		writeLock.lock();
		return signal.await(timeout, unit);
	}

	public void unlock() {
		writeLock.unlock();
	}

	class SyncLockListener implements LockListener {
		@Override
		public void lockAcquired() {
			signal.countDown();
		}

		@Override
		public void lockReleased() {
		}
	}

	public void close() {
		writeLock.close();
	}
}
