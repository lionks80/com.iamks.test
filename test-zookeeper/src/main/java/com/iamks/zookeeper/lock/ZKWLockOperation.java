package com.iamks.zookeeper.lock;

import static org.apache.zookeeper.CreateMode.EPHEMERAL_SEQUENTIAL;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.recipes.lock.ZooKeeperOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKWLockOperation implements ZooKeeperOperation {

	private static final Logger LOG = LoggerFactory.getLogger(ZKWLockOperation.class);

	private int createNodeRetryCnt = 10;
	ZKWLock zkwl;

	public ZKWLockOperation(ZKWLock zkwl) {
		this.zkwl = zkwl;
	}

	@Override
	public boolean execute() throws KeeperException, InterruptedException {

		if (zkwl.getId() == null) {
			this.createNode(zkwl.getPrefix(), zkwl.getZookeeper(), zkwl.getDir());
		}

		return doWork();

	}

	private void createNode(String prefix, ZooKeeper zookeeper, String dir) {

		for (int i = 0; i < this.createNodeRetryCnt; i++) {
			try {
				String nodeId = zookeeper.create(dir + "/" + prefix, zkwl.getData(), zkwl.getAcl(),
						EPHEMERAL_SEQUENTIAL);
				zkwl.setId(nodeId);
				zkwl.setIdName(new ZNodeName(nodeId));
				return;
			} catch (KeeperException e) {
			} catch (InterruptedException e) {
			}
		}

		throw new RuntimeException("ZKWLockOperation - createNode Error: " + this.createNodeRetryCnt + " time");

	}

	private boolean doWork() throws KeeperException, InterruptedException {

		List<String> names = zkwl.getZookeeper().getChildren(zkwl.getDir(), false);
		if (names.isEmpty()) {
			LOG.warn("No children in: " + zkwl.getDir() + " when we've just " + "created one! Lets recreate it...");
			return this.execute();
		}

		SortedSet<ZNodeName> sortedNames = new TreeSet<ZNodeName>();
		for (String name : names) {
			sortedNames.add(new ZNodeName(zkwl.getDir() + "/" + name));
		}
		String ownerId = sortedNames.first().getName();
		zkwl.setOwnerId(ownerId);

		// 현재 자신이 owner 일 경우
		if (zkwl.isOwner() == true) {
			if (zkwl.getCallback() != null) {
				zkwl.getCallback().lockAcquired();
			}
			return true;
		}

		SortedSet<ZNodeName> lessThanMe = sortedNames.headSet(zkwl.getIdName());

		if (lessThanMe.isEmpty()) {
			throw new RuntimeException("Could not find the stats for less than me");
		}

		ZNodeName lastChildName = lessThanMe.last();
		String lastChildId = lastChildName.getName();
		zkwl.setLastChildId(lastChildId);
		if (LOG.isDebugEnabled()) {
			LOG.debug("watching less than me node: " + lastChildId);
		}

		Stat lastChildIdStat = zkwl.getZookeeper().exists(lastChildId, new LockWatcher());
		if (lastChildIdStat != null) {
			// 정상 등록된 경우 현재 대기 상태임으로 false를 리턴한다.
			return false;
		} else {
			// 혹시 노드가 그 사이 사라지거나 하는 경우가 있으며 이런 경우 다시 한번 doWork를 실행한다.
			return doWork();
		}

	}

	class LockWatcher implements Watcher {
		public void process(WatchedEvent event) {
			// lets either become the leader or watch the new/updated node
			LOG.debug("Watcher fired on path: " + event.getPath() + " state: " + event.getState() + " type "
					+ event.getType());

			try {
				zkwl.lock();
			} catch (Exception e) {
				LOG.warn("Failed to acquire lock: " + e, e);
				System.out.println("Failed to acquire lock");
			}
		}
	}

}
