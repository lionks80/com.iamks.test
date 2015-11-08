package com.iamks.zookeeper.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.recipes.lock.LockListener;
import org.apache.zookeeper.recipes.lock.ZooKeeperOperation;

import java.util.List;

public class ZKWLock extends ProtocolSupport {

	private static final Logger LOG = LoggerFactory.getLogger(ZKWLock.class);

	private final String dir;
	private final String prefix = "zkwl-";
	private String id;
	private ZNodeName idName;
	private String ownerId;
	private String lastChildId;
	private byte[] data = { 0x12, 0x34 };
	private LockListener callback;
	private ZooKeeperOperation zop;

	public ZKWLock(ZooKeeper zookeeper, String dir, List<ACL> acl) {
		super(zookeeper);
		this.dir = dir;
		if (acl != null) {
			setAcl(acl);
		}
		zop = new ZKWLockOperation(this);
	}

	public ZKWLock(ZooKeeper zookeeper, String dir, List<ACL> acl, LockListener callback) {
		this(zookeeper, dir, acl);
		this.callback = callback;
	}

	public synchronized boolean lock() throws KeeperException, InterruptedException {
		if (isClosed()) {
			return false;
		}
		ensurePathExists(dir);

		return (Boolean) retryOperation(zop);
	}

	public synchronized void unlock() throws RuntimeException {

		if (!isClosed() && id != null) {
			// we don't need to retry this operation in the case of failure
			// as ZK will remove ephemeral files and we don't wanna hang
			// this process when closing if we cannot reconnect to ZK
			try {

				ZooKeeperOperation zopdel = new ZooKeeperOperation() {
					public boolean execute() throws KeeperException, InterruptedException {
						zookeeper.delete(id, -1);
						return Boolean.TRUE;
					}
				};
				zopdel.execute();
			} catch (InterruptedException e) {
				LOG.warn("Caught: " + e, e);
				// set that we have been interrupted.
				Thread.currentThread().interrupt();
			} catch (NoNodeException e) {
				System.out.println("NoNodeException" + e.getMessage());
				// do nothing
			} catch (KeeperException e) {
				LOG.warn("Caught: " + e, e);
				throw (RuntimeException) new RuntimeException(e.getMessage()).initCause(e);
			} catch (Exception e) {
				System.out.println("Exception" + e.getMessage());
			} finally {
				if (callback != null) {
					callback.lockReleased();
				}
				id = null;
			}
		}
	}

	public boolean isOwner() {
		return id != null && ownerId != null && id.equals(ownerId);
	}

	public String getDir() {
		return dir;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public ZNodeName getIdName() {
		return idName;
	}

	public void setIdName(ZNodeName idName) {
		this.idName = idName;
	}

	public String getOwnerId() {
		return ownerId;
	}

	public void setOwnerId(String ownerId) {
		this.ownerId = ownerId;
	}

	public String getLastChildId() {
		return lastChildId;
	}

	public void setLastChildId(String lastChildId) {
		this.lastChildId = lastChildId;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public LockListener getCallback() {
		return callback;
	}

	public void setCallback(LockListener callback) {
		this.callback = callback;
	}

	public String getPrefix() {
		return prefix;
	}

}
