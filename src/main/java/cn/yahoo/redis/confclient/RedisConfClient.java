package cn.yahoo.redis.confclient;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;

public class RedisConfClient {
	private static Logger log = LoggerFactory.getLogger(RedisConfClient.class);
	private static String[] groups = null;// groups
	private static String zookeeperConnectionString = "";// zk����
	private static String app = "";// appname
	private static String groupPath = "/redis/";
	private static String filename = "";// �����ļ���ַ
	private static CuratorFramework client = null;
	private static RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,
			3);
	private static Watcher appwatcher = new Watcher() {
		// watcher
		public void process(WatchedEvent event) {
			System.out.println("event:" + event.getType() + " path:"
					+ event.getPath());
			// �����۲�
			groups = addAppWatcher();
			// ���¹�ע groups
			writeConf();
		}
	};
	private static List<Watcher> groupsWatchers = null;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args == null || args.length != 4) {
			log.error("��������");
			System.exit(0);
		}
		zookeeperConnectionString = args[0];
		app = args[1];
		PropertyConfigurator.configure(args[2]);
		filename = args[3];
		// 1.��ʼ��
		log.info("��ʼ�����ڵ�...");
		client = init();
		if (client == null) {
			log.error("zk���Ӵ�������");
			System.exit(0);
		}
		log.info("�����ڵ����,��ʼ��ȡ��ǰ״̬...");
		// 2. get��watch app�ڵ�
		groups = addAppWatcher();
		log.info("�õ���ǰgroups����ʼwatch��д����...");
		// 3.get ��дconf��Ȼ��watch
		writeConf();
		log.info("д�������");
	}

	/**
	 * 1.��ʼ��
	 *
	 * @return
	 */
	private static CuratorFramework init() {
		try {
			client = CuratorFrameworkFactory.newClient(
					zookeeperConnectionString, retryPolicy);
			client.start();
			return client;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 2. get��watch app�ڵ�
	 *
	 * @return
	 */
	private static String[] addAppWatcher() {
		try {
			String group = new String(client.getData().forPath("/apps/" + app));
			if (group != null) {
				client.getData().usingWatcher(appwatcher)
						.forPath("/apps/" + app);
				groupsWatchers = null;
				return group.split(",");
			}
		} catch (Exception e) {
		}
		return null;
	}

	/**
	 * 3.get ��дconf��Ȼ��watch
	 */
	private static void writeConf() {
		StringBuilder sb = new StringBuilder();
		String host = "";
		if (groups != null && groups.length > 0) {
			groupsWatchers = new ArrayList<Watcher>();
			sb.append("<?php\r\n");
			for (String g : groups) {
				try {
					List<String> it = client.getChildren().forPath(
							groupPath + g);
					Collections.sort(it);
					for (int i = 0; i < it.size(); i++) {
						host = new String(client.getData().forPath(
								groupPath + g + "/" + it.get(i)));
						if (i == 0) {// master
							sb.append("$redis['" + g + "_master']['host'] = \""
									+ host + "\";\r\n");
						} else {
							sb.append("$redis['" + g + "_slave" + i
									+ "']['host'] = \"" + host + "\";\r\n");
						}
					}
					// watch
					groupsWatchers.add(newGroupWatcher(groupPath + g));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			sb.append("?>");
			try {
				FileWriter fw = new FileWriter(filename);
				fw.write(sb.toString());
				fw.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * �µĶ�group��watcher
	 *
	 * @param p
	 * @return
	 */
	private static Watcher newGroupWatcher(String p) {
		Watcher gwatcher = new Watcher() {
			// watcher
			public void process(WatchedEvent event) {
				System.out.println("event:" + event.getType() + " path:"
						+ event.getPath());
				// �����۲�
				groups = addAppWatcher();
				// ���¹�ע groups
				// ���¹�ע groups
				writeConf();
			}
		};
		try {
			client.getChildren().usingWatcher(gwatcher).forPath(p);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return gwatcher;
	}
}
