package com.marcreyesph;

import java.io.IOException;
import java.net.URI;
import java.sql.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.Event.CreateEvent;
import org.apache.hadoop.hdfs.inotify.Event.UnlinkEvent;
import org.apache.hadoop.hdfs.inotify.Event.AppendEvent;
import org.apache.hadoop.hdfs.inotify.Event.CloseEvent;
import org.apache.hadoop.hdfs.inotify.Event.RenameEvent;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;

public class HdfsINotify {

	public static void main(String[] args) throws IOException, InterruptedException, MissingEventsException {

		long lastReadTxid = 0;

		if (args.length > 1) {
			lastReadTxid = Long.parseLong(args[1]);
		}

		System.out.println("lastReadTxid = " + lastReadTxid);

		HdfsAdmin admin = new HdfsAdmin(URI.create(args[0]), new Configuration());

		DFSInotifyEventInputStream eventStream = admin.getInotifyEventStream(lastReadTxid);

		while (true) {
			EventBatch batch = eventStream.take();
			System.out.println("TxId = " + batch.getTxid());

			for (Event event : batch.getEvents()) {
				System.out.println("event type = " + event.getEventType());
				switch (event.getEventType()) {
					case CREATE:
						CreateEvent createEvent = (CreateEvent) event;
						HdfsINotify hdfsINotify = new HdfsINotify();
						FileActivity fileActivity = new FileActivity(
								Long.toString(batch.getTxid()),
								String.valueOf(event.getEventType()),
								createEvent.getPath(),
								createEvent.getOwnerName(),
								Long.toString(createEvent.getCtime()),
								null,
								null,
								null,
								null
						);

						long fileChangeId = hdfsINotify.insertFileActivity(fileActivity);
						System.out.println("New file activity added with fileChangeId: " + Long.toString(fileChangeId));

						System.out.println("\tpath = " + createEvent.getPath());
						System.out.println("\towner = " + createEvent.getOwnerName());
						System.out.println("\tctime = " + createEvent.getCtime());
						break;
					case UNLINK:
						UnlinkEvent unlinkEvent = (UnlinkEvent) event;
						HdfsINotify hdfsINotify1 = new HdfsINotify();
						FileActivity fileActivity1 = new FileActivity(
								Long.toString(batch.getTxid()),
								String.valueOf(event.getEventType()),
								unlinkEvent.getPath(),
								null,
								null,
								Long.toString(unlinkEvent.getTimestamp()),
								null,
								null,
								null
						);

						long fileChangeId1 = hdfsINotify1.insertFileActivity(fileActivity1);
						System.out.println("\tNew file activity added with fileChangeId: " + Long.toString(fileChangeId1));

						System.out.println("\tpath = " + unlinkEvent.getPath());
						System.out.println("\ttimeStamp = " + unlinkEvent.getTimestamp());
						break;
					case APPEND:
						AppendEvent appendEvent = (AppendEvent) event;
						HdfsINotify hdfsINotify2 = new HdfsINotify();
						FileActivity fileActivity2 = new FileActivity(
								Long.toString(batch.getTxid()),
								String.valueOf(event.getEventType()),
								appendEvent.getPath(),
								null,
								null,
								null,
								null,
								null,
								null
						);

						long fileChangeId2 = hdfsINotify2.insertFileActivity(fileActivity2);
						System.out.println("\tNew file activity added with fileChangeId: " + Long.toString(fileChangeId2));

						System.out.println("\tpath = " + appendEvent.getPath());
						System.out.println("\teventType = " + appendEvent.getEventType());
						break;
					case CLOSE:
						CloseEvent closeEvent = (CloseEvent) event;
						HdfsINotify hdfsINotify3 = new HdfsINotify();
						FileActivity fileActivity3 = new FileActivity(
								Long.toString(batch.getTxid()),
								String.valueOf(event.getEventType()),
								closeEvent.getPath(),
								null,
								null,
								Long.toString(closeEvent.getTimestamp()),
								Long.toString(closeEvent.getFileSize()),
								null,
								null
						);

						long fileChangeId3 = hdfsINotify3.insertFileActivity(fileActivity3);
						System.out.println("\tNew file activity added with fileChangeId: " + Long.toString(fileChangeId3));

						System.out.println("\tpath = " + closeEvent.getPath());
						System.out.println("\teventType = " + closeEvent.getEventType());
						System.out.println("\ttimeStamp = " + closeEvent.getTimestamp());
						System.out.println("\tfileSize = " + closeEvent.getFileSize());
						break;
					case RENAME:
						RenameEvent renameEvent = (RenameEvent) event;
						HdfsINotify hdfsINotify4 = new HdfsINotify();
						FileActivity fileActivity4 = new FileActivity(
								Long.toString(batch.getTxid()),
								String.valueOf(event.getEventType()),
								null,
								null,
								null,
								Long.toString(renameEvent.getTimestamp()),
								null,
								renameEvent.getDstPath(),
								renameEvent.getSrcPath()
						);

						long fileChangeId4 = hdfsINotify4.insertFileActivity(fileActivity4);
						System.out.println("\tNew file activity added with fileChangeId: " + Long.toString(fileChangeId4));

						System.out.println("\tsourcePath = " + renameEvent.getDstPath());
						System.out.println("\tdestinationPath = " + renameEvent.getSrcPath());
						System.out.println("\teventType = " + renameEvent.getEventType());
						System.out.println("\ttimeStamp = " + renameEvent.getTimestamp());
						break;
					default:
						HdfsINotify hdfsINotify5 = new HdfsINotify();
						FileActivity fileActivity5 = new FileActivity(
								Long.toString(batch.getTxid()),
								String.valueOf(event.getEventType()),
								null,
								null,
								null,
								null,
								null,
								null,
								null
						);

						long fileChangeId5 = hdfsINotify5.insertFileActivity(fileActivity5);
						System.out.println("\tNew file activity added with fileChangeId: " + Long.toString(fileChangeId5));

						System.out.println("\tNo file changes are being watched in this period.");
						break;
				}
			}
		}
	}

	private final String DB_URL = "dbc:postgresql://ec2-54-243-208-234.compute-1.amazonaws.com:5432/d3m9eobk6mkr7h";
	private final String DB_USER = "tpivhqsbtgcmny";
	private final String DB_PASSWORD = "92cfbdf76ca77493c3fdbfd3b45c457e89b8fd4c102dc2870d994ec85dc580b9";

	/**
	 * Connect to the PostgreSQL database
	 *
	 * @return a Connection object
	 */
	public Connection connect() throws SQLException {
		/*try {
			Class.forName("org.postgresql.Driver").newInstance();
			Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
			return conn;
		} catch(ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		}
		Class.forName("org.postgresql.Driver").newInstance();*/
		Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
		return conn;
	}

	public long insertFileActivity(FileActivity fileActivity) {
		String SQL = "INSERT INTO file_activity(transactionId, " +
				"eventType, " +
				"path, " +
				"ownerName, " +
				"cTime, " +
				"timeStamp, " +
				"fileSize, " +
				"dstPath, " +
				"srcPath) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

		long fileChangeId = 0;

		try (Connection conn = connect();
			 PreparedStatement pstmt = conn.prepareStatement(SQL,
					 Statement.RETURN_GENERATED_KEYS)) {

			pstmt.setString(1, fileActivity.getTransactionId());
			pstmt.setString(2, fileActivity.getEventType());
			pstmt.setString(3, fileActivity.getPath());
			pstmt.setString(4, fileActivity.getOwnerName());
			pstmt.setString(5, fileActivity.getcTime());
			pstmt.setString(6, fileActivity.getTimeStamp());
			pstmt.setString(7, fileActivity.getFileSize());
			pstmt.setString(8, fileActivity.getDstPath());
			pstmt.setString(9, fileActivity.getSrcPath());

			int affectedRows = pstmt.executeUpdate();
			// check the affected rows
			if (affectedRows > 0) {
				// get the ID back
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					if (rs.next()) {
						fileChangeId = rs.getLong(1);
					}
				} catch (SQLException ex) {
					System.out.println(ex.getMessage());
				}
			}
		} catch (SQLException ex) {
			System.out.println(ex.getMessage());
		}
		return fileChangeId;
	}
}