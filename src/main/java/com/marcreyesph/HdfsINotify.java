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
						System.out.println("  path = " + createEvent.getPath());
						System.out.println("  owner = " + createEvent.getOwnerName());
						System.out.println("  ctime = " + createEvent.getCtime());
						break;
					case UNLINK:
						UnlinkEvent unlinkEvent = (UnlinkEvent) event;
						System.out.println("  path = " + unlinkEvent.getPath());
						System.out.println("  timeStamp = " + unlinkEvent.getTimestamp());
						break;
					case APPEND:
						AppendEvent appendEvent = (AppendEvent) event;
						System.out.println(" path = " + appendEvent.getPath());
						System.out.println(" eventType = " + appendEvent.getEventType());
						break;
					case CLOSE:
						CloseEvent closeEvent = (CloseEvent) event;
						System.out.println(" path = " + closeEvent.getPath());
						System.out.println(" eventType = " + closeEvent.getEventType());
						System.out.println(" timeStamp = " + closeEvent.getTimestamp());
						System.out.println(" fileSize = " + closeEvent.getFileSize());
						break;
					case RENAME:
						RenameEvent renameEvent = (RenameEvent) event;
						System.out.println(" sourcePath = " + renameEvent.getDstPath());
						System.out.println(" destinationPath = " + renameEvent.getSrcPath());
						System.out.println(" eventType = " + renameEvent.getEventType());
						System.out.println(" timeStamp = " + renameEvent.getTimestamp());
						break;
					default:
						System.out.println(" No file changes are being watched in this period.");
						break;
				}
			}
		}
	}

	private final String DB_URL = "postgres://tpivhqsbtgcmny:92cfbdf76ca77493c3fdbfd3b45c457e89b8fd4c102dc2870d994ec85dc580b9@ec2-54-243-208-234.compute-1.amazonaws.com:5432/d3m9eobk6mkr7h";
	private final String DB_USER = "tpivhqsbtgcmny";
	private String DB_PASSWORD = "92cfbdf76ca77493c3fdbfd3b45c457e89b8fd4c102dc2870d994ec85dc580b9";

	/**
	 * Connect to the PostgreSQL database
	 *
	 * @return a Connection object
	 */
	public Connection connect() throws SQLException {
		return DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
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