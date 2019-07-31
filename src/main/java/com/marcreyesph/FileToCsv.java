package com.marcreyesph;

import java.io.IOException;
import java.net.URI;

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

public class FileToCsv {

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
                        System.out.println("\tpath = " + createEvent.getPath());
                        System.out.println("\towner = " + createEvent.getOwnerName());
                        System.out.println("\tctime = " + createEvent.getCtime());
                        break;
                    case UNLINK:
                        UnlinkEvent unlinkEvent = (UnlinkEvent) event;
                        System.out.println("\tpath = " + unlinkEvent.getPath());
                        System.out.println("\ttimeStamp = " + unlinkEvent.getTimestamp());
                        break;
                    case APPEND:
                        AppendEvent appendEvent = (AppendEvent) event;
                        System.out.println("\tpath = " + appendEvent.getPath());
                        System.out.println("\teventType = " + appendEvent.getEventType());
                        break;
                    case CLOSE:
                        CloseEvent closeEvent = (CloseEvent) event;
                        System.out.println("\tpath = " + closeEvent.getPath());
                        System.out.println("\teventType = " + closeEvent.getEventType());
                        System.out.println("\ttimeStamp = " + closeEvent.getTimestamp());
                        System.out.println("\tfileSize = " + closeEvent.getFileSize());
                        break;
                    case RENAME:
                        RenameEvent renameEvent = (RenameEvent) event;
                        System.out.println("\tsourcePath = " + renameEvent.getDstPath());
                        System.out.println("\tdestinationPath = " + renameEvent.getSrcPath());
                        System.out.println("\teventType = " + renameEvent.getEventType());
                        System.out.println("\ttimeStamp = " + renameEvent.getTimestamp());
                        break;
                    default:
                        System.out.println("\tNo file changes are being watched in this period.");
                        break;
                }
            }
        }
    }


}