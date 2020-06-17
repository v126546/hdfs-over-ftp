package org.apache.hadoop.contrib.ftp.commands;

import org.apache.ftpserver.command.impl.listing.FileFormater;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.util.DateUtils;
import org.apache.hadoop.contrib.ftp.HdfsFileObject;

public class MLSxFileFormaterExtend implements FileFormater {
    private static final String[] DEFAULT_TYPES = new String[] { "Size",
            "Modify", "Type", "Perm","UNIX.owner", "UNIX.group", "UNIX.mode" };

    private final static char[] NEWLINE = { '\r', '\n' };

    private String[] selectedTypes = DEFAULT_TYPES;
    private String command;

    /**
     * @param selectedTypes
     *            The types to show in the formated file
     */
    public MLSxFileFormaterExtend(String[] selectedTypes, String command) {
        this.command = command;
        if (selectedTypes != null) {
            this.selectedTypes = selectedTypes.clone();
        }
    }

    /**
     * @see FileFormater#format(FtpFile)
     */
    public String format(FtpFile file) {
        StringBuilder sb = new StringBuilder();
        if (this.command.equalsIgnoreCase("MLST"))
        {
            sb.append(" ");
        }
        for (int i = 0; i < selectedTypes.length; ++i) {
            String type = selectedTypes[i];
            if (type.equalsIgnoreCase("size")) {
                sb.append("size=");
                sb.append(String.valueOf(file.getSize()));
                sb.append(';');
            } else if (type.equalsIgnoreCase("modify")) {
                String timeStr = DateUtils.getFtpDate(file.getLastModified());
                sb.append("modify=");
                sb.append(timeStr);
                sb.append(';');
            } else if (type.equalsIgnoreCase("type")) {
                if (file.isFile()) {
                    sb.append("type=file;");
                } else if (file.isDirectory()) {
                    sb.append("type=dir;");
                }
            } else if (type.equalsIgnoreCase("UNIX.owner")) {
                 sb.append("UNIX.owner=");
                 sb.append(file.getOwnerName());
                 sb.append(';');
            } else if (type.equalsIgnoreCase("UNIX.group")) {
                sb.append("UNIX.group=");
                sb.append(file.getGroupName());
                sb.append(';');
            } else if (type.equalsIgnoreCase("UNIX.mode") && file instanceof HdfsFileObject) {
                sb.append("UNIX.mode=");
                sb.append(((HdfsFileObject)file).getMode());
                sb.append(';');
            } else if (type.equalsIgnoreCase("perm")) {
                sb.append("perm=");
                if (file.isReadable()) {
                    if (file.isFile()) {
                        sb.append('r');
                    } else if (file.isDirectory()) {
                        sb.append('e');
                        sb.append('l');
                    }
                }
                if (file.isWritable()) {
                    if (file.isFile()) {
                        sb.append('a');
                        sb.append('d');
                        sb.append('f');
                        sb.append('w');
                    } else if (file.isDirectory()) {
                        sb.append('f');
                        sb.append('p');
                        sb.append('c');
                        sb.append('m');
                    }
                }
                sb.append(';');
            }
        }
        sb.append(' ');
        sb.append(file.getName());

        sb.append(NEWLINE);

        return sb.toString();
    }
}
