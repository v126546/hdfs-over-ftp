package org.apache.hadoop.contrib.ftp.commands;

import java.io.IOException;

import org.apache.ftpserver.command.AbstractCommand;
import org.apache.ftpserver.command.impl.listing.FileFormater;
import org.apache.ftpserver.command.impl.listing.ListArgument;
import org.apache.ftpserver.command.impl.listing.ListArgumentParser;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.ftpserver.impl.FtpServerContext;
import org.apache.ftpserver.impl.LocalizedFtpReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MLSTextend extends AbstractCommand {
	
	private final Logger LOG = LoggerFactory.getLogger(MLSTextend.class);

	public void execute(final FtpIoSession session,
            final FtpServerContext context, final FtpRequest request)
            throws IOException {

        // reset state variables
        session.resetState();

        // parse argument
        ListArgument parsedArg = ListArgumentParser
                .parse(request.getArgument());

        FtpFile file = null;
        try {
            file = session.getFileSystemView().getFile(
                    parsedArg.getFile());
            if (file != null && file.doesExist()) {
                FileFormater formater = new MLSxFileFormaterExtend((String[]) session
                        .getAttribute("MLST.types"), "MLST");
                session.write(LocalizedFtpReply.translate(session, request, context,
                        FtpReply.REPLY_250_REQUESTED_FILE_ACTION_OKAY, "MLST",
                        formater.format(file)));
            } else {
                session
                        .write(LocalizedFtpReply
                                .translate(
                                        session,
                                        request,
                                        context,
                                        FtpReply.REPLY_501_SYNTAX_ERROR_IN_PARAMETERS_OR_ARGUMENTS,
                                        "MLST", null));
            }
        } catch (FtpException ex) {
            LOG.debug("Exception sending the file listing", ex);
            session.write(LocalizedFtpReply.translate(session, request, context,
                    FtpReply.REPLY_501_SYNTAX_ERROR_IN_PARAMETERS_OR_ARGUMENTS,
                    "MLST", null));
        }
    }
	
}
