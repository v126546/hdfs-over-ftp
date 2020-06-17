package org.apache.hadoop.contrib.ftp.commands;

import java.io.IOException;

import org.apache.ftpserver.command.AbstractCommand;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.ftpserver.impl.FtpServerContext;
import org.apache.ftpserver.impl.LocalizedFtpReply;
import org.apache.hadoop.contrib.ftp.HdfsFileObject;
import org.apache.hadoop.security.AccessControlException;

public class SITE_CHMODextend extends AbstractCommand {

    /**
     * Execute command.
     */
    public void execute(final FtpIoSession session,
            final FtpServerContext context, final FtpRequest request)
            throws IOException, FtpException {

        // reset state
        session.resetState();

        // get the listing types
        String argument = request.getArgument();

        String[] splitArg = argument.split(" ");
        
        String rightMask = splitArg[1];
        String fileName = splitArg[2];
        
        FtpFile file = null;
        try {
            file = session.getFileSystemView().getFile(fileName);
            
            if (file != null && file.doesExist()) {
            	
            	try {
            		boolean result = ((HdfsFileObject)file).chmod(rightMask);
            	
            	
	            	if (result)
	            	{
	            		session.write(LocalizedFtpReply.translate(session, request, context, FtpReply.REPLY_200_COMMAND_OKAY, "SITE.CHMOD", null));
	            	}
	            	else
	            	{
	            		session.write(LocalizedFtpReply.translate(session, request, context, FtpReply.REPLY_200_COMMAND_OKAY, "SITE.CHMOD", null));
	            	}
            	}
            	catch (AccessControlException e)
            	{
            		session.write(LocalizedFtpReply.translate(session, request, context, FtpReply.REPLY_550_REQUESTED_ACTION_NOT_TAKEN, "SITE.CHMOD", null));
            	}
            } else {
                session.write(LocalizedFtpReply
                                .translate(
                                        session,
                                        request,
                                        context,
                                        FtpReply.REPLY_450_REQUESTED_FILE_ACTION_NOT_TAKEN,
                                        "SITE.CHMOD", null));
            }
                
        } catch (FtpException ex) {
	        session.write(LocalizedFtpReply.translate(session, request, context,
	                FtpReply.REPLY_501_SYNTAX_ERROR_IN_PARAMETERS_OR_ARGUMENTS,
	                "SITE.CHMOD", null));
	    }
       
    }

   
}