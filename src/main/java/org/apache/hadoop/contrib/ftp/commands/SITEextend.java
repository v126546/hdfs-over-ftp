package org.apache.hadoop.contrib.ftp.commands;

import java.io.IOException;
import java.util.HashMap;

import org.apache.ftpserver.command.AbstractCommand;
import org.apache.ftpserver.command.Command;
import org.apache.ftpserver.command.impl.OPTS;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.ftpserver.impl.FtpServerContext;
import org.apache.ftpserver.impl.LocalizedFtpReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SITEextend extends AbstractCommand {

    private final Logger LOG = LoggerFactory.getLogger(OPTS.class);

    private static final HashMap<String, Command> COMMAND_MAP = new HashMap<String, Command>(
            16);

    /**
     * Execute command.
     */
    public void execute(final FtpIoSession session,
            final FtpServerContext context, final FtpRequest request)
            throws IOException, FtpException {

        // reset state
        session.resetState();

        // no params
        String argument = request.getArgument();
        if (argument == null) {
            session.write(LocalizedFtpReply.translate(session, request, context,
                    FtpReply.REPLY_501_SYNTAX_ERROR_IN_PARAMETERS_OR_ARGUMENTS,
                    "SITE", null));
            return;
        }

        // get request name
        int spaceIndex = argument.indexOf(' ');
        if (spaceIndex != -1) {
            argument = argument.substring(0, spaceIndex);
        }
        argument = argument.toUpperCase();

        // call appropriate command method
        String optsRequest = "SITE_" + argument;
        Command command = COMMAND_MAP.get(optsRequest);
        try {
            if (command != null) {
                command.execute(session, context, request);
            } else {
                session.resetState();
                session.write(LocalizedFtpReply.translate(session, request, context,
                        FtpReply.REPLY_502_COMMAND_NOT_IMPLEMENTED,
                        "SITE.not.implemented", argument));
            }
        } catch (Exception ex) {
            LOG.warn("OPTS.execute()", ex);
            session.resetState();
            session.write(LocalizedFtpReply.translate(session, request, context,
                    FtpReply.REPLY_500_SYNTAX_ERROR_COMMAND_UNRECOGNIZED,
                    "SITE", null));
        }
    }

    // initialize all the OPTS command handlers
    static {
        COMMAND_MAP.put("SITE_CHMOD",
                new org.apache.hadoop.contrib.ftp.commands.SITE_CHMODextend());
    }
}
