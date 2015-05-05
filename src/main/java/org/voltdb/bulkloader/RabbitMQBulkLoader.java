/*
 * The MIT License (MIT)
 *
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package org.voltdb.bulkloader;

import org.apache.commons.cli.HelpFormatter;
import org.voltcore.logging.VoltLogger;
import org.voltdb.CLIConfig;
import org.voltdb.client.Client;

public class RabbitMQBulkLoader
{
    private static final String APP_NAME = "voltdb-rabbitmq-bulkloader";

    private static final VoltLogger LOG = new VoltLogger("RABBITMQLOADER");

    /**
     * Configuration options.
     */
    public static class CLIConfigImpl extends CLIConfig
    {
        @Option(shortOpt = "s", desc = "comma-separated VoltDB server(s) (default: localhost)")
        String servers = "localhost";

        @Option(desc = "VoltDB server connection port (default: 21212)")
        int port = Client.VOLTDB_SERVER_PORT;

        @Option(desc = "VoltDB authentication username")
        String user = "";

        @Option(desc = "VoltDB authentication password")
        String password = "";

        @Option(shortOpt = "r", desc = "RabbitMQ server")
        String mqserver = "";

        @Option(shortOpt = "q", desc = "RabbitMQ message queue")
        String mqqueue = "";

        // Set to null if a table name is specified.
        @Option(shortOpt = "p", desc = "insert the data using this procedure")
        String procedure = "";

        // Set to null if a procedure name is specified.
        @AdditionalArgs(desc = "insert the data into this table.")
        String table = "";

        @Option(shortOpt = "m", desc = "maximum number of errors before giving up")
        int maxerrors = 100;

        @Option(shortOpt = "f", desc = "periodic flush interval in seconds. (default: 10)")
        int flush = 10;

        @Option(desc = "batch size for processing.")
        int batch = 200;

        /**
         * Validate command line options.
         */
        @Override
        public void validate()
        {
            if (this.batch < 0) {
                exitWithMessageAndUsage("Batch size must be >= 0");
            }
            if (this.flush <= 0) {
                exitWithMessageAndUsage("Periodic flush interval must be > 0");
            }
            if (this.mqqueue.length() <= 0) {
                exitWithMessageAndUsage("RabbitMQ message queue name must be specified.");
            }
            if (this.mqserver.length() <= 0) {
                exitWithMessageAndUsage("RabbitMQ server must be specified.");
            }
            if (this.port < 0) {
                exitWithMessageAndUsage("VoltDB port must be >= 0");
            }
            this.procedure = procedure.trim();
            this.table = table.trim();
            if (this.procedure.isEmpty()) {
                if (this.table.isEmpty()) {
                    exitWithMessageAndUsage("Either a procedure name or a table name is required");
                }
                this.procedure = null;
            }
            else {
                if (!this.table.isEmpty()) {
                    exitWithMessageAndUsage("Either a procedure name or a table name is required, but not both");
                }
                this.table = null;
            }
        }

        /**
         * Usage override for clean app (not class) name.
         */
        @Override
        public void printUsage()
        {
            String usage = String.format("%s [options] -r mq-server -q mq-queue tablename\n", APP_NAME);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(usage, this.options, false);
        }
    }

    public RabbitMQBulkLoader(CLIConfigImpl cfg)
    {
    }

    private void processMessages()
    {
    }

    /**
     * RabbitMQ bulk loader CLI main
     *
     * @param args
     *
     */
    public static void main(String[] args)
    {
        final CLIConfigImpl cfg = new CLIConfigImpl();
        cfg.parse(RabbitMQBulkLoader.class.getName(), args);
        try {
            final RabbitMQBulkLoader loader = new RabbitMQBulkLoader(cfg);
            loader.processMessages();
        }
        catch (Exception e) {
            LOG.error("Failure in RabbitMQ bulk loader", e);
            System.exit(-1);
        }

        System.exit(0);
    }
}
