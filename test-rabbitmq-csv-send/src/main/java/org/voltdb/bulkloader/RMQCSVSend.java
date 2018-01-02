/*
 * The MIT License (MIT)
 *
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.voltdb.bulkloader.CLIDriver.CLISpec;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class RMQCSVSend
{
    private static final String HELP_SYNTAX = "(below)";
    private static final String HELP_HEADER = ".\n"
          + "test-rabbitmq-csv-send [options] -f csvfile\n"
          + "test-rabbitmq-csv-send [options] -g genspec\n"
          + ".";
    private static final String HELP_FOOTER = ".\n"
          + "The genspec parameter is a string with one character per "
          + "generated column. "
          + "'s' represents a string column. "
          + "'i' represents an integer column.";
    private static final int HELP_WIDTH = 80;
    private static final String DEFAULT_EXCHANGE_TYPE = "direct";

    private interface LineIterator extends Iterator<String>
    {
        void open() throws IOException;
        void close();
    }

    /**
     * Iterator for CSV file rows.
     * Assumes the File object was pre-validated, e.g. for existance.
     */
    private static class FileIterator implements LineIterator
    {
        private final File file;
        private FileReader fileReader = null;
        private BufferedReader bufferedReader = null;
        private String nextLine = null;

        public FileIterator(File file)
        {
            this.file = file;
        }

        @Override
        public void open() throws IOException
        {
            this.fileReader = new FileReader(this.file);
            try {
                this.bufferedReader = new BufferedReader(this.fileReader);
            }
            catch(Throwable t) {
                this.close();
                throw new IOException("Failed to open CSV reader", t);
            }
        }

        @Override
        public void close()
        {
            try {
                if (this.bufferedReader != null) {
                    this.bufferedReader.close();
                }
                else if (this.fileReader != null) {
                    this.fileReader.close();
                }
            }
            catch(IOException e) {
                // Ignore
            }
            this.bufferedReader = null;
            this.fileReader = null;
            this.nextLine = null;
        }

        private String getNextLine()
        {
            if (this.nextLine == null) {
                if (this.bufferedReader != null) {
                    try {
                        this.nextLine = this.bufferedReader.readLine();
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                        this.close();
                    }
                }
            }
            return this.nextLine;
        }

        @Override
        public boolean hasNext()
        {
            return this.getNextLine() != null;
        }

        @Override
        public String next()
        {
            String line = this.getNextLine();
            this.nextLine = null;
            return line;
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Iterator for CSV rows generated based on a counter.
     * Assumes the spec string was pre-validated.
     */
    private static class SequentialCSVGenerator implements LineIterator
    {
        private String genspec;
        private Integer counter = 0;

        public SequentialCSVGenerator(String genspec)
        {
            this.genspec = genspec;
        }

        @Override
        public boolean hasNext()
        {
            return true;
        }

        @Override
        public String next()
        {
            counter++;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < this.genspec.length(); ++i) {
                if (i > 0) {
                    sb.append(",");
                }
                switch(this.genspec.charAt(i)) {
                case 'i':
                    sb.append(this.counter.toString());
                    break;
                case 's':
                    sb.append(String.format("\"S%d\"", this.counter));
                    break;
                default:
                    assert false;
                }
            }
            return sb.toString();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void open() throws IOException
        {
        }

        @Override
        public void close()
        {
        }

    }

    private static void sendMessages(
            RMQOptions rmqOpts,
            RandomSleeper.RSOptions sleeperOpts,
            TestOptions testOpts) throws InterruptedException
    {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rmqOpts.host);

        Connection connection = null;
        Channel channel = null;
        String exchangeName = "";
        // Use the queue name if the routing key is not specified.
        String routingKey = rmqOpts.routing != null ? rmqOpts.routing : rmqOpts.queue;
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            if (rmqOpts.exchange != null) {
                exchangeName = rmqOpts.exchange;
                channel.exchangeDeclare(exchangeName, rmqOpts.extype);
            }
        }
        catch (IOException e1) {
            e1.printStackTrace();
            System.exit(255);
        }

        try {
            channel.queueDeclare(rmqOpts.queue, rmqOpts.persistent, false, false, null);
            try {
                while (testOpts.lineIter.hasNext()) {
                    String message = testOpts.lineIter.next();
                    channel.basicPublish(
                            exchangeName,
                            routingKey,
                            MessageProperties.TEXT_PLAIN,
                            message.getBytes());
                    System.out.printf(" [x] Sent '%s'\n", message);
                    sleeperOpts.sleeper.sleep();
                }
            }
            finally {
                testOpts.lineIter.close();
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try {
                channel.close();
                connection.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static class TestOptions
    {
        public LineIterator lineIter = null;

    }

    private static class TestCLISpec implements CLISpec
    {
        /// Public option data
        public TestOptions opts = new TestOptions();

        @Override
        @SuppressWarnings("static-access")
        public void preParse(Options options)
        {
            options.addOption(OptionBuilder
                                .withLongOpt("csvfile")
                                .withArgName("csvfile")
                                .withType(String.class)
                                .hasArg()
                                .withDescription("input CSV file")
                                .create('f'));
            options.addOption(OptionBuilder
                                .withLongOpt("genspec")
                                .withArgName("genspec")
                                .withType(String.class)
                                .hasArg()
                                .withDescription("CSV generation specification (see below)")
                                .create('g'));
        }

        @Override
        public void postParse(CLIDriver driver)
        {
            String csvfilePath = driver.getString("csvfile");
            String genspec = driver.getString("genspec");
            if (csvfilePath == null && genspec == null) {
                driver.addError("Use --csvfile or --genspec to specify a data source.");
            }
            if (csvfilePath != null && genspec != null) {
                driver.addError("Combining a CSV file with a random generator is not allowed.");
            }
            if (csvfilePath != null) {
                File csvfile = new File(csvfilePath);
                if (csvfile.exists()) {
                    this.opts.lineIter = new FileIterator(csvfile);
                    try {
                        this.opts.lineIter.open();
                    } catch (IOException e) {
                        driver.addError("Could not open file: %s", csvfilePath);
                    }
                }
                else {
                    driver.addError("File does not exist: %s", csvfilePath);
                }
            }
            else if (genspec != null) {
                if (checkGenSpec(genspec)) {
                    this.opts.lineIter = new SequentialCSVGenerator(genspec);
                }
                else {
                    driver.addError("Bad generator specification: %s", genspec);
                }
            }
        }

        private static boolean checkGenSpec(String genspec)
        {
            for (int i = 0; i < genspec.length(); ++i) {
                switch(genspec.charAt(i)) {
                case 'i':
                case 's':
                    break;
                default:
                    return false;
                }
            }
            return true;
        }
    }

    public static void main(String[] args) throws IOException
    {
        // Set up and parse the CLI.
        RMQCLISpec rmqCLI = RMQCLISpec.createCLISpecForProducer(DEFAULT_EXCHANGE_TYPE);
        TestCLISpec testCLI = new TestCLISpec();
        RandomSleeper.RSCLISpec sleeperCLI = RandomSleeper.getCLISpec(1000L, 1000L, null, true);
        CLIDriver.HelpData helpData = new CLIDriver.HelpData();
        helpData.syntax = HELP_SYNTAX;
        helpData.header = HELP_HEADER;
        helpData.width = HELP_WIDTH;
        helpData.footer = HELP_FOOTER;
        CLIDriver.parse(helpData, args, rmqCLI, sleeperCLI, testCLI);

        try {
            sendMessages(rmqCLI.opts, sleeperCLI.opts, testCLI.opts);
        }
        catch (InterruptedException e) {
            System.exit(255);
        }
    }
}
