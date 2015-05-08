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

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.voltcore.logging.VoltLogger;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.utils.BulkLoaderErrorHandler;
import org.voltdb.utils.CSVBulkDataLoader;
import org.voltdb.utils.CSVDataLoader;
import org.voltdb.utils.CSVTupleDataLoader;
import org.voltdb.utils.RowWithMetaData;

public class RMQBulkLoader
{
    private static final String SYNTAX = "rabbitmq-bulkloader [options ...] [table]";

    private static final VoltLogger LOG = new VoltLogger("RABBITMQLOADER");

    private final MainOptions m_mainOpts;
    private final RMQCLIOptions m_rmqOpts;
    private final VoltDBCLIOptions m_voltOpts;
    private final static AtomicLong m_failedCount = new AtomicLong(0);
    private CSVDataLoader m_loader = null;
    private Client m_client = null;
    private ConsumerConnector m_consumer = null;
    private ExecutorService m_es = null;

    /**
     * Bulk loader constructor
     * @param config  command line options, etc.
     */
    public RMQBulkLoader(MainOptions mainOpts, RMQCLIOptions rmqOpts, VoltDBCLIOptions voltOpts)
    {
        m_mainOpts = mainOpts;
        m_rmqOpts = rmqOpts;
        m_voltOpts = voltOpts;
    }

    /**
     * Close the consumer when the app is exiting.
     * @throws InterruptedException
     */
    public void closeConsumer() throws InterruptedException
    {
        if (m_consumer != null) {
            m_consumer.stop();
            m_consumer = null;
        }
        if (m_es != null) {
            m_es.shutdownNow();
            m_es.awaitTermination(365, TimeUnit.DAYS);
            m_es = null;
        }
    }

    /**
     * Close all connections and cleanup on both sides.
     */
    public void close()
    {
        try {
            closeConsumer();
            m_loader.close();
            if (m_client != null) {
                m_client.close();
                m_client = null;
            }
        }
        catch (Exception ex) {
        }
    }

    /**
     * Perform the bulk load operation from start to finish.
     * @throws Exception
     */
    public void bulkLoad()
            throws Exception
    {
        // Create connection
        final ClientConfig c_config = new ClientConfig(m_voltOpts.user, m_voltOpts.password);
        c_config.setProcedureCallTimeout(0); // Set procedure all to infinite

        m_client = getClient(c_config, m_voltOpts.servers, m_voltOpts.port.intValue());

        ClientImpl clientImpl = (ClientImpl) m_client;
        BulkLoaderErrorHandler errorHandler = new ErrorHandler();
        String dbObjTypeName;
        String dbObjName;
        if (m_mainOpts.procedure != null) {
            m_loader = new CSVTupleDataLoader(clientImpl, m_mainOpts.procedure, errorHandler);
            dbObjName = m_mainOpts.procedure;
            dbObjTypeName = "procedure";
        }
        else {
            m_loader = new CSVBulkDataLoader(clientImpl, m_mainOpts.table, m_mainOpts.batch.intValue(), errorHandler);
            dbObjName = m_mainOpts.table;
            dbObjTypeName = "table";
        }
        m_loader.setFlushInterval(m_mainOpts.flush.intValue(), m_mainOpts.flush.intValue());
        m_consumer = new ConsumerConnector(m_rmqOpts.mqhost, m_rmqOpts.mqqueue, dbObjName);
        try {
            m_es = getConsumerExecutor(m_consumer, m_loader);
            LOG.info(String.format("RabbitMQ consumer started from %s:%s for %s: %s",
                            m_rmqOpts.mqhost, m_rmqOpts.mqqueue, dbObjTypeName, m_mainOpts.procedure));
            m_es.awaitTermination(365, TimeUnit.DAYS);
        }
        catch (Exception ex) {
            LOG.error("Error in RabbitMQ Consumer", ex);
            System.exit(-1);
        }
        close();
    }

    private ExecutorService getConsumerExecutor(ConsumerConnector consumer, CSVDataLoader loader)
            throws Exception
    {
        Map<String, Integer> topicCountMap = new HashMap<>();
        //Get this from config or arg. Use 3 threads default.
        ExecutorService executor = Executors.newFixedThreadPool(3);
        /*
        topicCountMap.put(m_config.topic, 3);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.m_consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(m_config.topic);

        // now launch all the threads for partitions.
        for (final KafkaStream stream : streams) {
            KafkaConsumer bconsumer = new KafkaConsumer(stream, loader);
            executor.submit(bconsumer);
        }
        */

        return executor;
    }

    /**
     * Get connection to servers in cluster.
     *
     * @param config
     * @param servers
     * @return client
     * @throws IOException
     * @throws UnknownHostException
     */
    public static Client getClient(ClientConfig config, String[] servers, int port)
            throws UnknownHostException, IOException
    {
        final Client client = ClientFactory.createClient(config);
        for (String server : servers) {
            client.createConnection(server.trim(), port);
        }
        return client;
    }

    private static class ConsumerConnector
    {
        public ConsumerConnector(String mqserver, String mqqueue, String string)
        {
            // TODO Auto-generated constructor stub
        }

        public void stop()
        {
            // TODO Auto-generated method stub

        }

    }

    public static class ErrorHandler implements BulkLoaderErrorHandler
    {

        @Override
        public boolean handleError(RowWithMetaData metaData, ClientResponse response, String error)
        {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean hasReachedErrorLimit()
        {
            // TODO Auto-generated method stub
            return false;
        }

    }

    /**
     * Main configuration options.
     */
    public static class MainOptions implements CLIDriver.ParsedOptions
    {
        // Either table or procedure will be non-null, but not both.
        String procedure = null;
        String table = null;
        Long maxerrors = (long) 100;
        Long flush = (long) 10;
        Long batch = (long) 200;

        @Override
        @SuppressWarnings("static-access")
        public void preParse(Options options)
        {
            options.addOption(OptionBuilder
                    .withLongOpt("procedure")
                    .withArgName("procedure")
                    .withType(String.class)
                    .hasArg()
                    .withDescription("insert the data using this procedure")
                    .create('p'));
            options.addOption(OptionBuilder
                    .withLongOpt("maxerrors")
                    .withArgName("maxerrors")
                    .withType(Number.class)
                    .hasArg()
                    .withDescription(String.format("maximum number of errors before giving up (default: %d)", this.maxerrors))
                    .create('m'));
            options.addOption(OptionBuilder
                    .withLongOpt("flush")
                    .withArgName("flush")
                    .withType(Number.class)
                    .hasArg()
                    .withDescription(String.format("periodic flush interval in seconds. (default: %d)", this.flush))
                    .create('f'));
            options.addOption(OptionBuilder
                    .withLongOpt("batch")
                    .withArgName("batch")
                    .withType(Number.class)
                    .hasArg()
                    .withDescription(String.format("batch size for processing. (default: %d)", this.batch))
                    .create('b'));
        }

        /**
         * Validate command line options.
         */
        @Override
        public void postParse(CLIDriver driver)
        {
            if (driver.args.length > 1) {
                driver.abort(true, "Only one argument is allowed.");
            }
            if (driver.args.length > 0) {
                this.table = driver.args[0].trim();
                if (this.table.isEmpty()) {
                    this.table = null;
                }
            }
            this.procedure = driver.getTrimmedString("procedure");
            if (this.table == null && this.procedure == null) {
                driver.abort(true, "Either a table or a procedure name is required.");
            }
            if (this.table != null && this.procedure != null) {
                driver.abort(true, "Either a table or a procedure name is required, but not both.");
            }
            this.batch = driver.getNumber("batch", this.batch);
            if (this.batch < 0) {
                driver.abort(true, "Batch size must be >= 0.");
            }
            this.flush = driver.getNumber("flush", this.flush);
            if (this.flush <= 0) {
                driver.abort(true, "Periodic flush interval must be > 0");
            }
        }
    }

    /**
     * RabbitMQ bulk loader CLI main
     *
     * @param args
     *
     */
    public static void main(String[] args)
    {
        MainOptions mainOpts = new MainOptions();
        RMQCLIOptions rmqOpts = new RMQCLIOptions();
        VoltDBCLIOptions voltOpts = new VoltDBCLIOptions();
        CLIDriver.parse(SYNTAX, args, mainOpts, rmqOpts, voltOpts);
        try {
            final RMQBulkLoader loader = new RMQBulkLoader(mainOpts, rmqOpts, voltOpts);
            loader.bulkLoad();
        }
        catch (Exception e) {
            LOG.error("Failure in RabbitMQ bulk loader", e);
            System.exit(-1);
        }

        System.exit(0);
    }
}
