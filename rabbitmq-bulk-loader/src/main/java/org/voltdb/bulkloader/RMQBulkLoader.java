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
import java.io.Reader;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.voltcore.logging.VoltLogger;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.utils.BulkLoaderErrorHandler;
import org.voltdb.utils.CSVDataLoader;
import org.voltdb.utils.RowWithMetaData;

import com.google_voltpatches.common.net.HostAndPort;

public class RMQBulkLoader
{
    private static final String HELP_SYNTAX = "(below)";
    private static final String HELP_HEADER = ".\n"
          + "rabbitmqloader [options] --host server[:port] table-name\n"
          + "rabbitmqloader [options] --amqp {uri} table-name\n"
          + "rabbitmqloader [options] --host server[:port] -p proc-name\n"
          + "rabbitmqloader [options] --amqp {uri} -p proc-name\n"
          + ".";
    private static final int HELP_WIDTH = 100;

    static final VoltLogger LOG = new VoltLogger("RABBITMQLOADER");

    private final static AtomicLong m_errorCount = new AtomicLong(0);
    private CSVDataLoader m_loader = null;
    private Client m_client = null;
    private ConsumerConnector m_consumer = null;
    private ExecutorService m_es = null;

    /**
     * Bulk loader constructor
     */
    public RMQBulkLoader()
    {
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
    public void bulkLoad(
            final BulkLoaderOptions loaderOpts,
            final RMQOptions rmqOpts,
            final VoltDBOptions voltOpts) throws Exception
    {
        // Create connection
        final ClientConfig c_config = new ClientConfig(voltOpts.user, voltOpts.password);
        c_config.setProcedureCallTimeout(0); // Set procedure all to infinite

        m_client = getClient(c_config, voltOpts.servers);

        ClientImpl clientImpl = (ClientImpl) m_client;
        BulkLoaderErrorHandler errorHandler = new ErrorHandler(loaderOpts.maxerrors);
        m_loader = loaderOpts.createCSVLoader(clientImpl, errorHandler);
        m_loader.setFlushInterval(loaderOpts.flush.intValue(), loaderOpts.flush.intValue());
        Reader msgReader = new RMQMessageReader(rmqOpts);
        LOG.info(String.format("RabbitMQ consumer started from %s:%s for %s: %s",
                rmqOpts.host, rmqOpts.queue,
                loaderOpts.dbObjType.toString(), loaderOpts.dbObjName));
        m_consumer = new ConsumerConnector(msgReader);

        for (BulkLoaderData data : m_consumer) {
            try {
                m_loader.insertRow(data.metaData, data.rowData);
            }
            catch (Exception e) {
                LOG.error("Error in RabbitMQ Consumer", e);
                System.exit(-1);
            }
        }
        close();
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
    public static Client getClient(ClientConfig config, HostAndPort[] servers)
            throws UnknownHostException, IOException
    {
        final Client client = ClientFactory.createClient(config);
        for (HostAndPort server : servers) {
            client.createConnection(server.getHostText(), server.getPort());
        }
        return client;
    }

    private static class BulkLoaderData
    {
        public final RowWithMetaData metaData;
        public final Object[] rowData;

        public BulkLoaderData(final RowWithMetaData metaData, Object[] rowData)
        {
            this.metaData = metaData;
            this.rowData = rowData;
        }
    }

    private static class ConsumerConnector implements Iterable<BulkLoaderData>
    {
        private final Reader m_msgReader;
        private final CsvPreference m_csvPrefs;
        private final CsvListReader m_csvReader;

        public ConsumerConnector(final Reader msgReader)
        {
            m_msgReader = msgReader;
            m_csvPrefs = CsvPreference.STANDARD_PREFERENCE;
            m_csvReader = new CsvListReader(m_msgReader, m_csvPrefs);
        }

        public void stop()
        {
            try {
                m_msgReader.close();
            }
            catch (IOException e) {
                LOG.error("Failed to close message reader.", e);
            }
        }

        @Override
        public Iterator<BulkLoaderData> iterator()
        {
            return new ConnectorDataIterator();
        }

        private class ConnectorDataIterator implements Iterator<BulkLoaderData>
        {
            /// Caching the data for one row allows hasNext() to look ahead.
            private BulkLoaderData m_rowCache = null;
            /// Row count.
            private int m_count = 0;
            /// Set to true when done.
            private boolean m_done = false;

            //=== Iterator required overrides

            @Override
            public boolean hasNext()
            {
                return cacheRowAsNeeded();
            }

            @Override
            public synchronized BulkLoaderData next()
            {
                // Cache a row if hasNext() wasn't previously called.
                cacheRowAsNeeded();
                // Return the row and drop it from the cache.
                BulkLoaderData rowData = m_rowCache;
                m_rowCache = null;
                return rowData;
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }

            //=== Private methods

            /**
             * Check if a row is already cached or attempt to cache the next one.
             * Used for both hasNext() and next().
             * @return true if a row is available.
             */
            private synchronized boolean cacheRowAsNeeded()
            {
                if (!m_done && m_rowCache == null) {
                    try {
                        // Cache a row to support hasNext() followed by next().
                        List<String> rowStringList = m_csvReader.read();
                        if (rowStringList != null) {
                            // Cache the next row.
                            m_count++;
                            String rowText = m_csvReader.getUntokenizedRow();
                            RowWithMetaData metaData = new RowWithMetaData(rowText, m_count);
                            Object[] rowData = rowStringList.toArray();
                            m_rowCache = new BulkLoaderData(metaData, rowData);
                        }
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                        m_rowCache = null;
                    }
                    m_done = (m_rowCache == null);
                }
                return !m_done;
            }
        }
    }

    private static boolean isFatalStatus(byte status)
    {
        return (status != ClientResponse.USER_ABORT && status != ClientResponse.GRACEFUL_FAILURE);

    }

    public class ErrorHandler implements BulkLoaderErrorHandler
    {
        private final long m_maxerrors;

        public ErrorHandler(long maxerrors)
        {
            m_maxerrors = maxerrors;
        }

        @Override
        public boolean handleError(RowWithMetaData metaData, ClientResponse response, String error)
        {
            boolean okay = false;
            if (response != null) {
                byte status = response.getStatus();
                if (status != ClientResponse.SUCCESS) {
                    LOG.error(String.format("Failed to insert row: %s", metaData.rawLine));
                    if (tooManyErrors(m_errorCount.incrementAndGet()) || isFatalStatus(status)) {
                        try {
                            LOG.error("RabbitMQ bulk loader will exit.");
                            closeConsumer();
                            okay = true;
                        }
                        catch (InterruptedException ex) {
                            // okay = false
                        }
                    }
                }
            }
            return okay;
        }

        private boolean tooManyErrors(long errorCount)
        {
            return (m_maxerrors > 0 && errorCount > m_maxerrors);
        }

        @Override
        public boolean hasReachedErrorLimit()
        {
            return tooManyErrors(m_errorCount.get());
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
        // Set up and parse the CLI.
        BulkLoaderCLI loaderOpts = new BulkLoaderCLI();
        RMQCLI rmqOpts = RMQCLI.createForConsumer();
        VoltDBCLI voltOpts = new VoltDBCLI();
        CLIDriver.HelpData helpData = new CLIDriver.HelpData();
        helpData.syntax = HELP_SYNTAX;
        helpData.header = HELP_HEADER;
        helpData.width = HELP_WIDTH;
        CLIDriver.parse(helpData, args, loaderOpts, rmqOpts, voltOpts);

        try {
            final RMQBulkLoader loader = new RMQBulkLoader();
            loader.bulkLoad(loaderOpts.opts, rmqOpts.opts, voltOpts.opts);
        }
        catch (Exception e) {
            LOG.error("Failure in RabbitMQ bulk loader", e);
            System.exit(-1);
        }

        System.exit(0);
    }
}
