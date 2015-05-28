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

import org.voltdb.client.ClientImpl;
import org.voltdb.utils.BulkLoaderErrorHandler;
import org.voltdb.utils.CSVBulkDataLoader;
import org.voltdb.utils.CSVDataLoader;
import org.voltdb.utils.CSVTupleDataLoader;


public class BulkLoaderOptions
{
    private static int DEFAULT_MAX_ERRORS = 100;
    private static int DEFAULT_FLUSH_INTERVAL = 10;
    private static int DEFAULT_BATCH_SIZE = 200;

    enum DBObjType {
        TABLE("table"),
        PROCEDURE("procedure");

        private final String text;

        private DBObjType(final String text)
        {
            this.text = text;
        }

        @Override
        public String toString()
        {
            return text;
        }
    }

    // Public option opts
    public DBObjType dbObjType = null;
    public String dbObjName = null;
    public Long maxerrors = (long) DEFAULT_MAX_ERRORS;
    public Long flush = (long) DEFAULT_FLUSH_INTERVAL;
    public Long batch = (long) DEFAULT_BATCH_SIZE;

    /**
     * Create a CSV data loader based on the option settings.
     *
     * @param clientImpl  VoltDB client
     * @param errorHandler  error handler call-back
     * @return CSV loader object
     * @throws IOException
     */
    public CSVDataLoader createCSVLoader(
            ClientImpl clientImpl,
            BulkLoaderErrorHandler errorHandler)
            throws IOException
    {
        try {
            switch(this.dbObjType) {
            case PROCEDURE:
                return new CSVTupleDataLoader(clientImpl, this.dbObjName, errorHandler);
            case TABLE:
                return new CSVBulkDataLoader(clientImpl, this.dbObjName, this.batch.intValue(), errorHandler);
            }
        }
        catch(Exception e) {
            throw new IOException("Failed to create CSV data loader.", e);
        }
        // Shouldn't get here.
        return null;
    }
}
