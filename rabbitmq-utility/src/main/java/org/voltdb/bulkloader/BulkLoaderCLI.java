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

import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.voltdb.bulkloader.BulkLoaderOptions.DBObjType;

public class BulkLoaderCLI implements CLIDriver.ParsedOptionSet
{
    // Public option opts
    public BulkLoaderOptions opts = new BulkLoaderOptions();

    @Override
    @SuppressWarnings("static-access")
    public void preParse(Options options)
    {
        options.addOption(OptionBuilder
                .withLongOpt("procedure")
                .withArgName("procedure")
                .withType(String.class)
                .hasArg()
                .withDescription("insert the opts using this procedure")
                .create('p'));
        options.addOption(OptionBuilder
                .withLongOpt("maxerrors")
                .withArgName("maxerrors")
                .withType(Number.class)
                .hasArg()
                .withDescription(String.format(
                        "maximum number of errors before giving up (default: %d)",
                        this.opts.maxerrors))
                .create('m'));
        options.addOption(OptionBuilder
                .withLongOpt("flush")
                .withArgName("flush")
                .withType(Number.class)
                .hasArg()
                .withDescription(String.format(
                        "periodic flush interval in seconds. (default: %d)",
                        this.opts.flush))
                .create('f'));
        options.addOption(OptionBuilder
                .withLongOpt("batch")
                .withArgName("batch")
                .withType(Number.class)
                .hasArg()
                .withDescription(String.format(
                        "batch size for processing. (default: %d)",
                        this.opts.batch))
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
            this.opts.dbObjName = driver.args[0].trim();
            if (this.opts.dbObjName.isEmpty()) {
                this.opts.dbObjName = null;
            }
            else {
                this.opts.dbObjType = DBObjType.TABLE;
            }
        }
        String procedure = driver.getTrimmedString("procedure");
        if (procedure != null) {
            if (this.opts.dbObjType != null) {
                driver.abort(true, "Either a table or a procedure name is required, but not both.");
            }
            this.opts.dbObjType = DBObjType.PROCEDURE;
            this.opts.dbObjName = procedure;
        }
        if (this.opts.dbObjType == null) {
            driver.abort(true, "Either a table or a procedure name is required.");
        }
        this.opts.batch = driver.getNumber("batch", this.opts.batch);
        if (this.opts.batch < 0) {
            driver.abort(true, "Batch size must be >= 0.");
        }
        this.opts.flush = driver.getNumber("flush", this.opts.flush);
        if (this.opts.flush <= 0) {
            driver.abort(true, "Periodic flush interval must be > 0");
        }
    }
}
