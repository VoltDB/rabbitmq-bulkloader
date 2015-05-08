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
import org.voltdb.client.Client;

public class VoltDBCLIOptions implements CLIDriver.ParsedOptions
{
    public String[] servers = null;
    public Long port = null;
    public String user = null;
    public String password = null;

    @Override
    @SuppressWarnings("static-access")
    public void preParse(Options options)
    {
        options.addOption(OptionBuilder
            .withLongOpt("servers")
            .withArgName("servers")
            .withType(String.class)
            .hasArg()
            .withDescription("comma-separated VoltDB server(s) (default: localhost)")
            .create('s'));
        options.addOption(OptionBuilder
            .withLongOpt("port")
            .withArgName("port")
            .withType(Long.class)
            .hasArg()
            .withDescription(String.format("VoltDB server connection port (default: %d)", Client.VOLTDB_SERVER_PORT))
            .create('p'));
        options.addOption(OptionBuilder
            .withLongOpt("user")
            .withArgName("user")
            .withType(String.class)
            .hasArg()
            .withDescription("VoltDB authentication user name")
            .create());
        options.addOption(OptionBuilder
            .withLongOpt("password")
            .withArgName("password")
            .withType(String.class)
            .hasArg()
            .withDescription("VoltDB authentication password")
            .create());
    }

    @Override
    public void postParse(CLIDriver driver)
    {
        this.servers = driver.getCommaSeparatedStrings("servers", "localhost");
        this.port = driver.getNumber("port", (long) Client.VOLTDB_SERVER_PORT);
        this.user = driver.getString("user");
        this.password = driver.getString("password");

        if (this.port < 0) {
            driver.abort(true, "VoltDB port must be >= 0");
        }
    }
}
