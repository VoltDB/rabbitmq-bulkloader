/*
 * The MIT License (MIT)
 *
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import com.google_voltpatches.common.net.HostAndPort;

public class VoltDBCLISpec implements CLIDriver.CLISpec
{
    // Public option opts
    public VoltDBOptions opts = new VoltDBOptions();

    @Override
    @SuppressWarnings("static-access")
    public void preParse(Options options)
    {
        options.addOption(OptionBuilder
            .withLongOpt("servers")
            .withArgName("servers")
            .withType(String.class)
            .hasArg()
            .withDescription(String.format(
                    "VoltDB servers (host1[:port1][,host2[:port2],...]) (default: localhost:%d)",
                    Client.VOLTDB_SERVER_PORT))
            .create('s'));
        // Deprecated in favor up using host:port.
        options.addOption(OptionBuilder
            .withLongOpt("port")
            .withArgName("port")
            .withType(Long.class)
            .hasArg()
            .withDescription(String.format(
                    "VoltDB server connection port (default: %d)",
                    Client.VOLTDB_SERVER_PORT))
            .create());
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
        String[] hostSpecs = driver.getCommaSeparatedStrings("servers", "localhost");
        Long defaultPort = driver.getNumber("port", (long) Client.VOLTDB_SERVER_PORT);
        this.opts.servers = new HostAndPort[hostSpecs.length];
        for (int i = 0; i < hostSpecs.length; ++i) {
            try {
                this.opts.servers[i] = HostAndPort.fromString(hostSpecs[i]);
                if (!this.opts.servers[i].hasPort()) {
                    this.opts.servers[i] = HostAndPort.fromParts(this.opts.servers[i].getHostText(), defaultPort.intValue());
                }
            }
            catch(IllegalArgumentException e) {
                driver.addError("Bad host[:port]: %s: %s", hostSpecs[i], e.getLocalizedMessage());
            }
        }
        this.opts.user = driver.getString("user");
        this.opts.password = driver.getString("password");
    }
}
