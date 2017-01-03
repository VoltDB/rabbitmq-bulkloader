/*
 * The MIT License (MIT)
 *
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import com.google_voltpatches.common.net.HostAndPort;

/**
 * Common CLI options for RabbitMQ.
 */
public class RMQCLISpec implements CLIDriver.CLISpec
{
    private static final String[] EXCHANGE_TYPES = {
        "direct",
        "topic",
        "headers",
        "fanout"
    };

    /// Public option opts
    public RMQOptions opts = new RMQOptions();

    // Private options for configuring CLI.
    private boolean m_enableExType = false;
    private boolean m_enableRoutingKey = false;
    private boolean m_enableBindingKey = false;
    private boolean m_enablePersistentFlag = false;

    static String EXCHANGE_TYPE_LIST;
    {
        StringBuilder sbext = new StringBuilder();
        for (int i = 0; i < EXCHANGE_TYPES.length; ++i) {
            sbext.append(EXCHANGE_TYPES[i]);
            if (i < EXCHANGE_TYPES.length - 1) {
                sbext.append('/');
            }
        }
        EXCHANGE_TYPE_LIST = sbext.toString();
    }

    private RMQCLISpec()
    {
    }

    public static RMQCLISpec createCLISpecForProducer(String defExType)
    {
        RMQCLISpec opts = new RMQCLISpec();
        opts.m_enableExType = true;
        opts.opts.extype = defExType;
        opts.m_enableRoutingKey = true;
        opts.m_enablePersistentFlag = true;
        return opts;
    }

    public static RMQCLISpec createCLISpecForConsumer()
    {
        RMQCLISpec opts = new RMQCLISpec();
        opts.m_enableBindingKey = true;
        return opts;
    }

    protected String checkExchangeType(String exType)
    {
        for (String cmpExType : EXCHANGE_TYPES) {
            if (exType.equalsIgnoreCase(cmpExType)) {
                return cmpExType;
            }
        }
        return null;
    }

    /**
     * Add handled option metadata to provided Commons CLI Options object.
     */
    @Override
    @SuppressWarnings("static-access")
    public void preParse(Options options)
    {
        options.addOption(OptionBuilder
                            .withLongOpt("host")
                            .withArgName("host")
                            .withType(String.class)
                            .hasArg()
                            .withDescription("RabbitMQ host[:port] (default: localhost)")
                            .create());
        options.addOption(OptionBuilder
                            .withLongOpt("queue")
                            .withArgName("queue")
                            .withType(String.class)
                            .hasArg()
                            .withDescription("RabbitMQ queue name: (default: \"\")")
                            .isRequired()
                            .create());
        options.addOption(OptionBuilder
                            .withLongOpt("exchange")
                            .withArgName("exchange")
                            .withType(String.class)
                            .hasArg()
                            .withDescription("RabbitMQ exchange name")
                            .create());
        options.addOption(OptionBuilder
                            .withLongOpt("user")
                            .withArgName("user")
                            .withType(String.class)
                            .hasArg()
                            .withDescription("RabbitMQ user name")
                            .create());
        options.addOption(OptionBuilder
                            .withLongOpt("password")
                            .withArgName("password")
                            .withType(String.class)
                            .hasArg()
                            .withDescription("RabbitMQ password")
                            .create());
        options.addOption(OptionBuilder
                            .withLongOpt("vhost")
                            .withArgName("vhost")
                            .withType(String.class)
                            .hasArg()
                            .withDescription("RabbitMQ virtual host name")
                            .create());
        options.addOption(OptionBuilder
                            .withLongOpt("amqp")
                            .withArgName("amqpuri")
                            .withType(String.class)
                            .hasArg()
                            .withDescription("RabbitMQ AMQP URI")
                            .create());
        if (m_enablePersistentFlag) {
            options.addOption(OptionBuilder
                                .withLongOpt("persistent")
                                .withArgName("persistent")
                                .withDescription("RabbitMQ queue should be persistent")
                                .create());
        }
        if (m_enableRoutingKey) {
            options.addOption(OptionBuilder
                                .withLongOpt("routing")
                                .withArgName("routing")
                                .withType(String.class)
                                .hasArg()
                                .withDescription(String.format(
                                        "RabbitMQ routing key (default: \"%s\")",
                                        this.opts.routing))
                                .create());
        }
        if (m_enableBindingKey) {
            options.addOption(OptionBuilder
                                .withLongOpt("mqbinding")
                                .withArgName("mqbinding")
                                .withType(String.class)
                                .hasArg()
                                .withDescription("RabbitMQ comma-separated binding key patterns")
                                .create());
        }
        if (m_enableExType) {
            options.addOption(OptionBuilder
                                .withLongOpt("extype")
                                .withArgName("extype")
                                .withType(String.class)
                                .hasArg()
                                .withDescription(String.format(
                                        "RabbitMQ exchange type: %s (default: %s)",
                                        EXCHANGE_TYPE_LIST, this.opts.extype))
                                .create());
        }
    }

    /**
     * Validate options and initialize fields.
     */
    @Override
    public void postParse(CLIDriver driver)
    {
        HostAndPort hostAndPort = null;
        try {
            hostAndPort = HostAndPort.fromString(driver.getString("host", "localhost"));
            this.opts.host = hostAndPort.getHostText();
            if (hostAndPort.hasPort()) {
                this.opts.port = (long) hostAndPort.getPort();
            }
        }
        catch(IllegalArgumentException | IllegalStateException e) {
            driver.addError("Bad host specifier: %s", this.opts.host);
            this.opts.host = null;
            this.opts.port = null;
        }

        this.opts.queue = driver.getString("queue", this.opts.queue);

        this.opts.exchange = driver.getString("exchange");
        if (this.opts.exchange != null && this.opts.exchange.isEmpty()) {
            driver.addError("Exchange name is empty.");
        }

        this.opts.user = driver.getString("user");
        if (this.opts.user != null && this.opts.user.isEmpty()) {
            driver.addError("User name is empty.");
        }

        this.opts.password = driver.getString("password");

        this.opts.vhost = driver.getString("vhost");
        if (this.opts.vhost != null && this.opts.vhost.isEmpty()) {
            driver.addError("Virtual host name is empty.");
        }

        this.opts.amqp = driver.getString("amqp");
        if (this.opts.amqp != null) {
            if (this.opts.amqp.isEmpty()) {
                driver.addError("AMQP URI is empty.");
                //TODO: More validation
            }
        }

        if (this.opts.exchange == null && this.opts.queue == null && this.opts.amqp == null) {
            driver.addError("One of these options must be specified: --exchange, --queue, or --ampq");
        }

        this.opts.persistent = driver.getBoolean("persistent");

        if (m_enableExType) {
            final String exTypeParam = driver.getTrimmedString(
                    "extype", this.opts.extype).toLowerCase();
            if(exTypeParam.isEmpty()) {
                driver.addError("Exchange type is empty.");
            }
            else {
                this.opts.extype = checkExchangeType(exTypeParam);
                if (this.opts.extype == null) {
                    driver.addError("Invalid exchange type: %s", exTypeParam);
                }
            }
        }

        if (m_enableRoutingKey) {
            this.opts.routing = driver.getString("routing", this.opts.routing);
        }

        if (m_enableBindingKey) {
            this.opts.bindings = driver.getCommaSeparatedStrings("mqbinding");
        }
    }
}
