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

import com.google_voltpatches.common.net.HostAndPort;

/**
 * Common CLI options for RabbitMQ.
 */
public class RMQCLIOptions implements CLIDriver.ParsedOptionSet
{
    private static final String[] EXCHANGE_TYPES = {
        "direct",
        "topic",
        "headers",
        "fanout"
    };

    // Option data fields - populated in postParse()
    public String mqhost = null;
    public Long mqport = null;
    public String mqqueue = null;
    public String mqexchange = null;
    public String mqrouting = "";
    public String[] mqbindings = {};
    public String mquser = null;
    public String mqpassword = null;
    public String mqvhost = null;
    public String amqp = null;
    public String mqextype = null;

    private boolean enableExType = false;
    private boolean enableRoutingKey = false;
    private boolean enableBindingKey = false;

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

    private RMQCLIOptions()
    {
    }

    public static RMQCLIOptions createForProducer(String defExType)
    {
        RMQCLIOptions opts = new RMQCLIOptions();
        opts.enableExType = true;
        opts.mqextype = defExType;
        opts.enableRoutingKey = true;
        return opts;
    }

    public static RMQCLIOptions createForConsumer()
    {
        RMQCLIOptions opts = new RMQCLIOptions();
        opts.enableBindingKey = true;
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
                            .withLongOpt("mqhost")
                            .withArgName("mqhost")
                            .withType(String.class)
                            .hasArg()
                            .withDescription("RabbitMQ host[:port] (default: localhost)")
                            .create());
        options.addOption(OptionBuilder
                            .withLongOpt("mqqueue")
                            .withArgName("mqqueue")
                            .withType(String.class)
                            .hasArg()
                            .withDescription("RabbitMQ queue name: (default: \"\")")
                            .isRequired()
                            .create());
        options.addOption(OptionBuilder
                            .withLongOpt("mqexchange")
                            .withArgName("mqexchange")
                            .withType(String.class)
                            .hasArg()
                            .withDescription("RabbitMQ exchange name")
                            .create());
        options.addOption(OptionBuilder
                            .withLongOpt("mquser")
                            .withArgName("mquser")
                            .withType(String.class)
                            .hasArg()
                            .withDescription("RabbitMQ user name")
                            .create());
        options.addOption(OptionBuilder
                            .withLongOpt("mqpassword")
                            .withArgName("mqpassword")
                            .withType(String.class)
                            .hasArg()
                            .withDescription("RabbitMQ password")
                            .create());
        options.addOption(OptionBuilder
                            .withLongOpt("mqvhost")
                            .withArgName("mqvhost")
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
        if (this.enableRoutingKey) {
            options.addOption(OptionBuilder
                                .withLongOpt("mqrouting")
                                .withArgName("mqrouting")
                                .withType(String.class)
                                .hasArg()
                                .withDescription(String.format(
                                        "RabbitMQ routing key (default: \"%s\")",
                                        this.mqrouting))
                                .create());
        }
        if (this.enableBindingKey) {
            options.addOption(OptionBuilder
                                .withLongOpt("mqbinding")
                                .withArgName("mqbinding")
                                .withType(String.class)
                                .hasArg()
                                .withDescription("RabbitMQ comma-separated binding key patterns")
                                .create());
        }
        if (this.enableExType) {
            options.addOption(OptionBuilder
                                .withLongOpt("mqextype")
                                .withArgName("mqextype")
                                .withType(String.class)
                                .hasArg()
                                .withDescription(String.format(
                                        "RabbitMQ exchange type: %s (default: %s)",
                                        EXCHANGE_TYPE_LIST, this.mqextype))
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
            hostAndPort = HostAndPort.fromString(driver.getString("mqhost", "localhost"));
            this.mqhost = hostAndPort.getHostText();
            if (hostAndPort.hasPort()) {
                this.mqport = (long) hostAndPort.getPort();
            }
        }
        catch(IllegalArgumentException | IllegalStateException e) {
            driver.addError("Bad host specifier: %s", this.mqhost);
            this.mqhost = null;
            this.mqport = null;
        }

        this.mqqueue = driver.getString("mqqueue", this.mqqueue);

        this.mqexchange = driver.getString("mqexchange");
        if (this.mqexchange != null && this.mqexchange.isEmpty()) {
            driver.addError("Exchange name is empty.");
        }

        this.mquser = driver.getString("mquser");
        if (this.mquser != null && this.mquser.isEmpty()) {
            driver.addError("User name is empty.");
        }

        this.mqpassword = driver.getString("mqpassword");

        this.mqvhost = driver.getString("mqvhost");
        if (this.mqvhost != null && this.mqvhost.isEmpty()) {
            driver.addError("Virtual host name is empty.");
        }

        this.amqp = driver.getString("amqp");
        if (this.amqp != null) {
            if (this.amqp.isEmpty()) {
                driver.addError("AMQP URI is empty.");
                //TODO: More validation
            }
        }

        if (this.mqexchange == null && this.mqqueue == null && this.amqp == null) {
            driver.addError("One of these options must be specified: --mqexchange, --mqqueue, or --ampq");
        }

        if (this.enableExType) {
            final String exTypeParam = driver.getTrimmedString(
                    "mqextype", this.mqextype).toLowerCase();
            if(exTypeParam.isEmpty()) {
                driver.addError("Exchange type is empty.");
            }
            else {
                this.mqextype = checkExchangeType(exTypeParam);
                if (this.mqextype == null) {
                    driver.addError("Invalid exchange type: %s", exTypeParam);
                }
            }
        }

        if (this.enableRoutingKey) {
            this.mqrouting = driver.getString("mqrouting", this.mqrouting);
        }

        if (this.enableBindingKey) {
            this.mqbindings = driver.getCommaSeparatedStrings("mqbinding");
        }
    }
}
