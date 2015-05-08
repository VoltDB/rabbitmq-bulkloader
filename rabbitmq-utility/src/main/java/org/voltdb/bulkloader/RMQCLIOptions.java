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

public class RMQCLIOptions implements CLIDriver.ParsedOptions
{
    public String mqhost = null;
    public Long mqport = null;
    public String mqqueue = null;
    public String mqexchange = null;
    public String mqtopic = null;
    public String mquser = null;
    public String mqpassword = null;
    public String mqvhost = null;
    public String mquri = null;

    @Override
    @SuppressWarnings("static-access")
    public void preParse(Options options)
    {
        options.addOption(OptionBuilder
                            .withLongOpt("mqhost")
                            .withArgName("mqhost")
                            .withType(String.class)
                            .hasArg()
                            .withDescription("RabbitMQ host (default: localhost)")
                            .create());
        options.addOption(OptionBuilder
                            .withLongOpt("mqport")
                            .withArgName("mqport")
                            .withType(Number.class)
                            .hasArg()
                            .withDescription("RabbitMQ port")
                            .create());
        options.addOption(OptionBuilder
                            .withLongOpt("mqqueue")
                            .withArgName("mqqueue")
                            .withType(String.class)
                            .hasArg()
                            .isRequired()
                            .withDescription("RabbitMQ queue")
                            .create());
        options.addOption(OptionBuilder
                            .withLongOpt("mqexchange")
                            .withArgName("mqexchange")
                            .withType(String.class)
                            .hasArg()
                            .withDescription("RabbitMQ exchange")
                            .create());
        options.addOption(OptionBuilder
                            .withLongOpt("mqtopic")
                            .withArgName("mqtopic")
                            .withType(String.class)
                            .hasArg()
                            .withDescription("RabbitMQ topic")
                            .create());
        options.addOption(OptionBuilder
                            .withLongOpt("mquser")
                            .withArgName("mquser")
                            .withType(String.class)
                            .hasArg()
                            .withDescription("RabbitMQ user")
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
                            .withDescription("RabbitMQ virtual host")
                            .create());
        options.addOption(OptionBuilder
                            .withLongOpt("mquri")
                            .withArgName("mquri")
                            .withType(String.class)
                            .hasArg()
                            .withDescription("RabbitMQ AMQP URI")
                            .create());
    }

    @Override
    public void postParse(CLIDriver driver)
    {
        this.mqhost = driver.getString("mqhost", "localhost");
        this.mqport = driver.getNumber("mqport");
        this.mqqueue = driver.getString("mqqueue");
        this.mqexchange = driver.getString("mqexchange");
        this.mqtopic = driver.getString("mqtopic");
        this.mquser = driver.getString("mquser");
        this.mqpassword = driver.getString("mqpassword");
        this.mqvhost = driver.getString("mqvhost");
        this.mquri = driver.getString("mquri");
    }
}
