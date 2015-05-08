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

import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.voltdb.bulkloader.CLIDriver.ParsedOptions;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RMQCSVSend
{
    private static final String SYNTAX = "test-rabbitmq-csv-send [options ...] messages ...";

    private static void sendMessages(RMQCLIOptions rmqOpts, TestOptions testOpts, String[] messages)
            throws InterruptedException
    {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rmqOpts.mqhost);

        Connection connection = null;
        Channel channel = null;
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
        }
        catch (IOException e1) {
            e1.printStackTrace();
            System.exit(255);
        }

        try {
            channel.queueDeclare(rmqOpts.mqqueue, false, false, false, null);
            int sent = 0;
            for (String message : messages) {
                if (sent++ > 0 && testOpts.interval != null && testOpts.interval > 0) {
                    Thread.sleep(testOpts.interval * 1000);
                }
                channel.basicPublish("", rmqOpts.mqqueue, null, message.getBytes());
                System.out.printf(" [x] Sent '%s'\n", message);
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

    private static class TestOptions implements ParsedOptions
    {
        Long interval = null;

        @Override
        @SuppressWarnings("static-access")
        public void preParse(Options options)
        {
            options.addOption(
                OptionBuilder
                    .withLongOpt("interval")
                    .withArgName("interval")
                    .withType(Number.class)
                    .hasArg()
                    .withDescription("# of seconds between sending messages (default: 0)")
                    .create());
        }

        @Override
        public void postParse(CLIDriver driver)
        {
            this.interval = driver.getNumber("interval", (long) 0);
        }
    }

    public static void main(String[] args) throws IOException
    {
        RMQCLIOptions rmqOpts = new RMQCLIOptions();
        TestOptions testOpts = new TestOptions();
        CLIDriver options = CLIDriver.parse(SYNTAX, args, rmqOpts, testOpts);
        try {
            sendMessages(rmqOpts, testOpts, options.args);
        }
        catch (InterruptedException e) {
            System.exit(255);
        }
    }
}
