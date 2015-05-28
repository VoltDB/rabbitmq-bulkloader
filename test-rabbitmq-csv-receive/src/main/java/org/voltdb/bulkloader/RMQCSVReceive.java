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

import org.apache.commons.cli.Options;
import org.voltdb.bulkloader.CLIDriver.ParsedOptionSet;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class RMQCSVReceive
{
    private static final String SYNTAX = "test-rabbitmq-csv-receive [options ...]";

    public static void receiveMessages(RMQOptions rmqOpts, TestOptions testOpts, String[] args)
            throws IOException
    {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rmqOpts.host);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        if (rmqOpts.exchange != null) {
            if (rmqOpts.extype != null) {
                channel.exchangeDeclare(rmqOpts.exchange, rmqOpts.extype);
            }
            for (String bindingKey : rmqOpts.bindings) {
                channel.queueBind(rmqOpts.queue, rmqOpts.exchange, bindingKey);
            }
        }

        try {
            channel.queueDeclare(rmqOpts.queue, true, false, false, null);
            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
            channel.basicQos(1);
            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(rmqOpts.queue, false, consumer);
            while (true) {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                String message = new String(delivery.getBody());
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                // Sleep 1 second for every trailing '.'.
                int dotCount = 0;
                for (int i = message.length() - 1; i >= 0; --i) {
                    if (message.charAt(i) == '.') {
                        dotCount++;
                    }
                    else {
                        break;
                    }
                }
                if (dotCount > 0) {
                    message = message.substring(0, message.length() - dotCount);
                }
                System.out.printf(" [x] Received '%s'\n", message);
                Thread.sleep(dotCount * 1000);
            }
        }
        catch (ShutdownSignalException|ConsumerCancelledException|InterruptedException e) {
            e.printStackTrace();
        }
        finally {
            channel.close();
            connection.close();
        }
    }

    //TODO: Additional test-specific option and argument handling. Delete if not needed.
    private static class TestOptions
    {
    }

    private static class TestCLI implements ParsedOptionSet
    {
        /// Public option data
        public TestOptions opts = new TestOptions();

        @Override
        public void preParse(Options options)
        {
        }

        @Override
        public void postParse(CLIDriver driver)
        {
        }
    }

    public static void main(String[] args) throws IOException
    {
        RMQCLI rmqCLI = RMQCLI.createForConsumer();
        TestCLI testCLI = new TestCLI();
        CLIDriver options = CLIDriver.parse(SYNTAX, args, rmqCLI, testCLI);
        try {
            receiveMessages(rmqCLI.opts, testCLI.opts, options.args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }
}
