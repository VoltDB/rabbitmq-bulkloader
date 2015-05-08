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
import org.voltdb.bulkloader.CLIDriver.ParsedOptions;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class RMQCSVReceive
{
    private static final String SYNTAX = "test-rabbitmq-csv-receive [options ...]";

    // For additional test-specific option and argument handling.
    private static class TestOptions implements ParsedOptions
    {
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
        RMQCLIOptions rmqOpts = new RMQCLIOptions();
        TestOptions testOpts = new TestOptions();
        CLIDriver.parse(SYNTAX, args, rmqOpts, testOpts);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rmqOpts.mqhost);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        try {
            channel.queueDeclare(rmqOpts.mqqueue, false, false, false, null);
            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(rmqOpts.mqqueue, false, consumer);
            while (true) {
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                String message = new String(delivery.getBody());
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                System.out.printf(" [x] Received '%s'\n", message);
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
}
