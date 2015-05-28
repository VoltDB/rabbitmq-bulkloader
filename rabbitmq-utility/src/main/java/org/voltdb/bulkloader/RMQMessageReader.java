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
import java.io.Reader;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Implements a Reader for reading from the RabbitMQ message stream.
 */
class RMQMessageReader extends Reader
{
    /// RabbitMQ-related options.
    private final RMQOptions m_opts;

    private ConnectionFactory m_factory = null;
    private Connection m_connection = null;
    private Channel m_channel = null;
    private QueueingConsumer m_consumer = null;
    // Current message (never null).
    private String m_message = "";
    // Position to continue character extraction.
    private int m_messagePos = 0;

    /**
     * Construct the reader with the options that help make the connection.
     *
     * @param rmqOpts
     */
    public RMQMessageReader(final RMQOptions opts)
    {
        m_opts = opts;
    }

    /**
     * Internal method to initialize the RabbitMQ stream.
     *
     * @throws IOException
     */
    private void initRabbitMQ() throws IOException
    {
        m_factory = new ConnectionFactory();
        m_factory.setHost(m_opts.host);
        try {
            m_connection = m_factory.newConnection();
            m_channel = m_connection.createChannel();
            if (m_opts.exchange != null) {
                if (m_opts.extype != null) {
                    m_channel.exchangeDeclare(m_opts.exchange, m_opts.extype);
                }
                for (String bindingKey : m_opts.bindings) {
                    m_channel.queueBind(m_opts.queue, m_opts.exchange, bindingKey);
                }
            }

            m_channel.queueDeclare(m_opts.queue, true, false, false, null);
            m_channel.basicQos(1);
            m_consumer = new QueueingConsumer(m_channel);
            m_channel.basicConsume(m_opts.queue, false, m_consumer);
        }
        catch (IOException e) {
            close();
            throw e;
        }
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException
    {
        // One time initialization of the RabbitMQ stream. Can throw IOException.
        if (m_factory == null) {
            initRabbitMQ();
        }

        // Assume m_message is non-null and m_messagePos is valid.
        assert m_message != null;
        assert m_messagePos >= 0;
        assert m_messagePos <= m_message.length();

        // Move characters into the buffer and get new message(s) until
        // the request is satisfied.
        int wasRead = 0;
        while (true) {

            if (m_messagePos < m_message.length()) {

                // Pull some or all remaining characters from the current message.
                int toRead = Math.min(len - wasRead, m_message.length() - m_messagePos);
                m_message.getChars(m_messagePos, toRead, cbuf, off + wasRead);
                wasRead += toRead;

                if (wasRead == len) {
                    // DONE!
                    break;
                }
            }

            // Buffer still needs feeding. Get another RabbitMQ message.
            QueueingConsumer.Delivery delivery;
            try {
                delivery = m_consumer.nextDelivery();
                m_message = new String(delivery.getBody());
                m_messagePos = 0;
                m_channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
            catch (ShutdownSignalException|ConsumerCancelledException e) {
                close();
                throw new IOException("Failed to read from the RabbitMQ stream.", e);
            }
            catch (InterruptedException e) {
                wasRead = -1;
                close();
                break;
            }
        }

        return wasRead;
    }

    @Override
    public void close() throws IOException
    {
        if (m_channel != null) {
            m_channel.close();
            m_channel = null;
        }
        if (m_connection != null) {
            m_connection.close();
            m_connection = null;
        }
        m_factory = null;
        m_consumer = null;
    }
}