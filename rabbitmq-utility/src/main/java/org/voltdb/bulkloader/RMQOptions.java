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

/**
 * RabbitMQ connection options.
 */
public class RMQOptions
{
    /// Host name or IP address
    public String host = null;

    /// Host port
    public Long port = null;

    /// RMQ queue name
    public String queue = null;

    /// RMQ exchange name
    public String exchange = null;

    /// RMQ exchange type
    public String extype = null;

    /// RMQ routing key
    public String routing = null;

    /// RMQ binding strings/patterns
    public String[] bindings = new String[] {};

    /// RMQ login user
    public String user = null;

    /// RMQ login password
    public String password = null;

    /// RMQ virtual host
    public String vhost = null;

    /// AMQP URI
    public String amqp = null;

    /// Make the queue persistent when true
    public boolean persistent = false;
}
