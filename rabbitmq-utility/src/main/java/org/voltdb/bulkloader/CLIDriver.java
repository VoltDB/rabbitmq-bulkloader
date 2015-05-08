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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class CLIDriver
{
    protected final String appName;
    protected PosixParser parser = null;
    protected CommandLine cmd = null;
    protected Options options = null;
    public String[] args = {};

    protected CLIDriver(String appName)
    {
        this.appName = appName;
    }

    public String getString(String name)
    {
        try {
            return (String) cmd.getParsedOptionValue(name);
        }
        catch (ParseException e) {
            this.abort(true, e.getLocalizedMessage());
            return null;
        }
    }

    public String getString(String name, String defaultValue)
    {
        if (!this.cmd.hasOption(name)) {
            return defaultValue;
        }
        return getString(name);
    }

    public String getTrimmedString(String name)
    {
        String value = this.getString(name);
        if (value != null) {
            value = value.trim();
        }
        return value;
    }

    public String getTrimmedString(String name, String defaultValue)
    {
        String value = this.getString(name, defaultValue);
        if (value != null) {
            value = value.trim();
        }
        // Empty becomes null.
        return value.isEmpty() ? null : value;
    }

    public String[] getCommaSeparatedStrings(String name, String... defaultValues)
    {
        try {
            if (!cmd.hasOption(name)) {
                return defaultValues;
            }
            return ((String) cmd.getParsedOptionValue(name)).split(",");
        }
        catch (ParseException e) {
            this.abort(true, e.getLocalizedMessage());
            return null;
        }
    }

    public Long getNumber(String name)
    {
        try {
            return (Long) cmd.getParsedOptionValue(name);
        }
        catch (ParseException e) {
            this.abort(true, e.getLocalizedMessage());
            return null;
        }
    }

    public Long getNumber(String name, Long defaultValue)
    {
        if (!this.cmd.hasOption(name)) {
            return defaultValue;
        }
        return getNumber(name);
    }

    /**
     * CLIDriver factory that parses command line arguments and allows
     * additional options to be included. See documentation for parse().
     *
     * @param args  command line arguments (and options)
     * @param addOpts  additional ParsedOptions objects
     */
    public static CLIDriver parse(String syntax, String[] args, ParsedOptions... addOpts)
    {
        CLIDriver config = new CLIDriver(syntax);
        try {
            config.parseArgs(args, addOpts);
        }
        catch (ParseException e) {
            config.abort(true, e.getLocalizedMessage());
            // exits above
        }
        return config;
    }

    public interface ParsedOptions
    {
        void preParse(Options options);
        void postParse(CLIDriver driver);
    }

    /**
     * Parse arguments and incorporate additional options, if provided.
     * Core MQ options are accessible through public members, while added
     * options must be accessed through the get<type>() methods. To fully
     * customize the directly accessible data members create a subclass.
     *
     * @param args  command line arguments (and options)
     * @param addOpts  additional ParsedOptions objects
     * @throws ParseException  when command line input fails validation
     */
    protected void parseArgs(String[] args, ParsedOptions... addOpts)
            throws ParseException
    {
        this.options = new Options();
        for (ParsedOptions addOpt : addOpts) {
            addOpt.preParse(this.options);
        }

        this.parser = new PosixParser();
        this.cmd = this.parser.parse(this.options, args);
        this.args = this.cmd.getArgs();

        for (ParsedOptions addOpt : addOpts) {
            addOpt.postParse(this);
        }
    }

    public void usage()
    {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(this.appName, this.options, false);
    }

    public void abort(boolean showUsage, String fmt, Object... args)
    {
        System.err.printf(fmt, args);
        System.err.println("");
        if (showUsage) {
            this.usage();
        }
        System.exit(255);
    }
}
