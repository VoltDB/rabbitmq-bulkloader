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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class CLIDriver
{
    public static class HelpData
    {
        String syntax = null;
        String header = null;
        String footer = null;
        Integer width = null;
    }
    protected final HelpData helpData;
    protected PosixParser parser = null;
    protected CommandLine cmd = null;
    protected Options options = null;
    public String[] args = {};
    protected List<String> errors = new ArrayList<String>();

    protected CLIDriver(HelpData helpData)
    {
        this.helpData = helpData;
    }

    public String getString(String name)
    {
        try {
            return (String) this.cmd.getParsedOptionValue(name);
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
            if (!this.cmd.hasOption(name)) {
                return defaultValues;
            }
            return ((String) this.cmd.getParsedOptionValue(name)).split(",");
        }
        catch (ParseException e) {
            this.abort(true, e.getLocalizedMessage());
            return null;
        }
    }

    public Long getNumber(String name)
    {
        try {
            return (Long) this.cmd.getParsedOptionValue(name);
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

    public boolean getBoolean(String name)
    {
        return this.cmd.hasOption(name);
    }

    /**
     * CLIDriver factory that parses command line arguments and allows
     * additional options to be included. See documentation for parse().
     *
     * @param syntax  syntax text
     * @param args  command line arguments (and options)
     * @param addOpts  additional ParsedOptionSet objects
     */
    public static CLIDriver parse(
            String syntax,
            String[] args,
            ParsedOptionSet... addOpts)
    {
        HelpData helpOptions = new HelpData();
        helpOptions.syntax = syntax;
        return parse(helpOptions, args, addOpts);
    }

    /**
     * CLIDriver factory that parses command line arguments and allows
     * additional options to be included. See documentation for parse().
     *
     * @param args  command line arguments (and options)
     * @param addOpts  additional ParsedOptionSet objects
     */
    public static CLIDriver parse(HelpData helpOptions, String[] args, ParsedOptionSet... addOpts)
    {
        CLIDriver config = new CLIDriver(helpOptions);
        try {
            config.parseArgs(args, addOpts);
        }
        catch (ParseException e) {
            config.abort(true, e.getLocalizedMessage());
            // exits above
        }
        return config;
    }

    public interface ParsedOptionSet
    {
        void preParse(Options options);
        void postParse(CLIDriver driver);
    }

    private class HelpOptionSet implements ParsedOptionSet
    {
        @Override
        @SuppressWarnings("static-access")
        public void preParse(Options options)
        {
            options.addOption(OptionBuilder
                                .withLongOpt("help")
                                .withDescription("display help")
                                .create('h'));
        }

        @Override
        public void postParse(CLIDriver driver)
        {
            if (driver.getBoolean("help")) {
                driver.usage();
                System.exit(0);
            }
        }
    }

    /**
     * Parse arguments and incorporate additional options, if provided.
     * Core MQ options are accessible through public members, while added
     * options must be accessed through the get<type>() methods. To fully
     * customize the directly accessible data members create a subclass.
     *
     * @param args  command line arguments (and options)
     * @param userOptionSets  user parsed option sets
     * @throws ParseException  when command line input fails validation
     */
    protected void parseArgs(String[] args, ParsedOptionSet... userOptionSets)
            throws ParseException
    {
        this.options = new Options();

        // Add help options to provided option sets.
        List<ParsedOptionSet> optionSets = new ArrayList<ParsedOptionSet>(userOptionSets.length + 1);
        for (ParsedOptionSet addOpt : userOptionSets) {
            optionSets.add(addOpt);
        }
        optionSets.add(new HelpOptionSet());

        for (ParsedOptionSet parsedOptionSet : optionSets) {
            parsedOptionSet.preParse(this.options);
        }

        this.parser = new PosixParser();
        this.cmd = this.parser.parse(this.options, args);
        this.args = this.cmd.getArgs();

        for (ParsedOptionSet parsedOptionSet : optionSets) {
            parsedOptionSet.postParse(this);
        }

        if (!this.errors.isEmpty()) {
            StringBuilder sb = new StringBuilder(String.format(
                    "Error%s", this.errors.size() > 1 ? "s:\n" : ": "));
            for (int ierror = 0; ierror < this.errors.size(); ++ierror) {
                sb.append(this.errors.get(ierror));
                if (ierror < this.errors.size() - 1) {
                    sb.append('\n');
                }
            }
            this.abort(true, sb.toString());
        }
    }

    public void addError(String format, Object... args)
    {
        this.errors.add(String.format(format, args));
    }

    public void usage()
    {
        HelpFormatter formatter = new HelpFormatter();
        if (this.helpData.width != null) {
            formatter.setWidth(this.helpData.width);
        }
        formatter.printHelp(
                this.helpData.syntax,
                this.helpData.header,
                this.options,
                this.helpData.footer,
                false);
    }

    public void abort(boolean showUsage, String fmt, Object... args)
    {
        if (fmt != null) {
            System.out.printf(fmt, args);
            System.out.println("");
        }
        if (showUsage) {
            this.usage();
        }
        System.exit(255);
    }
}
