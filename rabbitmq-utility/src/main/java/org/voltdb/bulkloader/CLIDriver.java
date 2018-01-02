/*
 * The MIT License (MIT)
 *
 * Copyright (C) 2008-2018 VoltDB Inc.
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

/**
 * The CLIDriver class coordinates configuration and parsing of
 * command line options. It can combine multiple CLI specifications
 * (CLISpec interfaces) that bundle related options. The specifications
 * can be reused in multiple applications.
 *
 * A CLISpec implementation can be written with parameterization that
 * customizes the set of available options, e.g. for different '-'/'--'
 * letters/labels, or to limit or extend the set of supported options.
 */
public class CLIDriver
{
    /// Data for help screen
    public static class HelpData
    {
        String syntax = null;
        String header = null;
        String footer = null;
        Integer width = null;
    }

    /// Help screen data provided by caller.
    protected final HelpData helpData;

    /// Argument parser.
    protected PosixParser parser = null;

    /// Apache Commons CLI command line object
    protected CommandLine cmd = null;

    /// Apache Commons CLI options, composed by one or more CLISpec's.
    protected Options options = null;

    /// Command line arguments.
    public String[] args = {};

    /// Error messages
    protected List<String> errors = new ArrayList<String>();

    /**
     * Constructor for derived class to invoke
     * @param helpData  help screen data
     */
    protected CLIDriver(HelpData helpData)
    {
        this.helpData = helpData;
    }

    /**
     * Get string option value
     * @param name  option name
     * @return  option value
     */
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

    /**
     * Get string option value with default value
     * @param name  option name
     * @param defaultValue  default value
     * @return  option value
     */
    public String getString(String name, String defaultValue)
    {
        if (!this.cmd.hasOption(name)) {
            return defaultValue;
        }
        return getString(name);
    }

    /**
     * Get trimmed string option value
     * @param name  option name
     * @return  option value
     */
    public String getTrimmedString(String name)
    {
        String value = this.getString(name);
        if (value != null) {
            value = value.trim();
        }
        return value;
    }

    /**
     * Get trimmed string option value with default value
     * @param name  option name
     * @param defaultValue  default value
     * @return  option value
     */
    public String getTrimmedString(String name, String defaultValue)
    {
        String value = this.getString(name, defaultValue);
        if (value != null) {
            value = value.trim();
        }
        // Empty becomes null.
        return value.isEmpty() ? null : value;
    }

    /**
     * Get and parse comma-separated string option list, e.g. servers list
     * @param name  option name
     * @param defaultValues  default values
     * @return  option value
     */
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

    /**
     * Get number option value
     * @param name  option name
     * @return  option value
     */
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

    /**
     * Get number option value with default value
     * @param name  option name
     * @param defaultValue  default value
     * @return  option value
     */
    public Long getNumber(String name, Long defaultValue)
    {
        if (!this.cmd.hasOption(name)) {
            return defaultValue;
        }
        return getNumber(name);
    }

    /**
     * Get boolean option value
     * @param name  option name
     * @return  option value
     */
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
     * @param addlCLISpecs  additional CLISpec objects
     * @return  CLIDriver object
     */
    public static CLIDriver parse(
            String syntax,
            String[] args,
            CLISpec... addlCLISpecs)
    {
        HelpData helpOptions = new HelpData();
        helpOptions.syntax = syntax;
        return parse(helpOptions, args, addlCLISpecs);
    }

    /**
     * CLIDriver factory that parses command line arguments and allows
     * additional options to be included. See documentation for parse().
     *
     * @param helpOptions  command line help data
     * @param args  command line arguments (and options)
     * @param addlCLISpecs  additional CLISpec objects
     * @return  CLIDriver object
     */
    public static CLIDriver parse(HelpData helpOptions, String[] args, CLISpec... addlCLISpecs)
    {
        CLIDriver config = new CLIDriver(helpOptions);
        try {
            config.parseArgs(args, addlCLISpecs);
        }
        catch (ParseException e) {
            config.abort(true, e.getLocalizedMessage());
            // exits above
        }
        return config;
    }

    /**
     * Interface that must be implemented in order to specify the set of
     * available options.
     */
    public interface CLISpec
    {
        void preParse(Options options);
        void postParse(CLIDriver driver);
    }

    /**
     * CLI spec for standard help option.
     */
    private class HelpOptionSet implements CLISpec
    {
        /**
         * The hook for adding options (Commons CLI Option objects) to
         * the provided Options object.
         */
        @Override
        @SuppressWarnings("static-access")
        public void preParse(Options options)
        {
            options.addOption(OptionBuilder
                                .withLongOpt("help")
                                .withDescription("display help")
                                .create('h'));
        }

        /**
         * The hook for validating and capturing option and argument data,
         * e.g. as class data members.
         */
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
     * customize the directly accessible opts members create a subclass.
     *
     * @param args  command line arguments (and options)
     * @param userCLISpecs  user parsed CLI specs
     * @throws ParseException  when command line input fails validation
     */
    protected void parseArgs(String[] args, CLISpec... userCLISpecs)
            throws ParseException
    {
        this.options = new Options();

        // Add help options to provided option sets.
        List<CLISpec> cliSpecs = new ArrayList<CLISpec>(userCLISpecs.length + 1);
        for (CLISpec userCLISpec : userCLISpecs) {
            cliSpecs.add(userCLISpec);
        }
        cliSpecs.add(new HelpOptionSet());

        for (CLISpec cliSpec : cliSpecs) {
            cliSpec.preParse(this.options);
        }

        this.parser = new PosixParser();
        this.cmd = this.parser.parse(this.options, args);
        this.args = this.cmd.getArgs();

        for (CLISpec cliSpec : cliSpecs) {
            cliSpec.postParse(this);
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

    /**
     * Add an error message.
     * @param format  printf format string
     * @param args  printf variable arguments
     */
    public void addError(String format, Object... args)
    {
        this.errors.add(String.format(format, args));
    }

    /**
     * Display usage screen
     */
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

    /**
     * Abort due to error
     * @param showUsage  display usage screen when true
     * @param fmt  printf format string for message
     * @param args  printf variable arguments
     */
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
