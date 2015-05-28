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

import java.util.Random;

import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.voltdb.bulkloader.CLIDriver.ParsedOptionSet;

public class RandomSleeper
{
    private final Random random;
    private final OptionSet opts = new OptionSet();
    private boolean verbose = false;

    public static class OptionSet implements ParsedOptionSet
    {
        public Long sleepmin = null;
        public Long sleepmax = null;

        @Override
        @SuppressWarnings("static-access")
        public void preParse(Options options)
        {
            options.addOption(OptionBuilder
                    .withLongOpt("sleepmin")
                    .withArgName("sleepmin")
                    .withType(Number.class)
                    .hasArg()
                    .withDescription("minimum sleep milliseconds")
                    .create());
            options.addOption(OptionBuilder
                    .withLongOpt("sleepmax")
                    .withArgName("sleepmax")
                    .withType(Number.class)
                    .hasArg()
                    .withDescription("maximum sleep milliseconds")
                    .create());
        }

        @Override
        public void postParse(CLIDriver driver)
        {
            this.sleepmin = driver.getNumber("sleepmin", this.sleepmin);
            this.sleepmax = driver.getNumber("sleepmax", this.sleepmax);
            // Values will either both be null or they will both be set.
            if (this.sleepmin == null && this.sleepmax != null) {
                this.sleepmin = 0L;
            }
            if (this.sleepmax == null && this.sleepmin != null) {
                this.sleepmax = this.sleepmin;
            }
            if (this.sleepmin != null) {
                // Effectively checks both max and min are not negative.
                if (this.sleepmin < 0) {
                    driver.addError("Sleep milliseconds must be >= 0");
                }
                if (this.sleepmax < this.sleepmin) {
                    driver.addError("Maximum sleep milliseconds must be >= minimum");
                }
            }
        }
    }

    public RandomSleeper(Long seed)
    {
        this.random = seed != null ? new Random(seed) : new Random();
    }

    public ParsedOptionSet getOptionSet()
    {
        return opts;
    }

    public void setDefaultRange(Long sleepmin, Long sleepmax)
    {
        this.opts.sleepmin = sleepmin;
        this.opts.sleepmax = sleepmax;
    }

    public void setVerbose(boolean verbose)
    {
        this.verbose = verbose;
    }

    public void sleep() throws InterruptedException
    {
        if (this.opts.sleepmin != null && this.opts.sleepmax > 0) {
            int delta = this.opts.sleepmax.intValue() - this.opts.sleepmin.intValue();
            long offset = delta == 0 ? 0 : this.random.nextInt(delta);
            long sleepMillis = this.opts.sleepmin + offset;
            if (this.verbose) {
                System.out.printf("  (sleep %d)\n", sleepMillis);
            }
            Thread.sleep(sleepMillis);
        }
    }
}
