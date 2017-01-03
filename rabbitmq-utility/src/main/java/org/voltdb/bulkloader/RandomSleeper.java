/*
 * The MIT License (MIT)
 *
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import org.voltdb.bulkloader.CLIDriver.CLISpec;

public class RandomSleeper
{
    private final Random random;
    private final RSOptions opts = new RSOptions();

    public static class RSOptions
    {
        public Long sleepmin = null;
        public Long sleepmax = null;
        public Long seed = null;
        public RandomSleeper sleeper;
        public boolean verbose = false;
    }

    public static class RSCLISpec implements CLISpec
    {
        public RSOptions opts = new RSOptions();

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
            options.addOption(OptionBuilder
                    .withLongOpt("seed")
                    .withArgName("seed")
                    .withType(Number.class)
                    .hasArg()
                    .withDescription("random seed")
                    .create());
        }

        @Override
        public void postParse(CLIDriver driver)
        {
            this.opts.sleepmin = driver.getNumber("sleepmin", this.opts.sleepmin);
            this.opts.sleepmax = driver.getNumber("sleepmax", this.opts.sleepmax);
            // Values will either both be null or they will both be set.
            if (this.opts.sleepmin == null && this.opts.sleepmax != null) {
                this.opts.sleepmin = 0L;
            }
            if (this.opts.sleepmax == null && this.opts.sleepmin != null) {
                this.opts.sleepmax = this.opts.sleepmin;
            }
            if (this.opts.sleepmin != null) {
                // Effectively checks both max and min are not negative.
                if (this.opts.sleepmin < 0) {
                    driver.addError("Sleep milliseconds must be >= 0");
                }
                if (this.opts.sleepmax < this.opts.sleepmin) {
                    driver.addError("Maximum sleep milliseconds must be >= minimum");
                }
            }
            this.opts.seed = driver.getNumber("seed", this.opts.seed);
            this.opts.sleeper = new RandomSleeper(this.opts.seed);
            this.opts.sleeper.setRange(this.opts.sleepmin, this.opts.sleepmax);
            this.opts.sleeper.setVerbose(this.opts.verbose);
        }
    }

    public RandomSleeper(Long seed)
    {
        this.random = seed != null ? new Random(seed) : new Random();
    }

    public static RSCLISpec getCLISpec(Long defaultSleepmin, Long defaultSleepmax, Long seed, boolean verbose)
    {
        RSCLISpec cli = new RSCLISpec();
        cli.opts.sleepmin = defaultSleepmin;
        cli.opts.sleepmax = defaultSleepmax;
        cli.opts.seed = seed;
        cli.opts.verbose = verbose;
        return cli;
    }

    public void setRange(Long sleepmin, Long sleepmax)
    {
        this.opts.sleepmin = sleepmin;
        this.opts.sleepmax = sleepmax;
    }

    public void setVerbose(boolean verbose)
    {
        this.opts.verbose = verbose;
    }

    public void sleep() throws InterruptedException
    {
        if (this.opts.sleepmin != null && this.opts.sleepmax > 0) {
            int delta = this.opts.sleepmax.intValue() - this.opts.sleepmin.intValue();
            long offset = delta == 0 ? 0 : this.random.nextInt(delta);
            long sleepMillis = this.opts.sleepmin + offset;
            if (this.opts.verbose) {
                System.out.printf("  (sleep %d)\n", sleepMillis);
            }
            Thread.sleep(sleepMillis);
        }
    }
}
