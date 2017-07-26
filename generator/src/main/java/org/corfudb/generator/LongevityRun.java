package org.corfudb.generator;

import java.time.Duration;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Created by rmichoud on 7/27/17.
 */

/**
 * This longevity test launcher will set the duration of the test
 * based on inputs.
 */
public class LongevityRun {
    public static void main(String[] args) {
        long longevity;

        Options options = new Options();

        Option amountTime = new Option("t", "time_amount", true, "time amount");
        amountTime.setRequired(true);
        Option timeUnit = new Option("u", "time_unit", true, "time unit (s, m, h)");
        timeUnit.setRequired(true);

        options.addOption(amountTime);
        options.addOption(timeUnit);


        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
            String timeUnitValue = cmd.getOptionValue("time_unit");
            if (!timeUnitValue.equals("m") &&
                    !timeUnitValue.equals("s") &&
                    !timeUnitValue.equals("h")){
                throw new ParseException("Time unit should be {s,m,h}");
            }
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("longevity", options);

            System.exit(1);
            return;
        }

        long amountTimeValue = Long.parseLong(cmd.getOptionValue("time_amount"));
        String timeUnitValue = cmd.getOptionValue("time_unit");

        switch (timeUnitValue) {
            case "s":
                longevity = Duration.ofSeconds(amountTimeValue).toMillis();
                break;
            case "m":
                longevity = Duration.ofMinutes(amountTimeValue).toMillis();
                break;
            case "h":
                longevity = Duration.ofHours(amountTimeValue).toMillis();
                break;
            default:
                longevity = Duration.ofHours(1).toMillis();
        }

        LongevityApp la = new LongevityApp(longevity);
        la.runLongevityTest();
    }
}
