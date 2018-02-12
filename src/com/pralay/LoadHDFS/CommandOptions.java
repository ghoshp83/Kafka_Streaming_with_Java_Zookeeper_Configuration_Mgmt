package com.pralay.LoadHDFS;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * CommandOptions: validates command options that are used to
 *         start the App :
 */
public class CommandOptions {
    private static final Logger log = LoggerFactory.getLogger(CommandOptions.class.getName());
    @Option(name = "-topic", usage = "Comma separated list of topics : Example: -topic Protocol1,Protocol2,Protocol3,Protocol4,Protocol5")
    private String topicOption = null;

    public String getTopicOption() {
        return topicOption;
    }

    @Option(name = "-c", usage = "configuration file. I.e -c config.txt")
    private String configfilenameOption = null;

    public String getConfigfilenameOption() {
        return configfilenameOption;
    }


    @Option(name = "-v", usage = "application version. I.e -v 1.1")
    private String appVersionOption = null;

    public String getAppVersionOption() {
        return appVersionOption;
    }

    // receives other command line parameters than options
    @Argument
    private List<String> arguments = new ArrayList<String>();
    private String[] args = null;

    /**
     * @param args
     *            : options arguments. process the application command options
     */
    public CommandOptions(String[] args) {
        this.args = args;
    }

    /**
     * @return: true if all command options are valid false otherwise.
     */
    /**
     * @return
     */
    public boolean processOptions() {
        CmdLineParser parser = new CmdLineParser(this);
        // if you have a wider console, you could increase the value;
        // here 80 is also the default
        parser.setUsageWidth(80);
        try {
            // parse the arguments.
            parser.parseArgument(args);

            KafkaToLoadHdfs.topic = this.topicOption;
            KafkaToLoadHdfs.configFileName = this.configfilenameOption;
            KafkaToLoadHdfs.appVersion = this.appVersionOption;

        } catch (CmdLineException e) {
            // if there's a problem in the command line,
            // you'll get this exception. this will report
            // an error message.
            System.out.println("invalid arguments:" + e.getMessage());
            log.error("invalid arguments:" + e.getMessage());

            // print the list of available options
            parser.printUsage(System.out);
            return false;
        }
        return true;
    }
}