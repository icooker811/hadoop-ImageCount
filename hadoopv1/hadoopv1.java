/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoopv1;

/**
 *
 * @author icooker811
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


/**
 *
 * @author icooker811
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Hadoopv1 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        args = new String[6];
        args[0] = "-m"; //no of map
        args[1] = "5"; //was 10
        args[2] = "-r"; //no of reduce
        args[3] = "5"; //was 10
        args[4] = "/user/tuk/hadooptest/input"; //input folder
        args[5] = "/user/tuk/hadooptest/output2"; //output folder

        int res = ToolRunner.run(new Configuration(), new Hadoopv1(), args); //args);
        System.exit(res);
    }

    /**
     * @author Jongwook Woo (jwoo5@calstatela.edu)
     * @param args
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), Hadoopv1.class);
        conf.setJobName("MyMaxTemperatureWithCombiner");

        conf.setMapperClass(HadoopMapper.class);
        /*[*/        conf.setCombinerClass(HadoopReducer.class)/*]*/;
        conf.setReducerClass(HadoopReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        List<String> other_args = new ArrayList<String>();
        for (int i = 0; i < args.length; ++i) {
            try {
                if ("-m".equals(args[i])) {
                    conf.setNumMapTasks(Integer.parseInt(args[++i]));
                } else if ("-r".equals(args[i])) {
                    conf.setNumReduceTasks(Integer.parseInt(args[++i]));
                } else {
                    other_args.add(args[i]);
                }
            } catch (NumberFormatException except) {
                System.out.println("ERROR: Integer expected instead of " + args[i]);
                return printUsage();
            } catch (ArrayIndexOutOfBoundsException except) {
                System.out.println("ERROR: Required parameter missing from "
                        + args[i - 1]);
                return printUsage();
            }
        }
        // Make sure there are exactly 2 parameters left.
        if (other_args.size() != 2) {
            System.out.println("ERROR: Wrong number of parameters: "
                    + other_args.size() + " instead of 2.");
            return printUsage();
        }
        //conf.setInputPath(new Path(other_args.get(0)));
        //conf.setOutputPath(new Path(other_args.get(1)));

        // Updated input/output
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(other_args.get(0)));
        FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

        JobClient.runJob(conf);
        return 0;
    }

    static int printUsage() {
        System.out.println("multifetch [-m nmaps] [-r nreduces] <inputs> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }
}
