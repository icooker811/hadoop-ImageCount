/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoopv1;

/**
 *
 * @author root
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
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

// vv MaxTemperatureReducer2
public class HadoopReducer extends MapReduceBase
        implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterator<IntWritable> values,
            OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {

        int maxValue = Integer.MIN_VALUE;
        while (values.hasNext()) {
            maxValue = Math.max(maxValue, values.next().get());
        }
        output.collect(key, new IntWritable(maxValue));
    }
}
