/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoopv1;

/**
 *
 * @author icooker811
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


import java.io.*;
import java.util.*;



import static ij.IJ.d2s;
import ij.IJ;
import ij.ImagePlus;
import ij.WindowManager;
import ij.gui.GenericDialog;
import ij.io.OpenDialog;
import ij.plugin.PlugIn;
import ij.process.ImageProcessor;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Map.Entry;

import com.labun.surf.IntegralImage;
import com.labun.surf.InterestPoint;
import com.labun.surf.Matcher;
import com.labun.surf.Params;
import com.labun.surf.Matcher.Point2Df;

public class HadoopMapper extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;
    
    String title = "SURF: Find Interest Points";
    List<InterestPoint> ipts1;
    List<InterestPoint> ipts2;
    
    public void map(LongWritable key, Text value,
            OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {

        String line = value.toString();
        String year = "";
        int airTemperature = 0;
        /*
        if (line.length() > 92 && line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
            airTemperature = Integer.parseInt(line.substring(88, 92));
        } else {
            if (line.length() > 92) {
                airTemperature = Integer.parseInt(line.substring(87, 92));
            }
        }
        if (line.length() > 93) {
            String quality = line.substring(92, 93);
            if (airTemperature != MISSING && quality.matches("[01459]")) {
                output.collect(new Text(year), new IntWritable(airTemperature));
            }
        }
        */
        
        
        try {
            Scanner in = new Scanner(new File("/home/tuk/TestFindMember/input/webclientSNSD_Twitter_BG_by_aznnerd09.jpg.txt"));

            // Header
            int iptsSize = in.nextInt(); // Number of Interest Points

            // Points
            ipts1 = new ArrayList<InterestPoint>(iptsSize);
            float x, y, strength, trace, scale, ori;
            int descSize;
            InterestPoint ipt;

            for (int i = 0; i < iptsSize; i++) {

                x = in.nextFloat();
                y = in.nextFloat();
                strength = in.nextFloat();
                trace = in.nextFloat();
                scale = in.nextFloat();
                ori = in.nextFloat();
                descSize = in.nextInt();
                ipt = new InterestPoint(x, y, strength, trace, scale);
                ipt.orientation = ori;
                if (descSize > 0) {
                    ipt.descriptor = new float[descSize];
                    for (int k = 0; k < descSize; k++) {
                        ipt.descriptor[k] = in.nextFloat();
                    }
                }
                ipts1.add(ipt);
            }

            in.close();


        } catch (Exception e) {
            IJ.error("SURF: loadPointsFromFile", e.getMessage());
        }
       
        try {

            Scanner in = new Scanner(line);
            
            int iptsSize = in.nextInt(); // Number of Interest Points
            ipts2 = new ArrayList<InterestPoint>(iptsSize);
            float x, y, strength, trace, scale, ori;
            int descSize;
            InterestPoint ipt;

            for (int i = 0; i < iptsSize; i++) {

                x = in.nextFloat();
                y = in.nextFloat();
                strength = in.nextFloat();
                trace = in.nextFloat();
                scale = in.nextFloat();
                ori = in.nextFloat();
                descSize = in.nextInt();

                 //System.out.println("namefile :" +x);
                ipt = new InterestPoint(x, y, strength, trace, scale);
                ipt.orientation = ori;
                if (descSize > 0) {
                    ipt.descriptor = new float[descSize];
                    for (int k = 0; k < descSize; k++) {
                        ipt.descriptor[k] = in.nextFloat();
                    }
                }
                ipts2.add(ipt);
            }

            year = in.next();
            in.close();


        } catch (Exception e) {
           
        }

        Map<InterestPoint, InterestPoint> matchedPoints = Matcher.findMathes(ipts1, ipts2);
        if (true) {
            Map<InterestPoint, InterestPoint> matchedPointsReverse = Matcher.findMathes(ipts2, ipts1);
            matchedPoints = intersection(matchedPoints, matchedPointsReverse);
        }


        if (matchedPoints.size() == 0) {
            IJ.showMessage(title, "No matches found.");
            return;
        }else{
             airTemperature = matchedPoints.size();
             output.collect(new Text(year), new IntWritable(airTemperature));
        
        }
        System.out.print(matchedPoints.size());
        
    }
    
    
    Map<InterestPoint, InterestPoint> intersection(Map<InterestPoint, InterestPoint> map1, Map<InterestPoint, InterestPoint> map2) {
        // take only those points that matched in the reverse comparison too
        Map<InterestPoint, InterestPoint> result = new HashMap<InterestPoint, InterestPoint>();
        for (InterestPoint ipt1 : map1.keySet()) {
            InterestPoint ipt2 = map1.get(ipt1);
            if (ipt1 == map2.get(ipt2)) {
                result.put(ipt1, ipt2);
            }
        }
        return result;
    }

    float[][] loadHomographyMatrixFromFile() {

        OpenDialog od = new OpenDialog("Choose a file containing 3x3 Homography Matrix" + " (" + title + ")", null);
        String dir = od.getDirectory();
        String fileName = od.getFileName();
        if (fileName == null) {
            return null;
        }
        String fullName = dir + fileName;
        float[][] res = new float[3][3];

        try {
            Scanner in = new Scanner(new File(fullName));
            for (int i = 0; i < 3; i++) {
                for (int j = 0; j < 3; j++) {
                    res[i][j] = in.nextFloat();
                }
            }
            in.close();
        } catch (FileNotFoundException e) {
            IJ.error("SURF: loadHomographyFromFile", e.getMessage());
            res = null;
        }

        return res;
    }
}
