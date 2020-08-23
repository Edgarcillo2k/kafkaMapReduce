package utils;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ElReducer extends Reducer<Text, Text, Text, FloatWritable> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	float sumaValues = 0;
    	for(Text value : values) {
            sumaValues ++;
    	}
        context.write(key, new FloatWritable(sumaValues));
    }
}
