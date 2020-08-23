package utils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ElMaper extends Mapper<LongWritable, Text, Text, Text> {
    

    public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
    	String fields[] = lineText.toString().split("\t"); // "origen,destino,fecha,monto"
		String keyFields = fields[0].concat("\t").concat(fields[1]).concat("\t").concat(fields[2]); //origen + destino + fecha
		Text key = new Text(keyFields);
		context.write(key, key);
    }
}