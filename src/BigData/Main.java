package BigData;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
        
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
        
public class Main {
        
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        
    public void map(LongWritable key, Text value, Context context) 
    		throws IOException, InterruptedException {

    	String[] inputToken = value.toString().split(",");
    	
    	String type = inputToken[1];
    	
       	String latString = inputToken[inputToken.length-2];
       	String lngString = inputToken[inputToken.length-1];
    	
       	double lat = Double.parseDouble(latString);
    	double lng = Double.parseDouble(lngString);

    	context.write(new Text(type), new Text(lat + "," + lng));

    }
 }
 
 public static class Combine extends Reducer<Text, Text, Text, Text>{
	 public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
            try{
                // real price data path
            	Path pt=new Path("hdfs://localhost:9000/user/hduser/testA/Daan_Real_Price.csv");
                
            	FileSystem fs = FileSystem.get(new Configuration());
    			
                if(!fs.exists(pt)){
    				
                	System.err.println("File does not exists");
    			
                } else {
    				
    				FSDataInputStream in = fs.open(pt);
    				
    				BufferedReader br = new BufferedReader(new InputStreamReader(in));
                	
    				String line;
    				                	
                	line = br.readLine();
                	
                	ArrayList<String> locationList = new ArrayList<String>();
                	          
                	// store content of the iteration
            		for(Text val:values){
            			locationList.add(val.toString());
            			
            		}
                	int id = 0;
                	while (line != null){
                        id++;	
                		String[] stringToken = line.split(",");
                        	
                		//Use address to be the key. Maybe will change to index.
                       	String address = stringToken[1];
                       	String latString = stringToken[stringToken.length-2];
                       	String lngString = stringToken[stringToken.length-1];
                       	
                       	double lat = Double.parseDouble(latString);
                    	double lng = Double.parseDouble(lngString);
                    	
                    	int typeCounter = 0;
                    	
                		for(int i=0; i<locationList.size(); i++){

                			String[] location = locationList.get(i).split(",");
                			
                			//divide location to lat and lng
                			double locationLat = Double.parseDouble(location[0]);
                			double locationLng = Double.parseDouble(location[1]);
                			
//                			System.out.println("location: " + locationLat + ", " + locationLng);
                			double distance = Candidate.getShortestDistanceBetweenTowCandidates(lat, lng, locationLat, locationLng);
                			
                			if(distance < 1000) {
                        		context.write(new Text(address + "-" + key.toString()), new Text("1"));
                        		typeCounter++;
                			}
                			
                		}
                		
//                		System.out.println("address: " + address + " " + key + ": " + typeCounter);
                		
//                		context.write(new Text(address + "-" + key.toString()), new Text(Integer.toString(typeCounter)));

                        line = br.readLine();
                        
                	}
                	
                	in.close();
    			}

            } catch(Exception e){
            	e.printStackTrace();
            }	
					
		}
	}
        
 public static class Reduce extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {

    	System.out.println("********************************************************************");
		
		int counter = 0;
    	for(Text val:values){
			counter++;
		}
    	
		System.out.println(key + ": " + counter);
		
		context.write(key, new Text(Integer.toString(counter)));

    	
		System.out.println("********************************************************************");


    }
 }
        
 public static void main(String[] args) throws Exception {
	 
	Configuration conf = new Configuration();
    
    conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/core-site.xml"));
    conf.addResource(new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml"));
    conf.set("mapreduce.output.textoutputformat.separator", ", ");
    
    Job job = new Job(conf, "BigData");
    job.setJarByClass(Main.class);
        
    job.setMapperClass(Map.class);
    job.setCombinerClass(Combine.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path("test"));
    FileOutputFormat.setOutputPath(job, new Path("output"));
    
	
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
		

    FileSystemTools.checkAndDelete(conf, "output");
    
    boolean status = job.waitForCompletion(true);
    
	if(status){
		
		System.err.println("Integrate Alert Job Finished!");
		
	}
	else{
		
		System.err.println("Integrate Alert Job Failed!");
		
	}
	
	System.exit(1);
 }      
}
