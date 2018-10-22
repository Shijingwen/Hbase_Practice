/**  

* @author ShiJingwen

* @versionVersion 1.00

*/ 
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.lang.String;
import java.lang.StringBuilder;
import java.lang.Double;
import java.lang.Integer;
import java.text.NumberFormat;
import java.math.RoundingMode;
import java.text.DecimalFormat;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.log4j.*;


public class Hw1Grp3 {

         /*static configure HBase*/
        static Configuration configuration = HBaseConfiguration.create();
	static String tableName= "Result";

	
	/**
	 * creat hbase table
         * @return void
	 * @throws IOException 
	 * @throws ZooKeeperConnectionException 
	 * @throws MasterNotRunningException 
	 */
	public static void CreateTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{   
                HBaseAdmin hAdmin = new HBaseAdmin(configuration);
		if(hAdmin.tableExists(tableName)){
		    System.out.println("Table already exists,delete...");
            	    hAdmin.disableTable(tableName);
	            hAdmin.deleteTable(tableName);
                    System.out.println("Finish deleting...");
		}
                HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
                htd.addFamily(new HColumnDescriptor("res"));
                hAdmin.createTable(htd);
                System.out.println("table "+tableName+ " created successfully");
                hAdmin.close();
	}
    	
    	/**
	 * put a new record into hbase
         * @return void 
	 * @param tableName name of table
	 * @param row key of rows
	 * @throws IOException 
	 */
    public static void PutData(HTable table,String[] funOrder,String[] res,int count) throws IOException{
         int pos2 = 1;
         Put put = new Put(res[0].getBytes());
         NumberFormat nf2 = NumberFormat.getNumberInstance();
	 nf2.setMaximumFractionDigits(2);
         for(int i=0;i<funOrder.length;i++){
             if(funOrder[i].contains("count")){
	         put.add("res".getBytes(),funOrder[i].getBytes(),String.valueOf(count).getBytes());
    		 table.put(put);  
                 
             }  
	     else if(funOrder[i].contains("avg")){
		 DecimalFormat df = new DecimalFormat("###0.00");
		 df.setRoundingMode(RoundingMode.HALF_UP);
		 String strAvg = String.valueOf(df.format(Double.valueOf(res[pos2])/count));
		 put.add("res".getBytes(),funOrder[i].getBytes(),strAvg.getBytes());
    		 table.put(put);
		 
		 pos2++;			
		 }
                 else if(funOrder[i].contains("max")){
		     put.add("res".getBytes(),funOrder[i].getBytes(),res[pos2].getBytes());
    		     table.put(put);                   
		     pos2++;
		 }
            }   
                           
    }

	
	/**
	 * main
         * @return void
	 * @param args[] command from shell
	 * @throws IOException 
	 */
    public static void main(String[] args) throws  MasterNotRunningException, ZooKeeperConnectionException, IOException, URISyntaxException{
        
         /* Split args*/
        String appendPath = args[0].split("=")[1];
        
        String group = args[1].split(":")[1];
        int groupKey = Integer.valueOf(group.substring(1,group.length())).intValue();
        
        String[] funOrder = args[2].split(":")[1].split(",");
  
        /* Create hbase table*/         
        Logger.getRootLogger().setLevel(Level.WARN);
        CreateTable();
        HTable table = new HTable(configuration, tableName);
        String file= "hdfs://localhost:9000/"+appendPath;

        /* Configure dfs file*/  
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        FSDataInputStream in_stream = fs.open(path);
        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));

       
        /* Read every line of the file and extract colum*/  
        String s;//Save every line of the file
        ArrayList<String> list = new ArrayList<String>(); //Extract useful information and save as one string in list
        while ((s=in.readLine())!=null) {
             int pos = 1;
             String[] strArray = s.split("\\|");
             String[] strExtract = new String[funOrder.length+1]; 
             strExtract[0]=strArray[groupKey];
             for(int i=0;i<funOrder.length;i++){
                if(funOrder[i].contains("avg")){
			int rowEvg =Integer.valueOf(funOrder[i].split("R")[1].substring(0,funOrder[i].split("R")[1].length()-1)).intValue();
			strExtract[pos++]=strArray[rowEvg];			                        
		}
                else if(funOrder[i].contains("max")){
			int rowMax =Integer.valueOf(funOrder[i].split("R")[1].substring(0,funOrder[i].split("R")[1].length()-1)).intValue();
                        strExtract[pos++]=strArray[rowMax];
		}
	}

            list.add(StringUtils.join(strExtract,","));
        }

         /* Sorted List*/
        Collections.sort(list);     	   

        /*Statistic and write in hbase*/
        if (null != list && list.size() > 0) {
             String[] before = list.get(0).split(",");
             String[] res = before;
             
             int num = 1;
	     int count = 1;
             NumberFormat nf = NumberFormat.getInstance();
             nf.setGroupingUsed(false);

             for(int k = 1;k<list.size();k++){
                        String index = list.get(k);
                 	int pos1 = 1;    
                 	String[] string = index.split(",");
                 	if(string[0].equals(res[0]))
                 	{
                        	count++;
                        	for(int i=0;i<funOrder.length;i++){
                            		if(funOrder[i].contains("avg")){
		                  		res[pos1] =   nf.format(Double.valueOf(res[pos1])+ Double.valueOf(string[pos1]));
                                  		pos1++;
		           		 }
                            		else if(funOrder[i].contains("max")){
				  		if(Double.valueOf(string[pos1]) > Double.valueOf(res[pos1])) res[pos1]= string[pos1];
                                  		pos1++;
		            		}
                       	 	}
   

                 	}
                 	else
                 	{       num++;
                        	PutData(table,funOrder,res,count);
                        	before = string;
                        	count = 1;
                        	res = before;
                        
                 	}
                
	     }           
            
            PutData(table,funOrder,res,count); 
            System.out.println("Gourp num:"+num);
        }
        table.close(); 
        in.close();
        fs.close();
    }
}
