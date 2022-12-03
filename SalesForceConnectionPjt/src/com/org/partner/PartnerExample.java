package com.org.partner;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;


public class PartnerExample {
            static final String USERNAME = "datalake.integrationtest@org.com.acet.preprod";
            static final String PASSWORD ="usiw7sv2b!";
            static PartnerConnection connection;
            public static final TimeZone UTC = TimeZone.getTimeZone("UTC");
          
           static String fileName=null;

public static void main(String[] args) throws IOException {
	
	//Configuring the Connectors and setting the properties
    ConnectorConfig config = new ConnectorConfig();
    config.setUsername(USERNAME);
    config.setPassword(PASSWORD);
    config.setAuthEndpoint("https://cs30.salesforce.com/services/Soap/u/45.0");
    
    //Loading the properties for the filesLocation
    Properties prop = new Properties();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();           
    InputStream stream = loader.getResourceAsStream("url.properties");
    prop.load(stream);
    
    
   String tablesLocation=prop.getProperty("tablesLocation");
   String filesLocation=prop.getProperty("filesLocation");
   String lastRunTimeLocation=prop.getProperty("lastRunTimeLocation");
   
   SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyyMMdd'T'hhmmss");
   simpleDateFormat.setTimeZone(UTC);
    fileName = simpleDateFormat.format(new Date());

   
   //Writing the data into the above loading file locations
    try {
        FileReader fileReader=new FileReader(tablesLocation);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        List<String> tableNames = new ArrayList<String>();
        String line;
        while ((line = bufferedReader.readLine()) != null ) {
        	tableNames.add(line);
        }
       
        bufferedReader.close();
        
        
        FileReader lastRutimeFile=new FileReader(lastRunTimeLocation);
        BufferedReader lastRutimeFileBufferedReader = new BufferedReader(lastRutimeFile);
        String data;
        String time=null;
        while ((data = lastRutimeFileBufferedReader.readLine()) != null ) {
        	time=data;
        }
       
        lastRutimeFileBufferedReader.close();
        //Passing configuration to connection object    
        connection = Connector.newConnection(config);
        //If connection is successful session Id can be obtained with below line

        
      
        if(config.getSessionId() != null || config.getSessionId() !=""   ) {
        	for(String table:tableNames) {
        	String objectToDescribe = table;
            DescribeSObjectResult[] dsrArray = connection.describeSObjects(new String[] { objectToDescribe });
          
            //Meta File Generation based on the above query
           String columns[]=generateMetaFile(dsrArray,table,filesLocation);
            String sqlQuery="select ";
        	for(int i=0;i<columns.length;i++) {
        		if(i+1!=columns.length)
        		 sqlQuery+=columns[i]+",";
        		else
        			sqlQuery+=columns[i];
        	}
        	//sqlQuery+=" from "+table+" where isDeleted=false";
        	sqlQuery+=" from "+table;
        	if(time!=null)		
        	  sqlQuery+=" where LASTMODIFIEDDATE>2019-02-24T00:00:29.000Z and LASTMODIFIEDDATE<=2019-02-25T00:00:22.000Z";
        	try {
        	 QueryResult queryResults = connection.queryAll(sqlQuery); 
        	 //Generating the dat and ctl files based on the above query resutls
            queryGeneration(queryResults,table,columns,filesLocation);
            
            
        	}catch(Exception e) {
        		e.printStackTrace();
        	}
          }
        }
        
        //Setting the Time Format
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        sdf.setTimeZone(UTC);
         final String ISO_8601_24H_FULL_FORMAT = sdf.format(new Date());
        
      //Printing the last run time in the below mentioned Location
         FileWriter lastRunTimeWriter=new FileWriter(lastRunTimeLocation);

        lastRunTimeWriter.write(ISO_8601_24H_FULL_FORMAT);
        lastRunTimeWriter.close();
        
    }catch (ConnectionException e1) {
        e1.printStackTrace();
    }  
    catch (IOException e1) {
        e1.printStackTrace();
    }  
}
private static void queryGeneration(QueryResult queryResults,String tableName,String[] columns,String filesLocation) throws IOException {
	
	   
    FileWriter dataFileWriter = new FileWriter(filesLocation+tableName+"_"+fileName+".dat");

   
      FileWriter fileWriter = new FileWriter(filesLocation+tableName+"_"+fileName+".ctl");
      SimpleDateFormat time=new SimpleDateFormat("hh:mm:ss");
      SimpleDateFormat date=new SimpleDateFormat("yyyy-MM-dd");
      SimpleDateFormat dateandTime=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
      String[] controlData=new String[21];

  
      //Query results can be be convereted to sobject array using below method
      SObject[] sObjList = queryResults.getRecords();
      int noOfRecords=sObjList.length;
      char ctrlA = 0x1;
      char ctrlB = 0x2;
      for(SObject so:sObjList){
    	 StringBuffer data=new StringBuffer();
    	 int endOfColumns=columns.length-1;
    	 for(int i=0;i<columns.length;i++) {
    		if(so.getField(columns[i])==null)
    			data.append("");
    		else
    		 data.append(so.getField(columns[i]));
    		 
    		 
    		 if(i==endOfColumns)
    			 data.append(Character.toString(ctrlB));
    		 else
    			 data.append(Character.toString(ctrlA));
    		
    	 }
    	 data.append(System.lineSeparator());
    	 dataFileWriter.write(data.toString());
      }
      controlData[0]="SourceName=ACT";
      controlData[1]="SchemaName=ACT_QA";
      controlData[2]="EntityName="+tableName;
      controlData[3]="RecordCount="+sObjList.length;
      controlData[4]="ExtractDate="+date.format(new Date());
      controlData[5]="ExtractTime="+time.format(new Date());
      controlData[6]="FileTimeStamp="+time.format(new Date());
      controlData[7]="ExtractDateTime="+dateandTime.format(new Date());
      controlData[8]="ETLLandingDirectory=/mapr/datalake/orglake/tst/t_inbound/org/act";
      controlData[9]="FileLoadType=FULL";
      controlData[10]="ETLAppUserId=CDC";
      controlData[11]="ExtractStatus=SUCCESS";
      controlData[12]="SourceTimeZone=CDT";
      controlData[13]="EffectiveDate="+date.format(new Date());
      controlData[14]="DataFileExtension=.dat";
      controlData[15]="AdditionalFieldExtensions=.meta, .ctl";
      controlData[16]="FileFormatType=Delimited";
      controlData[17]="ETLProcess=push";
      controlData[18]="Frequency=daily";
      controlData[19]="RecordDelimiter='^B\\n'";
      controlData[20]="FileDelimiter='^A'";
      for(int i=0;i<21;i++) {
    	  fileWriter.write(controlData[i]+System.lineSeparator());
      }
      
      
      fileWriter.close();
      dataFileWriter.close();
}
/**
* @param dsrArray
 * @throws IOException 
*/
private static String[] generateMetaFile(DescribeSObjectResult[] dsrArray,String tableName,String filesLocation) throws IOException {
            // TODO Auto-generated method stub
      

	FileWriter fileWriter = new FileWriter(filesLocation+tableName+"_"+fileName+".meta");
	String header="Source Name|Schema_Name|TABLE_NAME|COLUMN_NAME|DATA_TYPE|DATA_LENGTH|DATA_SCALE|Format|Primary_Keys|COLUMN_ID"+System.lineSeparator();
    fileWriter.write(header); 
	DescribeSObjectResult dsr = dsrArray[0];
     String[] columns=new String[dsr.getFields().length];
     for (int i = 0; i < dsr.getFields().length; i++) {
         // Get the field
         Field field = dsr.getFields()[i];
         
         // Write some field properties
         // System.out.println("Field name: " + field.getName());
         String Name=field.getName();
         String Type = (field.getType()).toString();
         String Format = "";
         String PK="N";
         int CID = i+1;
         String[] myArray = {"INT","DOUBLE","FLOAT","DATE","DATETIME","BOOLEAN"};
         int Length =field.getLength();
         int Scale=field.getScale();
         if (Type.equalsIgnoreCase("double") || Type.equalsIgnoreCase("currency"))
         {
             Length = field.getPrecision();
             Scale =field.getScale();
            
         }
         if (Name.equalsIgnoreCase("Id"))
             PK = "Y";
         
         if (Arrays.asList(myArray).contains(Type.toUpperCase())) {
             Type = ((field.getType()).toString()).toUpperCase();
         }
         else{
             Type ="STRING";
         }
         if (Type.equalsIgnoreCase("date") ) {
             Type ="DATE";
             Length = 0;
             Scale =0;
             Format = "yyyy-MM-dd";
         }
             
         if (Type.equalsIgnoreCase("datetime")) {
             Format = "yyyy-MM-dd'T'HH:mm:ss'.000Z'";
             Type ="DATE";
             Length = 0;
             Scale =0;
             }
        
         String fileContent="ACT|ACT_QA|"+(dsr.getName()).toString().toUpperCase()+"|"+field.getName().toUpperCase()+"|"+Type+"|"+Length+"|"+Scale+"|"+Format+"|"+PK+"|"+CID+System.lineSeparator();
         
        columns[i]=field.getName();
         
        fileWriter.write(fileContent);
     }
     fileWriter.close();
     return columns;
}
}
