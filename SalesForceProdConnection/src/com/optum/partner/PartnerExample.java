package com.org.partner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
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
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.commons.io.FileUtils;

import com.opencsv.CSVWriter;
import com.sforce.bulk.CsvWriter;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class PartnerExample {
	static final String USERNAME = "integration.user@org.com.acet";
	static final String PASSWORD = "acetapi$11appSk0HuMiN8MQS70MiuRCbm9";
	static PartnerConnection connection;
	public static final TimeZone CST = TimeZone.getTimeZone("CST");
	static Logger logger = Logger.getLogger("ACET_Log");
	static FileHandler fh;

	public static void main(String[] args) throws IOException {

		// Configuring the Connectors and setting the properties
		ConnectorConfig config = new ConnectorConfig();
		config.setUsername(USERNAME);
		config.setPassword(PASSWORD);
		config.setAuthEndpoint("https://login.salesforce.com/services/Soap/u/45.0");

		// Loading the properties for the filesLocation
		Properties prop = new Properties();
		/*ClassLoader loader = Thread.currentThread().getContextClassLoader();
		InputStream stream = loader.getResourceAsStream("produrl.properties");
		//InputStream stream = loader.getResourceAsStream("url.properties");
		prop.load(stream);*/

		InputStream input = null;
		 
	    try {
	       // input = new FileInputStream("C:\\VaggaRavi\\url.properties");
	    	input = new FileInputStream("./produrl.properties");
	        // load the properties file
	        prop.load(input);
	 
	    } catch (IOException ex) {
	        ex.printStackTrace();
	    } finally {
	        if (input != null) {
	            try {
	                input.close();
	            } catch (IOException e) {
	                e.printStackTrace();
	            }
	        }
	    }
		
		
		
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd'T'hhmmss");
		simpleDateFormat.setTimeZone(CST);
		String fileName = simpleDateFormat.format(new Date());

		String tablesLocation = prop.getProperty("tablesLocation");
		String filesLocation = prop.getProperty("filesLocation");
		String lastRunTimeLocation = prop.getProperty("lastRunTimeLocation");
		String reportsFileLocation = prop.getProperty("reportsFileLocation") + fileName + ".csv";
		String metadataFilesLocation = prop.getProperty("metadataFilesLocation");
		String inboundfilesLocation=prop.getProperty("inboundfilesLocation");

		
		//cleaning files in data,meta and ctl folders
		FileUtils.cleanDirectory(new File(filesLocation+"/data"));
		FileUtils.cleanDirectory(new File(filesLocation+"/meta"));
		FileUtils.cleanDirectory(new File(filesLocation+"/ctl"));
		//FileUtils.cleanDirectory(new File(inboundfilesLocation));
		
		
		File file = new File(reportsFileLocation);

		// create FileWriter object with file as parameter
		FileWriter outputfile = new FileWriter(file);

		// create CSVWriter object filewriter object as parameter
		CSVWriter writer = new CSVWriter(outputfile);

		// adding header to csv
		String[] header = { "FileName", "Dat Record Count", "Meta Record Count" };
		writer.writeNext(header);

		fh = new FileHandler(prop.getProperty("logFileLocation") + fileName + ".log");
		logger.addHandler(fh);
	    SimpleFormatter formatter = new SimpleFormatter();
		fh.setFormatter(formatter);

		// Writing the data into the above loading file locations
		try {
			FileReader fileReader = new FileReader(tablesLocation);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			List<String> tableNames = new ArrayList<String>();
			String line;
			while ((line = bufferedReader.readLine()) != null) {
				tableNames.add(line);
			}

			bufferedReader.close();
			fileReader.close();

			FileReader lastRutimeFile = new FileReader(lastRunTimeLocation);
			BufferedReader lastRutimeFileBufferedReader = new BufferedReader(lastRutimeFile);
			String data;
			String time = null;
			while ((data = lastRutimeFileBufferedReader.readLine()) != null) {
				time = data;
			}

			lastRutimeFileBufferedReader.close();
			lastRutimeFile.close();
			// Passing configuration to connection object
			connection = Connector.newConnection(config);
			// If connection is successful session Id can be obtained with below line
			//tableNames.add("PROJECT_DETAILS__C");
			if (config.getSessionId() != null || config.getSessionId() != "") {
				for (String table : tableNames) {
					List<String> metaFileList=new ArrayList<String>();
					List<String> salesForceMeta=new ArrayList<String>();
					List<String> finalMetaDataList=new ArrayList<String>();
					metaFileList.clear();
					salesForceMeta.clear();
					finalMetaDataList.clear();
					String objectToDescribe = table;
					DescribeSObjectResult[] dsrArray = connection.describeSObjects(new String[] { objectToDescribe });

					// Meta File Generation based on the above query

					FileReader metadataFile = new FileReader(metadataFilesLocation + "ACT_ACTQA_" + table + ".meta");
					BufferedReader metadataFileBufferedReader = new BufferedReader(metadataFile);

					metadataFileBufferedReader.readLine();
					String metadata = null;
					int j = 0;
					 for (int i = 0; i < dsrArray[0].getFields().length; i++) {
						 Field field = dsrArray[0].getFields()[i]; 
						 salesForceMeta.add(field.getName());
						 j++;
					 }
					 
					
					
					
					while ((metadata = metadataFileBufferedReader.readLine()) != null) {

						metaFileList.add(metadata.split("\\|")[3]);
						//j++;
					}
					
				for(int i=0;i<metaFileList.size();i++) {
					for(int k=0;k<salesForceMeta.size();k++) {
						if(salesForceMeta.get(k).equalsIgnoreCase(metaFileList.get(i))) {
							finalMetaDataList.add(salesForceMeta.get(k));
							break;
						}
					}
				}
				
					metadataFileBufferedReader.close();
					metadataFile.close();
					String sqlQuery = "select ";
					for (int i = 0; i < salesForceMeta.size(); i++) {
						
						if (i + 1 != salesForceMeta.size())
							sqlQuery += salesForceMeta.get(i) + ",";
						else
							sqlQuery += salesForceMeta.get(i);
						
					}
					// sqlQuery+=" from "+table+" where isDeleted=false";
					sqlQuery += " from " + table;
					if (time != null)
						sqlQuery += " where  LASTMODIFIEDDATE>"+time;
					try {
						QueryResult queryResults = connection.queryAll(sqlQuery);

						// Generating the dat and ctl files based on the above query resutls
						queryGeneration(queryResults, table, salesForceMeta, filesLocation, dsrArray, writer, j);
						// System.out.println(sqlQuery);

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}

			// Setting the Time Format
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
			sdf.setTimeZone(CST);
			final String ISO_8601_24H_FULL_FORMAT = sdf.format(new Date());

			// Printing the last run time in the below mentioned Location
			FileWriter lastRunTimeWriter = new FileWriter(lastRunTimeLocation);

			lastRunTimeWriter.write(ISO_8601_24H_FULL_FORMAT);
			lastRunTimeWriter.close();
			// closing writer connection
			writer.close();

			CopyDirectoryExample.copyFolders(filesLocation+"/data", inboundfilesLocation);
			CopyDirectoryExample.copyFolders(filesLocation+"/meta", inboundfilesLocation);
			Thread.sleep(1000);
			CopyDirectoryExample.copyFolders(filesLocation+"/ctl", inboundfilesLocation);
			SendMail.sendAttachmentEmail(reportsFileLocation);
		} catch (ConnectionException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void queryGeneration(QueryResult queryResults, String tableName, List<String> columns,
			String filesLocation, DescribeSObjectResult[] dsrArray, CSVWriter writer, int metaRecordCount)
			throws IOException, ConnectionException {

		SimpleDateFormat time = new SimpleDateFormat("hh:mm:ss");
		SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat dateandTime = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		String[] controlData = new String[21];

		// Query results can be be convereted to sobject array using below method

		int noOfRecords = 0;
		char ctrlA = 0x1;
		char ctrlB = 0x2;
		boolean done = false;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd'T'hhmmss");
		simpleDateFormat.setTimeZone(CST);
		String fileName = simpleDateFormat.format(new Date());

		FileWriter dataFileWriter = new FileWriter(filesLocation+"data/ACT_ACTQA_" + tableName + "_" + fileName + ".dat");
		if (queryResults.getSize() > 0) {

			while (!done) {

				SObject[] sObjList = queryResults.getRecords();
				noOfRecords = noOfRecords + sObjList.length;
				StringBuffer data = new StringBuffer();
				for (SObject so : sObjList) {
					int endOfColumns = columns.size() - 1;
					for (int i = 0; i < columns.size(); i++) {
						if (so.getField(columns.get(i)) == null)
							data.append("");
						else
							data.append(so.getField(columns.get(i)));

						if (i == endOfColumns)
							data.append(ctrlB);
						else
							data.append(ctrlA);

					}
					data.append(System.lineSeparator());

				}
				if (queryResults.isDone())
					done = true;
				else
					queryResults = connection.queryMore(queryResults.getQueryLocator());

				// Writing data File
				dataFileWriter.write(data.toString());

			}
		}
		
		dataFileWriter.close();
		
		generateMetaFile(dsrArray, tableName, filesLocation, fileName);
		FileWriter fileWriter = new FileWriter(filesLocation+"ctl/ACT_ACTQA_" + tableName + "_" + fileName + ".ctl");

		controlData[0] = "SourceName=ACT";
		controlData[1] = "SchemaName=ACT_QA";
		controlData[2] = "EntityName=" + tableName;
		controlData[3] = "RecordCount=" + noOfRecords;
		controlData[4] = "ExtractDate=" + date.format(new Date());
		controlData[5] = "ExtractTime=" + time.format(new Date());
		controlData[6] = "FileTimeStamp=" + time.format(new Date());
		controlData[7] = "ExtractDateTime=" + dateandTime.format(new Date());
		controlData[8] = "ETLLandingDirectory=/mapr/datalake/orglake/tst/t_inbound/org/act";
		controlData[9] = "FileLoadType=FULL";
		controlData[10] = "ETLAppUserId=CDC";
		controlData[11] = "ExtractStatus=SUCCESS";
		controlData[12] = "SourceTimeZone=CDT";
		controlData[13] = "EffectiveDate=" + date.format(new Date());
		controlData[14] = "DataFileExtension=.dat";
		controlData[15] = "AdditionalFieldExtensions=.meta, .ctl";
		controlData[16] = "FileFormatType=Delimited";
		controlData[17] = "ETLProcess=push";
		controlData[18] = "Frequency=daily";
		controlData[19] = "RecordDelimiter='^B\\n'";
		controlData[20] = "FileDelimiter='^A'";
		for (int i = 0; i < 21; i++) {
			// Writing ctl File
			fileWriter.write(controlData[i] + System.lineSeparator());
		}

		fileWriter.close();
		

		logger.info(tableName + " dat file has been written");
		logger.info(noOfRecords + " files have been written");
		//int metaFileCount =
				

		logger.info(tableName + " ctl file has been written");

		// Writing meta File

		logger.info(tableName + " table Done Writing");

		// add data to csv
		String[] data1 = { tableName + "_" + fileName + ".dat", String.valueOf(noOfRecords),
				String.valueOf(metaRecordCount) };
		writer.writeNext(data1);

	}

	/**
	 * @param dsrArray
	 * @throws IOException
	 */
	private static void generateMetaFile(DescribeSObjectResult[] dsrArray, String tableName, String filesLocation,
			String fileName) throws IOException {
		// TODO Auto-generated method stub

		FileWriter metaFileWriter = new FileWriter(filesLocation +"meta/ACT_ACTQA_"+ tableName + "_" + fileName + ".meta");
		String header = "Source Name|Schema_Name|TABLE_NAME|COLUMN_NAME|DATA_TYPE|DATA_LENGTH|DATA_SCALE|Format|Primary_Keys|COLUMN_ID"
				+ System.lineSeparator();
		metaFileWriter.write(header);

		int CID = 0;
		DescribeSObjectResult dsr = dsrArray[0];
		String[] columns = new String[dsr.getFields().length];
		for (int i = 0; i < dsr.getFields().length; i++) {
			// Get the field
			Field field = dsr.getFields()[i];

			// Write some field properties
			// System.out.println("Field name: " + field.getName());
			String Name = field.getName();
			String Type = (field.getType()).toString();
			String Format = "";
			String PK = "N";
			CID = i + 1;
			String[] myArray = { "INT", "DOUBLE", "FLOAT", "DATE", "DATETIME", "BOOLEAN" };
			int Length = field.getLength();
			int Scale = field.getScale();
			if (Type.equalsIgnoreCase("double") || Type.equalsIgnoreCase("currency")) {
				Length = field.getPrecision();
				Scale = field.getScale();

			}
			if (Name.equalsIgnoreCase("Id"))
				PK = "Y";

			if (Arrays.asList(myArray).contains(Type.toUpperCase())) {
				Type = ((field.getType()).toString()).toUpperCase();
			} else {
				Type = "STRING";
			}
			if (Type.equalsIgnoreCase("date")) {
				Type = "DATE";
				Length = 0;
				Scale = 0;
				Format = "yyyy-MM-dd";
			}

			if (Type.equalsIgnoreCase("datetime")) {
				Format = "yyyy-MM-dd'T'HH:mm:ss'.000Z'";
				Type = "DATE";
				Length = 0;
				Scale = 0;
			}

			columns[i] = field.getName();
			String fileContent = "ACT|ACT_QA|" + (dsr.getName()).toString().toUpperCase() + "|"
					+ field.getName().toUpperCase() + "|" + Type + "|" + Length + "|" + Scale + "|" + Format + "|" + PK
					+ "|" + CID + System.lineSeparator();
			metaFileWriter.write(fileContent);

		}
		

		logger.info(tableName + " meta file has been written");

		
		
		/*FileReader metadataFile = new FileReader("./metadata/" + "ACT_ACTQA_" + tableName + ".meta");
		//FileReader metadataFile = new FileReader("C:\\VaggaRavi\\metadata\\" + "ACT_ACTQA_" + tableName + ".meta");
		BufferedReader metadataFileBufferedReader = new BufferedReader(metadataFile);

		String metadata = null;
		
		while ((metadata = metadataFileBufferedReader.readLine()) != null) {
			String data=metadata+ System.lineSeparator();
			metaFileWriter.write(data);
		}
		metadataFileBufferedReader.close();
		metadataFile.close();*/
		metaFileWriter.close();
	}
}
