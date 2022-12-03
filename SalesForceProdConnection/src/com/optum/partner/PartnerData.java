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

public class PartnerData {
	static final String USERNAME = "integration.user@org.com.acet";
	static final String PASSWORD = "acetapi$11appSk0HuMiN8MQS70MiuRCbm9";
	static PartnerConnection connection;
	public static final TimeZone CST = TimeZone.getTimeZone("CST");
	static Logger logger = Logger.getLogger("ACET_Log");
	static FileHandler fh;

	

	public static void queryGeneration(QueryResult queryResults, String tableName, List<String> columns,
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
	public static void generateMetaFile(DescribeSObjectResult[] dsrArray, String tableName, String filesLocation,
			String fileName) throws IOException {
		// TODO Auto-generated method stub

		FileWriter metaFileWriter = new FileWriter(filesLocation +"meta/ACT_ACTQA_"+ tableName + "_" + fileName + ".meta");


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
			String fileContent =field.getName().toUpperCase() + System.lineSeparator();
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
