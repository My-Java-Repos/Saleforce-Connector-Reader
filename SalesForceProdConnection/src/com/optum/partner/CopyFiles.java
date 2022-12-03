package com.org.partner;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class CopyFiles {
	
	
	public static void copytoInboundData() throws IOException {
		
		
		Properties prop = new Properties();
	    InputStream input = null;
	 
	    try {
	        input = new FileInputStream("C:\\VaggaRavi\\url.properties");
	         
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
	    
	    
	    String inboundfilesLocation=prop.getProperty("inboundfilesLocation");
	    String filesLocation = prop.getProperty("filesLocation");
	    
	    
	    //Writing files into the inbound folder
		FileWriter dataFileWriter = new FileWriter(inboundfilesLocation+"data/");
		FileWriter metaFileWriter = new FileWriter(inboundfilesLocation+"meta/");
		FileWriter ctlFileWriter = new FileWriter(inboundfilesLocation+"ctl/");
		
		
		FileReader dataFileReader = new FileReader(filesLocation+"data/");
		FileReader metaFileReader = new FileReader(filesLocation+"meta/");
		FileReader ctlFileReader = new FileReader(filesLocation+"ctl/");
		
		
		BufferedReader dataFileBufferedReader = new BufferedReader(dataFileReader);		
		BufferedReader metaFileBufferedReader = new BufferedReader(metaFileReader);
		BufferedReader ctlFileBufferedReader = new BufferedReader(ctlFileReader);

		String data = null;
		String meta=null;
		String ctl=null;
		
		//Writing Data into files
		while ((data = dataFileBufferedReader.readLine()) != null) {
			dataFileWriter.write(data+ System.lineSeparator());
		}
		
		while ((meta = metaFileBufferedReader.readLine()) != null) {
			metaFileWriter.write(meta+ System.lineSeparator());
		}
		
		while ((ctl = ctlFileBufferedReader.readLine()) != null) {
			ctlFileWriter.write(ctl+ System.lineSeparator());
		}
		
		
		dataFileBufferedReader.close();
		dataFileReader.close();
		dataFileWriter.close();
		
		metaFileBufferedReader.close();
		metaFileReader.close();
		metaFileWriter.close();
		
		ctlFileBufferedReader.close();
		ctlFileReader.close();
		ctlFileWriter.close();
		
		
		

		
		
		
	}

}
