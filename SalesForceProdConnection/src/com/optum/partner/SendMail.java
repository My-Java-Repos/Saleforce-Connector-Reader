package com.org.partner;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import com.opencsv.CSVWriter;

public class SendMail {


	
	/**
	 * Utility method to send email with attachment
	 * @param session
	 * @param toEmail
	 * @param subject
	 * @param body
	 */
	
	public static void sendAttachmentEmail(String fileName){
		try{
			
			
			Properties prop = new Properties();
		    InputStream input = null;
		 
		    try {
		       input = new FileInputStream("./produrl.properties");
		    	//input = new FileInputStream("C:\\VaggaRavi\\url.properties");
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
			
			
			

			// Get system properties
		      Properties properties = System.getProperties();

		      // Setup mail server
		      properties.setProperty("mail.smtp.host", "mailo2.org.com");
		      
		      

		      // Get the default Session object.
		      Session session = Session.getDefaultInstance(properties);
		      
		      
		      String body="Hi,\r\n" + 
		      		"	 Please find attached Test Report.\r\n" + 
		      		"\r\n" + 
		      		"Regards,\r\n" + 
		      		"Datalake Team\r\n" ;
			
		      String subject="ACET PROD DL file count test results";
			
		      String toEmail=prop.getProperty("toEmail");
		      String ccMail=prop.getProperty("ccMail");
		      String fromMail=prop.getProperty("fromMail");
		      InternetAddress address=new InternetAddress();
		      address.setAddress(ccMail);
		      
		      
	         MimeMessage msg = new MimeMessage(session);
	         msg.addHeader("Content-type", "text/HTML; charset=UTF-8");
		     msg.addHeader("format", "flowed");
		     msg.addHeader("Content-Transfer-Encoding", "8bit");
		      
		     msg.setFrom(new InternetAddress(fromMail, "DataLake"));

		     msg.setReplyTo(InternetAddress.parse(toEmail, false));
		     msg.addRecipient(Message.RecipientType.CC, address);

		     msg.setSubject(subject, "UTF-8");

		     msg.setSentDate(new Date());

		     msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toEmail, false));
		      
	         // Create the message body part
	         BodyPart messageBodyPart = new MimeBodyPart();

	         // Fill the message
	         messageBodyPart.setText(body);
	         
	         // Create a multipart message for attachment
	         Multipart multipart = new MimeMultipart();

	         // Set text message part
	         multipart.addBodyPart(messageBodyPart);

	         // Second part is attachment
	         messageBodyPart = new MimeBodyPart();
	         DataSource source = new FileDataSource(fileName);
	         messageBodyPart.setDataHandler(new DataHandler(source));
	         messageBodyPart.setFileName(fileName);
	         multipart.addBodyPart(messageBodyPart);

	         // Send the complete message parts
	         msg.setContent(multipart);

	         // Send message
	         Transport.send(msg);
	         System.out.println("EMail Sent Successfully with attachment!!");
	      }catch (MessagingException e) {
	         e.printStackTrace();
	      } catch (UnsupportedEncodingException e) {
			 e.printStackTrace();
		}
}
	
	

	
}
