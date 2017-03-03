import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.mail.BodyPart;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.apache.commons.mail.util.MimeMessageParser;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClient;
import com.amazonaws.services.simpleemail.model.RawMessage;
import com.amazonaws.services.simpleemail.model.SendRawEmailRequest;

/*
 * This AWS lambda function will get triggered when an email is sent to SES and that email object is places in S3 bucket.
 * Email S3 bucket should define an create/put event to trigger this lambda.
 * 
 * This handler parses the raw email with MimeMessageParser and it requires commons-email-1.3.1.jar.
 * It also needs to parse the html content and requires jsoup-1.10.2.jar
 */
public class SesEmailForwardManager implements RequestHandler<S3Event, Object> {

	private static LambdaLogger logger = null;
	
    @Override
    public Object handleRequest(S3Event input, Context context) {
        logger = context.getLogger(); 
        
		if (input == null ||
			input.getRecords() == null	||
			input.getRecords().size() < 1){
				logger.log("*** S3Event input object is empty ***");
				return null;
		}
 
		String bucketName = input.getRecords().get(0).getS3().getBucket().getName();
		String key = input.getRecords().get(0).getS3().getObject().getKey();		
		logger.log("** Bucket: " + bucketName + ", key : " + key);
		
		//Get the email object (stream) from bucket for this given key
		AmazonS3 s3Client = new AmazonS3Client();
		S3Object s3object = s3Client.getObject(bucketName, key);
		
		//Configurable items
		String[] toAddresses = {"to_addr1@domain.com", "to_addr2@domain.com"}; // CHANGE THE IDs
		String fromAddress = "from_addr@domain.com"; // This should be a AWS SES verified email Id. 
		String subject = null;
		String content = null;
		List<DataSource> attachments = null;
		String SUBJECT_PREFIX = "SES FW: ";

		try {
			//Parse raw email and get the subject, body and attachments. 
		    MimeMessage emailMessage = new MimeMessage(null, s3object.getObjectContent());
		    MimeMessageParser parser = new MimeMessageParser(emailMessage);
		    parser.parse();
	
		    if (parser.hasHtmlContent()){	    
		      	Document doc = Jsoup.parse(parser.getHtmlContent());
		      	content = doc.outerHtml();
		    } 
		    else if (parser.hasPlainContent()){
		    	content = parser.getPlainContent();
		    }
		    
		    subject = parser.getSubject();
		    attachments = parser.getAttachmentList();
	    
		    //Prepare to send email
		 	Properties props=new Properties();
			Session mailSession = Session.getDefaultInstance(props);
			InternetAddress[] toInetAddresses = new InternetAddress[toAddresses.length];
			for (int i = 0; i < toAddresses.length; i++){
				toInetAddresses[i] = new InternetAddress(toAddresses[i]);
			}
			
			// Create an email message and set To, From, Subject, Body & Attachment to it.
			MimeMessage msg = new MimeMessage(mailSession);
				msg.setFrom(new InternetAddress(fromAddress));
				msg.setRecipients(javax.mail.Message.RecipientType.TO, toInetAddresses);
				msg.setSubject(SUBJECT_PREFIX + subject);

			//Create message part
			BodyPart part = new MimeBodyPart();
			part.setContent(content.toString(), "text/html");

			//Add a MIME part to the message
			MimeMultipart mp = new MimeMultipart();
			mp.addBodyPart(part);
			
			//Add attachments
			for(DataSource source : attachments){
				BodyPart mbpAttachment = new MimeBodyPart();
				mbpAttachment.setDataHandler(new DataHandler(source));
				mbpAttachment.setFileName(source.getName());
				mp.addBodyPart(mbpAttachment);
			}
			
			msg.setContent(mp);
			logger.log("Message created : " + msg);

			// Write the raw email content to stream
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			msg.writeTo(out);
			
			RawMessage rm = new RawMessage();
			rm.setData(ByteBuffer.wrap(out.toString().getBytes()));

			//Send email with Amazon SES client
			AmazonSimpleEmailServiceClient client = new AmazonSimpleEmailServiceClient();
			Region REGION = Region.getRegion(Regions.US_WEST_2);
			client.setRegion(REGION);
			client.sendRawEmail(new SendRawEmailRequest().withRawMessage(rm));
			
			logger.log("Email forwarded successfully.");
		} catch (IOException e) {
			logger.log("Exception : " + e.getMessage());				
		} catch (AddressException e) {
			logger.log("Exception : " + e.getMessage());
		} catch (IllegalArgumentException e) {
			logger.log("Exception : " + e.getMessage());
		} catch (MessagingException e) {
			logger.log("Exception : " + e.getMessage());
		} catch (Exception e) {
			  logger.log("Exception : " + e.getMessage());
		}
	    
        return "Success";
    }


}
