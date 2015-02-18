import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;

public class LocalApplication {
	
	final static Tag MANAGER_TAG = new Tag("name","manager");
	private static String localApplicationId;

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();

		String inputFileName = args[0];
		String outputFileName = args[1];
		int n_workers = Integer.parseInt(args[2]);
		boolean terminate = false;

		if (args.length > 3 && args[3].equals("terminate")) {
			terminate=true;
		}

		AWSObject awsObject = new AWSObject();
		awsObject.initEC2();
		awsObject.initS3();
		localApplicationId = UUID.randomUUID().toString();

		String mangerInstanceID;
		try {

			/*Step 1 - Check if Manager node is active on the EC2 Cloud. If it's not, the application will start the manager node*/
			mangerInstanceID = checkIfManagerIsRunning(awsObject);
			if(mangerInstanceID == null)
			{
				mangerInstanceID = initiateManager(awsObject);
				System.out.println("Step 1 - Manager Initiated");
			}
			else
				System.out.println("Step 1 - Manager Already Exists");


			/*Step 2 - Upload the input file to S3*/
			String fileUrl = uplaodFileToS3(awsObject, inputFileName);

			System.out.println("Step 2 - Input file Uploaded to: "+fileUrl);


			/*Step 3 - Send a message to an SQS queue, stating the location of the file on S3*/
			awsObject.initSQS();
			sendToSQS(awsObject, n_workers, terminate);
			System.out.println("Step 3 - Message to the queue was sent");

			/*Step 4 - Check an SQS queue for a message indicating the process is done and the response is available on S3*/
			retrieveFromSQS(awsObject, localApplicationId);
			System.out.println("Step 4 - Output already in S3");

			/*Step 5 - Download the summary file from S3*/
			List<String> analyzedTweets = downloadOutput(awsObject);
			System.out.println("Step 5 - Summary file downlaoded");

			/*Step 6 - Create an HTML file representing the results*/
			TextToHtml.listToHtml(analyzedTweets, outputFileName);

			/*Step 7 - Sends a termination message to the manager if it was supplied as one of its input arguments*/
			if (terminate) {
				terminateManager(awsObject, mangerInstanceID);
				System.out.println("Step 7 - Manager Terminated");
			}

		} catch (AmazonServiceException ase) {
			System.out.println("Caught Exception: " + ase.getMessage());
			System.out.println("Reponse Status Code: " + ase.getStatusCode());
			System.out.println("Error Code: " + ase.getErrorCode());
			System.out.println("Request ID: " + ase.getRequestId());
		}


		System.out.println("Step8 - END!!!!");
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("Total time: "+totalTime/60000);

	}


	//STEP 1
	/**
	 * @return id of manager instance created
	 */
	private static String initiateManager(AWSObject awsObject) {
		ArrayList<String> managerInstanceId = awsObject.EC2initiateInstance("ami-b66ed3de", 1, 1, InstanceType.T2Small.toString(),
				StaticVars.MANAGER_SCRIPT, StaticVars.INSTANCE_KEY_NAME, MANAGER_TAG);

		return 	managerInstanceId.get(0);

	}

	/**
	 * @return instanceID if manager found, else null
	 */
	private static String checkIfManagerIsRunning(AWSObject awsObject) {
		return awsObject.EC2SearchInstanceByTagForID(MANAGER_TAG, "running");
	}

	//STEP 2
	/**
	 * 
	 * @param inputFileName location of the file to upload
	 * @return url of the uploaded file in S3
	 */
	private static String uplaodFileToS3(AWSObject awsobject, String inputFileName) {
		//Create the input file
		File file = new File(inputFileName);
		return awsobject.S3UploadFile(StaticVars.INPUT_BUCKET_NAME, localApplicationId, file);
	}

	//STEP 3
	/**
	 * Initialize Queue (if needed) and sent message with the input file's URL
	 */
	private static void sendToSQS(AWSObject awsObject, int n_workers, boolean terminate) {
		String queueURL = awsObject.SQSinitializeQueues(StaticVars.URL_INPUT_QUEUE_NAME, "0");

		//Send URL of input file to the Queue
		String message = localApplicationId+StaticVars.SQS_MESSAGE_DELIMETER+terminate+StaticVars.SQS_MESSAGE_DELIMETER+n_workers;
		awsObject.SQSSendMessage(queueURL, message);
	}

	//STEP 4
	/**
	 * Check SQS until there's a message with the local application id 
	 * (meaning the manager finished processing the file)
	 */
	private static void retrieveFromSQS(AWSObject awsObject, String stringKey) {
		String queueUrl = awsObject.SQSinitializeQueues(StaticVars.URL_OUTPUT_QUEUE_NAME, "0");
		List<Message> messages;

		boolean localIdfound=false;
		System.out.println("--waiting to retrieve from "+StaticVars.URL_OUTPUT_QUEUE_NAME);
		while(!localIdfound)
		{

			//Receive List of all messages in queue
			messages = awsObject.SQSReceiveMessages(queueUrl);

			for (Message message : messages)
			{
				if(message.getBody().equals(stringKey))
				{
					localIdfound = true;
					String messageRecieptHandle = message.getReceiptHandle();
					awsObject.SQSDeleteMessage(StaticVars.URL_OUTPUT_QUEUE_NAME, messageRecieptHandle);
					break;
				}
			}


			try 
			{
				Thread.sleep(2000);
			} catch (InterruptedException e) 
			{
				e.printStackTrace();
			}
		}

	}

	//STEP 5
	/**
	 * Download summary file from S3
	 * @return list containing all the processed tweets
	 */
	private static List<String> downloadOutput(AWSObject awsObject) {

		//Download summary file from S3 Bucket
		S3Object object = awsObject.S3DownloadFile(StaticVars.OUTPUT_BUCKET_NAME, localApplicationId);

		//Add every line (analyzed tweet) from the downloaded summary file to a List
		InputStream input = object.getObjectContent();
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		List<String> analyzedTweets = new ArrayList<String>();
		while (true) {
			String line;
			try {
				line = reader.readLine();
				if (line == null) break;

				analyzedTweets.add(line);
				//System.out.println("    " + line);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		awsObject.S3DeleteObject(StaticVars.OUTPUT_BUCKET_NAME, localApplicationId);
		try{
//			awsObject.S3DeleteBucket(staticVars.OUTPUT_BUCKET_NAME);
		}
		catch (AmazonServiceException ase) {
			if(ase.getStatusCode()!=409)
			//Status Code 409 = BucketNotEmpty
			{
				System.out.println("Caught Exception: " + ase.getMessage());
				System.out.println("Reponse Status Code: " + ase.getStatusCode());
				System.out.println("Error Code: " + ase.getErrorCode());
				System.out.println("Request ID: " + ase.getRequestId());
			}
		}
		
		return analyzedTweets;
	}

	//STEP 7
	/**
	 * Terminate the Manager Instance on EC2
	 */
	private static void terminateManager(AWSObject awsObject, String mangerInstanceID) {
		retrieveFromSQS(awsObject, StaticVars.TERMINATED_STRING);
		awsObject.EC2TerminateInstance(mangerInstanceID);
	}

}
