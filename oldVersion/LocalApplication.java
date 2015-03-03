import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceState;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
//TODO access keys

public class LocalApplication {
	private static final String MANAGER_SCRIPT = "IyEvYmluL2Jhc2ggDQoNCndnZXQgaHR0cHM6Ly9zMy5hbWF6b25hd3MuY29tL2FwcGxpY2F0aW9uY29kZS1kcy0xNTEtZWxpZG9yL21hbmFnZXJhcHAuamFyIC1PIC8vaG9tZS9lYzItdXNlci9tYW5hZ2VyYXBwLmphcg0KDQpjaG1vZCA3NzcgLy9ob21lL2VjMi11c2VyL21hbmFnZXJhcHAuamFyDQoNCmphdmEgLWphciAvL2hvbWUvZWMyLXVzZXIvbWFuYWdlcmFwcC5qYXI=";
	private static final String TERMINATED_STRING = "TERMINATED";
	private static final String SQS_MESSAGE_DELIMETER = "##karish##";
	private static final String INSTANCE_KEY_NAME = "first_instance_ds";
	private static final String URL_INPUT_QUEUE_NAME = "urlInputQueue";
	private static final String URL_OUTPUT_QUEUE_NAME = "urlOutputQueue";
	private static final String INPUT_BUCKET_NAME = "input-bucket-ds-151-elidor";
	private static final String OUTPUT_BUCKET_NAME = "output-bucket-ds-151-elidor";
	final static Tag MANAGER_TAG = new Tag("name","manager");
	private static BasicAWSCredentials credentials;
	//private static AWSCredentials credentials; 
	private static AmazonSQS sqs;
	private static AmazonS3 s3;
	private static String instanceID;
	private static String localApplicationId;


	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		AmazonEC2 ec2;
		/*credentials = new ProfileCredentialsProvider().getCredentials();*/
		credentials = new BasicAWSCredentials("AKXXXXXXXXXXXXXX",
				"YYYYYYYYYYYYYYYYYYYYYYYY");
		//Eliyahu
		/*BasicAWSCredentials credentials = new BasicAWSCredentials("AKIAJ5GPL6EZF33TV3SA", 
				"ou9I2VXTP6BV87Oio6QEji3eEU/8vi3laq3bNZqx");*/
		ec2 = new AmazonEC2Client(credentials);

		localApplicationId = UUID.randomUUID().toString();

		String inputFileName = args[0];
		String outputFileName = args[1];
		int n_workers = Integer.parseInt(args[2]);
		boolean terminate = false;
		if (args.length > 3 && args[3].equals("terminate")) {
			terminate=true;
		}

		try {

			/*Step 1 - Check if Manager node is active on the EC2 Cloud. If it's not, the application will start
			 * the manager node*/
			if(checkIfManagerIsRunning(ec2)==false)
			{
				initiateManager(ec2);
				System.out.println("Step 1 - Manager Initiated");
			}
			else
				System.out.println("Step 1 - Manager Already Exists");

			/*Step 2 - Upload the input file to S3*/


			//String inputFileName = "C:\\Users\\dvainrub\\Code\\University\\DistributedSystems2014\\Assignment1\\input\\tweets-10-Text.txt";
			String fileUrl = uplaodFileToS3(inputFileName);

			System.out.println("Step 2 - Input file Uploaded to: "+fileUrl);


			//			Step 3 - Send a message to an SQS queue, stating the location of the file on S3
			sendToSQS(n_workers, terminate);
			System.out.println("Step 3 - Message to the queue was sent");

			//			Step 4 - Check an SQS queue for a message indicating the process is done and the response is available on S3
			retrieveFromSQS(localApplicationId);
			System.out.println("Step 4 - Output already in S3");

			//			Step 5 - Download the summary file from S3
			List<String> analyzedTweets = downloadOutput();
			System.out.println("Step 5 - Summary file downlaoded");

			//			Step 6 - Create an HTML file representing the results

//			File file = textToHtml.listToHtml(analyzedTweets, outputFileName);
//			System.out.println(file);
			textToHtml.listToHtml(analyzedTweets, outputFileName);
			
			//			Step 7 - Sends a termination message to the manager if it was supplied as one of its input arguments
			if (terminate) {
				terminateManager(ec2);
				System.out.println("Step 7 - Manager Terminated");
			}




			/*
			//DescribeInstanceStatusRequest describeInstanceRequest = new DescribeInstanceStatusRequest().withInstanceIds("i-2adb80cb");
			DescribeInstanceStatusResult describeInstanceResult = ec2.describeInstanceStatus(describeInstanceRequest);
			List<InstanceStatus> state = describeInstanceResult.getInstanceStatuses();
			while (state.size() < 1) { 
				// Do nothing, just wait, have thread sleep if needed
				describeInstanceResult = ec2.describeInstanceStatus(describeInstanceRequest);
				state = describeInstanceResult.getInstanceStatuses();
			}
			String status = state.get(0).getInstanceState().getName();
			System.out.println(status);*/

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

	private static void terminateManager(AmazonEC2 ec2) {
		retrieveFromSQS(TERMINATED_STRING);

		//Create and apply request to terminate the manager instance
		TerminateInstancesRequest tir = new TerminateInstancesRequest().withInstanceIds(instanceID);
		ec2.terminateInstances(tir);



	}

	private static List<String> downloadOutput() {

		//Download summary file from S3 Bucket
		S3Object object = s3.getObject(new GetObjectRequest(OUTPUT_BUCKET_NAME, localApplicationId));

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
				System.out.println("    " + line);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		s3.deleteObject(new DeleteObjectRequest(OUTPUT_BUCKET_NAME, localApplicationId));
		return analyzedTweets;
	}

	private static void retrieveFromSQS(String stringKey) {
		//Create Queue (doesn't overwrite or creates a new queue if queue already exists)
		String myQueueUrl = initializeQueue(URL_OUTPUT_QUEUE_NAME);

		//Create request to retrieve a list of messages in the SQS queue
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
		List<Message> messages;

		boolean localIdfound=false;
		System.out.println("--waiting to retrieve from "+URL_OUTPUT_QUEUE_NAME);
		while(!localIdfound){
			//Receive List of all messages in queue
			messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

			for (Message message : messages) {
				if(message.getBody().equals(stringKey)){
					localIdfound = true;
					String messageRecieptHandle = message.getReceiptHandle();
					sqs.deleteMessage(new DeleteMessageRequest(URL_OUTPUT_QUEUE_NAME, messageRecieptHandle));
					break;
				}
			}
			

			try {
				Thread.sleep(2000);
//				System.out.println("--waiting to retrieve form URL_OUTPUT_QUEUE");
//				System.in.read();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

	}

	private static void sendToSQS(int n_workers, boolean terminate) {
		//Open connection with SQS
		sqs = new AmazonSQSClient(credentials);

		//Create Queue (doesn't overwrite or creates a new queue if queue already exists)
		String myQueueUrl = initializeQueue(URL_INPUT_QUEUE_NAME);
/*		CreateQueueRequest createQueueRequest = new CreateQueueRequest(URL_INPUT_QUEUE_NAME);
		
		Map<String, String> attributes = new HashMap<String, String>();
		attributes.put("VisibilityTimeout", "0");
		
		createQueueRequest.setAttributes(attributes);
		
		String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
*/
		//Send URL of input file to the Queue
		sqs.sendMessage(new SendMessageRequest(myQueueUrl, localApplicationId+SQS_MESSAGE_DELIMETER+terminate+SQS_MESSAGE_DELIMETER+n_workers));
	}

	private static String uplaodFileToS3(String inputFileName) {
		String bucketName = INPUT_BUCKET_NAME;

		//Create unique name for the file to upload
		/*		Date date = new Date();
		Timestamp ts = new Timestamp(date.getTime());
		String rawkey = "inputFile_"+ ts;
		String key = rawkey.replace(' ', '_').replace(':', '-').replace('.','-');*/
		String key = localApplicationId;

		//Open connection with the S3 client
		s3 = new AmazonS3Client(credentials);
		s3.createBucket(bucketName);

		//Create and upload the input file to the bucket
		File file = new File(inputFileName);
		s3.putObject(new PutObjectRequest(bucketName, key, file));

		//Return the url of the uploaded file
		String fileUrl = "https://s3.amazonaws.com/"+INPUT_BUCKET_NAME+"/"+key;
		return fileUrl;
	}

	private static void initiateManager(AmazonEC2 ec2) {

		//Create new instance in EC2
		RunInstancesRequest request = new RunInstancesRequest("ami-b66ed3de", 1, 1);
		request.setInstanceType(InstanceType.T2Small.toString());
		
		//Send Manager Script
		request.setUserData(MANAGER_SCRIPT);
		request.setKeyName(INSTANCE_KEY_NAME);
		List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();

		instanceID = instances.get(0).getInstanceId();

		//Add manager tag to recently created instance
		List<Tag> tags = new ArrayList<Tag>();
		tags.add(MANAGER_TAG);

		CreateTagsRequest ctr = new CreateTagsRequest();
		ctr.setTags(tags);
		ctr.withResources(instanceID);
		ec2.createTags(ctr);

	}

	private static boolean checkIfManagerIsRunning(AmazonEC2 ec2) {


		//Get a list with all the instances in EC2
		DescribeInstancesResult describeInstancesRequest = ec2.describeInstances();
		List<Reservation> reservations = describeInstancesRequest.getReservations();

		//add all instances to a Set.
		Set<Instance> instances = new HashSet<Instance>();
		for (Reservation reservation : reservations) {
			instances.addAll(reservation.getInstances());
		}

		//System.out.println("You have " + instances.size() + " Amazon EC2 instance(s).");

		for (Instance ins : instances){
			/*
		 // instance id
		 String instancesId = ins.getInstanceId();
			 */
			// instance state
			InstanceState instanceState = ins.getState();
			//instance's tags
			List<Tag> isTags = ins.getTags();

			if(isTags.contains(MANAGER_TAG))
			{
				if(instanceState.getName().equals("running"))
				{
					instanceID = ins.getInstanceId();
					return true;
				}
			}

			//System.out.println(instancesId+" "+is.getName()+" "+isTags);
		}

		return false;
	}

	private static String initializeQueue(String queueName) {
		// Create Queue (doesn't overwrite
		// or creates a new queue if queue already exists) 
		String myQueueUrl = null;
		
		try {
			myQueueUrl = sqs.getQueueUrl(queueName).getQueueUrl();
		}
		catch (AmazonServiceException ase) {
			if (ase.getStatusCode() == 400) //The specified queue does not exist
			{
				CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
				Map<String, String> attributes = new HashMap<String, String>();
				attributes.put("VisibilityTimeout", "0");
				
				createQueueRequest.setAttributes(attributes);
				myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
				System.out.println("QUEUE CREATED: " + myQueueUrl);
			}
			else {
				System.out.println("Caught Exception: " + ase.getMessage());
				System.out.println("Reponse Status Code: " + ase.getStatusCode());
				System.out.println("Error Code: " + ase.getErrorCode());
				System.out.println("Request ID: " + ase.getRequestId());
			}
			
		}

		
		return myQueueUrl;
		
	}

}
