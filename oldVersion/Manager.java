import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
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
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

//TODO send terminate message to worker and finish them
public class Manager {

	private static final String WORKER_SCRIPT = "IyEvYmluL2Jhc2ggDQoNCndnZXQgaHR0cDovL3JlcG8xLm1hdmVuLm9yZy9tYXZlbjIvY29tL2dvb2dsZWNvZGUvZWZmaWNpZW50LWphdmEtbWF0cml4LWxpYnJhcnkvZWptbC8wLjIzL2VqbWwtMC4yMy5qYXIgLU8gLy9ob21lL2VjMi11c2VyL2VqbWwtMC4yMy5qYXINCg0Kd2dldCBodHRwOi8vcmVwbzEubWF2ZW4ub3JnL21hdmVuMi9lZHUvc3RhbmZvcmQvbmxwL3N0YW5mb3JkLWNvcmVubHAvMy4zLjAvc3RhbmZvcmQtY29yZW5scC0zLjMuMC5qYXIgLU8gLy9ob21lL2VjMi11c2VyL3N0YW5mb3JkLWNvcmVubHAtMy4zLjAuamFyDQoNCndnZXQgaHR0cDovL3JlcG8xLm1hdmVuLm9yZy9tYXZlbjIvZWR1L3N0YW5mb3JkL25scC9zdGFuZm9yZC1jb3JlbmxwLzMuMy4wL3N0YW5mb3JkLWNvcmVubHAtMy4zLjAtbW9kZWxzLmphciAtTyAvL2hvbWUvZWMyLXVzZXIvc3RhbmZvcmQtY29yZW5scC0zLjMuMC1tb2RlbHMuamFyDQoNCndnZXQgaHR0cDovL2dhcnIuZGwuc291cmNlZm9yZ2UubmV0L3Byb2plY3Qvam9sbHlkYXkvcmVsZWFzZXMvMC40Ljcvam9sbHlkYXktMC40LjcuamFyIC1PIC8vaG9tZS9lYzItdXNlci9qb2xseWRheS0wLjQuNy5qYXINCg0KDQp3Z2V0IGh0dHBzOi8vczMuYW1hem9uYXdzLmNvbS9hcHBsaWNhdGlvbmNvZGUtZHMtMTUxLWVsaWRvci93b3JrZXJhcHAuamFyIC1PIC8vaG9tZS9lYzItdXNlci93b3JrZXJhcHAuamFyDQoNCg0KY2htb2QgLVIgNzc3IC8vaG9tZS9lYzItdXNlcg0KDQoNCmphdmEgLWNwIC46d29ya2VyYXBwLmphcjpzdGFuZm9yZC1jb3JlbmxwLTMuMy4wLmphcjpzdGFuZm9yZC1jb3JlbmxwLTMuMy4wLW1vZGVscy5qYXI6ZWptbC0wLjIzLmphcjpqb2xseWRheS0wLjQuNy5qYXIgLWphciAvL2hvbWUvZWMyLXVzZXIvd29ya2VyYXBwLmphcg0K";
	private static final String SQS_MESSAGE_DELIMETER = "##karish##";
	private static final String TERMINATED_STRING = "TERMINATED";
	private static BasicAWSCredentials credentials;
	private static AmazonSQS sqs;
	private static AmazonS3 s3;
	private static AmazonEC2 ec2;
	private static boolean terminate;
	private static final String INSTANCE_KEY_NAME = "first_instance_ds";
	private static final String URL_OUTPUT_QUEUE_NAME = "urlOutputQueue";
	private static final String URL_INPUT_QUEUE_NAME = "urlInputQueue";
	private static final String RAW_TWEETS_QUEUE_NAME = "rawTweetsQueue";
	private static final String PROCESSED_TWEETS_QUEUE_NAME = "processedTweetsQueue";
	private static final String INPUT_BUCKET_NAME = "input-bucket-ds-151-elidor";
	private static final String OUTPUT_BUCKET_NAME = "output-bucket-ds-151-elidor";
	private static HashMap<String, String> awsQueues;
	private static HashMap<String, LocalApplicationsDataStructure> localApplicationCollection;
	private static int currentAmountOfWorkers;
	private final static Logger LOGGER_MANAGER = Logger.getLogger(Manager.class.getName());
	private static final Tag WORKER_TAG = new Tag("name","worker");
	private static Collection<String> workersInstancesIds = new ArrayList<String>();



	public static void main(String[] args) {

		credentials = new BasicAWSCredentials("AKXXXXXXXXXXXXXX",
				"YYYYYYYYYYYYYYYYYYYYYYYY");
		ec2 = new AmazonEC2Client(credentials);
		sqs = new AmazonSQSClient(credentials);
		s3 = new AmazonS3Client(credentials);

		terminate = false;
		localApplicationCollection = new HashMap<String, LocalApplicationsDataStructure>();


		try {

			initLogger();

			initializeAllQueues();

			while (true) {
				LOGGER_MANAGER.info("Starting loop again");

				// Step 1 - Check & download new input files in S3
				if (!terminate) {
					downloadNewInputFilesFromS3();
				}

				//Check if there are enough workers <TODO>

				// Step 2 - Check for new processed tweets and add to Data Structure
				addProcessedTweet();

				if(localApplicationCollection.isEmpty()){
					LOGGER_MANAGER.info("localApplicationCollection.isEmpty()==true");
					if (terminate)
					{
						deleteWorkers();
						//Delete unused queues
						sqs.deleteQueue(new DeleteQueueRequest(awsQueues.get(RAW_TWEETS_QUEUE_NAME)));
						sqs.deleteQueue(new DeleteQueueRequest(awsQueues.get(PROCESSED_TWEETS_QUEUE_NAME)));
						LOGGER_MANAGER.info("terminate && localApplicationCollection.isEmpty()--Exiting the manager");

						break;
					}
				}
				LOGGER_MANAGER.info("Going To Sleep -main loop");

				//To prevent intensive busy wait
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					LOGGER_MANAGER.warning("-----------------------------------------------EXCEPTION-----------------------------------------------------------");
					LOGGER_MANAGER.warning(e.getMessage());
				}

			}
			LOGGER_MANAGER.info("Sending Terminated string to SQS");
			sendToSQS(awsQueues.get(URL_OUTPUT_QUEUE_NAME), TERMINATED_STRING);



			LOGGER_MANAGER.info("MANAGER FINISHED");

		} catch (NullPointerException e) {
			LOGGER_MANAGER.warning("-----------------------------------------------EXCEPTION-----------------------------------------------------------");
			LOGGER_MANAGER.warning(e.getMessage());
		}catch (AmazonServiceException ase) {
			LOGGER_MANAGER.warning("-----------------------------------------------EXCEPTION-----------------------------------------------------------");
			LOGGER_MANAGER.warning("Caught Exception: " + ase.getMessage());
			LOGGER_MANAGER.warning("Reponse Status Code: " + ase.getStatusCode());
			LOGGER_MANAGER.warning("Error Code: " + ase.getErrorCode());
			LOGGER_MANAGER.warning("Request ID: " + ase.getRequestId());
		} catch (IOException e1) {
			LOGGER_MANAGER.warning("-----------------------------------------------EXCEPTION-----------------------------------------------------------");
			LOGGER_MANAGER.warning(e1.getMessage());
		}
	}

	private static void deleteWorkers() {
		if(workersInstancesIds.size()>0){

			TerminateInstancesRequest tir = new TerminateInstancesRequest().withInstanceIds(workersInstancesIds);
			ec2.terminateInstances(tir);
			workersInstancesIds.clear();
			currentAmountOfWorkers=0;

		}
	}

	private static void initLogger() throws IOException {
		LOGGER_MANAGER.setLevel(Level.ALL);
		FileHandler fileHandler = new FileHandler("logger_manager.txt");
		fileHandler.setFormatter(new SimpleFormatter());
		LOGGER_MANAGER.addHandler(fileHandler);
		LOGGER_MANAGER.config("Logger Created");
	}

	private static void initializeAllQueues() {
		awsQueues = new HashMap<String, String>();
		initializeQueue(URL_OUTPUT_QUEUE_NAME);
		initializeQueue(URL_INPUT_QUEUE_NAME);
		initializeRawTweetQueue(RAW_TWEETS_QUEUE_NAME);
		initializeQueue(PROCESSED_TWEETS_QUEUE_NAME);
		LOGGER_MANAGER.config("Queues Initialized");
	}

	private static void initializeRawTweetQueue(String queueName) {
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
				attributes.put("VisibilityTimeout", "30");

				createQueueRequest.setAttributes(attributes);
				myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();
			}
			else {

				System.out.println("Caught Exception: " + ase.getMessage());
				System.out.println("Reponse Status Code: " + ase.getStatusCode());
				System.out.println("Error Code: " + ase.getErrorCode());
				System.out.println("Request ID: " + ase.getRequestId());
			}

		}

		LOGGER_MANAGER.info("QUEUE CREATED: " + myQueueUrl);

		awsQueues.put(queueName, myQueueUrl);
	}

	private static void initializeQueue(String queueName) {
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
			}
			else {

				System.out.println("Caught Exception: " + ase.getMessage());
				System.out.println("Reponse Status Code: " + ase.getStatusCode());
				System.out.println("Error Code: " + ase.getErrorCode());
				System.out.println("Request ID: " + ase.getRequestId());
			}

		}

		LOGGER_MANAGER.info("QUEUE CREATED: " + myQueueUrl);

		awsQueues.put(queueName, myQueueUrl);
	}

	private static void addProcessedTweet() throws IOException, AmazonServiceException, NullPointerException {
		LOGGER_MANAGER.info("retrieving new messages from SQS");
		// Step 2 - Check for new processed tweets and add to Data Structure
		List<Message> processedTweets = retrieveFromSQS(awsQueues.get(PROCESSED_TWEETS_QUEUE_NAME));

		// - Add tweets to Data Structure and delete them from queue
		String applicationId;
		String processedAnswer;
		boolean localAppIsFull = false;

		LOGGER_MANAGER.info("starting for (Message message : processedTweets)");
		for (Message message : processedTweets) {
			//Parse every message returned by the worker
			String[] parsedMessage = parseMessage(message);
			applicationId = parsedMessage[0];
			processedAnswer = parsedMessage[1]; //tweet#tiburon#sentimentNumber#tiburon#entititesList
			LOGGER_MANAGER.info("Returned tweet from worker, applicationID: "+applicationId+", message: "+processedAnswer);



			//Add message to our dataStructure
			LocalApplicationsDataStructure localApp = localApplicationCollection.get(applicationId);
			if (localApp!=null){

				localAppIsFull = localApp.addProcessedTweets(processedAnswer);
				localApplicationCollection.put(applicationId, localApp);
				LOGGER_MANAGER.info("message added to data structure --line 216");

			}
			else
			{
				LOGGER_MANAGER.info("--------localApp is null---------");
			}
			//If one localApplication is done, upload file to S3 and update the relevant outputQueue
			if (localAppIsFull) {
				LOGGER_MANAGER.info("localAppIsFull==true");
				File outputFile = createOutputFile(applicationId);
				uploadToS3(outputFile, applicationId);
				sendToSQS(awsQueues.get(URL_OUTPUT_QUEUE_NAME), applicationId);
				LOGGER_MANAGER.info("Application: "+applicationId+" is full, file uploaded to S3");
				localAppIsFull=false;
			}

			//Delete message from SQS
			String messageRecieptHandle = message.getReceiptHandle();
			sqs.deleteMessage(new DeleteMessageRequest(awsQueues.get(PROCESSED_TWEETS_QUEUE_NAME),messageRecieptHandle));
			LOGGER_MANAGER.info("message deleted from SQS");
		}

		LOGGER_MANAGER.info("exiting addProcessedTweet() --for ended");

	}

	private static File createOutputFile(String applicationId) throws IOException {
		File file = File.createTempFile(applicationId, ".txt");
		file.deleteOnExit();
		Writer writer = new OutputStreamWriter(new FileOutputStream(file));

		//Get all the processed tweets for the current localApplication
		LocalApplicationsDataStructure currentLocalApp = localApplicationCollection.get(applicationId);
		List<String> proccessedTweets = currentLocalApp.getProcessedTweets(); //tweet#tiburon#sentimentNumber#tiburon#entititesList

		//Add every tweet to our new file
		for (String string : proccessedTweets) {
			writer.write(string+"\n");
		}
		writer.close();

		//Remove localApplication from our current Collection
		localApplicationCollection.remove(applicationId);

		return file;
	}

	private static void uploadToS3(File outputFile, String applicationId) {
		s3.createBucket(OUTPUT_BUCKET_NAME);

		//Create and upload the output file to the bucket
		s3.putObject(new PutObjectRequest(OUTPUT_BUCKET_NAME, applicationId, outputFile));

	}

	private static void downloadNewInputFilesFromS3() {
		// - Check if there is a new input file to download from S3
		String queueURL = awsQueues.get(URL_INPUT_QUEUE_NAME);
		List<Message> messages = retrieveFromSQS(queueURL);

		if(!messages.isEmpty()){
			Message message = messages.get(0);
			String[] parsedMessages = parseMessage(message);


			String localApplicationId = parsedMessages[0];
			LOGGER_MANAGER.info("Received message from: " +localApplicationId);

			// - Download the input file
			List<String> tweets = downloadInput(localApplicationId);
			LOGGER_MANAGER.info("Input File downloaded, Creating object with: " + tweets.size() + " tweets");

			//get amount of tweets per worker
			int tweetsPerWorker = Integer.parseInt(parsedMessages[2]);
			LOGGER_MANAGER.info("Tweets per Worker = "+ tweetsPerWorker);

			//start worker instances
			initiateWorkers(tweetsPerWorker, tweets.size());

			//Create object with the relevant information from the local application that uploaded the file
			localApplicationCollection.put(localApplicationId, new LocalApplicationsDataStructure(localApplicationId, tweets.size()));

			LOGGER_MANAGER.info("localApplicationCollection object was created \n sending to SQS");

			// - Iterate over each tweet and send a message to SQS "rawTweetsQueue"
			SendToRawTweetsQueue(tweets, localApplicationId);

			// - Check if there's need to terminate
			if(parsedMessages[1].equalsIgnoreCase("true")){
				terminate = true;
				LOGGER_MANAGER.info("terminate = TRUE");
			}



			//Delete message from the Queue
			String messageRecieptHandle = message.getReceiptHandle();
			sqs.deleteMessage(new DeleteMessageRequest(queueURL,messageRecieptHandle));
			s3.deleteObject(new DeleteObjectRequest(INPUT_BUCKET_NAME, localApplicationId));
		}
	}

	private static void initiateWorkers(int tweetsPerWorker, int amountOfTweets) {
		//The manager should create a worker for every n messages, if there are no running workers.
		int neccessaryWorkers =(int) ((double)amountOfTweets/tweetsPerWorker+0.999);
		int newWorkers = neccessaryWorkers-currentAmountOfWorkers;

		while(newWorkers>0){
			//We dont want to create more instances in EC2 than the necessary (20 is the maximum)
			int workersToCreate = Math.min(20, newWorkers);


			RunInstancesRequest request = new RunInstancesRequest("ami-b66ed3de", workersToCreate, workersToCreate);
			request.setInstanceType(InstanceType.T2Small.toString());

			request.setKeyName(INSTANCE_KEY_NAME);
			//Send Worker Script
			request.setUserData(WORKER_SCRIPT);
			//request.setKeyName(INSTANCE_KEY_NAME);
			List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();

			//Add worker tag to recently created instances
			List<Tag> tags = new ArrayList<Tag>();
			tags.add(WORKER_TAG);
			CreateTagsRequest ctr = new CreateTagsRequest();
			ctr.setTags(tags);

			for (Instance instance : instances) {
				try {
					//Create a tag request for every instance (with their id)
					String instanceID = instance.getInstanceId();
					ctr.withResources(instanceID);
					ec2.createTags(ctr);
					workersInstancesIds.add(instanceID);
				} catch (AmazonServiceException ase) {
					LOGGER_MANAGER.warning("Caught Exception: " + ase.getMessage());
					LOGGER_MANAGER.warning("Reponse Status Code: " + ase.getStatusCode());
					LOGGER_MANAGER.warning("Error Code: " + ase.getErrorCode());
					LOGGER_MANAGER.warning("Request ID: " + ase.getRequestId());
				} 
			}

			currentAmountOfWorkers += workersToCreate;
			newWorkers -= workersToCreate;
		}
	}

	private static void SendToRawTweetsQueue(List<String> tweetsList, String localApplicationId) {
		for (String tweet : tweetsList) {
			sendToSQS(awsQueues.get(RAW_TWEETS_QUEUE_NAME),localApplicationId+SQS_MESSAGE_DELIMETER+tweet);
		}
	}

	private static void sendToSQS(String queueURL, String message) {

		//Send URL of input file to the Queue
		sqs.sendMessage(new SendMessageRequest(queueURL, message));

	}

	private static List<String> downloadInput(String localApplicationId) {

		//Download summary file from S3 Bucket
		S3Object object = s3.getObject(new GetObjectRequest(INPUT_BUCKET_NAME, localApplicationId));

		//Add every line (analyzed tweet) from the downloaded summary file to a List
		InputStream input = object.getObjectContent();
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		List<String> tweets = new ArrayList<String>();
		while (true) {
			String line;
			try {
				line = reader.readLine();
				if (line == null) break;

				tweets.add(line);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}


		return tweets;
	}

	private static String[] parseMessage(Message sqsMessage){
		//--URL_OUTPUT_QUEUE_NAME/URL_INTPUT_QUEUE_NAME
		//String[0] - LocalApplicationID / ""
		//String[1] - processed tweet / terminate
		//String[2] - empty / tweetsPerWorkers

		//--PROCESSED_TWEETS_QUEUE_NAME
		//String[0] - LocalApplicationID
		//String[1] - tweet#tiburon#sentimentNumber#tiburon#entititesList

		return sqsMessage.getBody().split(SQS_MESSAGE_DELIMETER);
	}

	private static List<Message> retrieveFromSQS(String queueUrl) { 
		// Create request to retrieve a list of messages in the SQS queue
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);

		// Receive List of all messages in queue 
		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

		return messages;
	}

}
