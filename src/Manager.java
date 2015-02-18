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
import java.util.Map.Entry;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.AbstractMap.SimpleEntry;







import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;

public class Manager {

	private static boolean terminate;
	private static HashMap<String, String> awsQueues;
	private static HashMap<String, LocalApplicationsDataStructure> localApplicationCollection;
	private static int currentAmountOfWorkers;
	private final static Logger LOGGER_MANAGER = Logger.getLogger(Manager.class.getName());
	private static final Tag WORKER_TAG = new Tag("name","worker");
	private static Collection<String> workersInstancesIds = new ArrayList<String>();



	public static void main(String[] args) {
		AWSObject awsObject = new AWSObject("WRITE YOUR OWN ACCES KEY", "WRITE YOUR OWN SECRET KEY");
		awsObject.AWSInitAllServices();

		terminate = false;
		localApplicationCollection = new HashMap<String, LocalApplicationsDataStructure>();


		try {

			initLogger();

			initializeAllQueues(awsObject);

			while (true) {
				//LOGGER_MANAGER.info("Starting loop again");

				// Step 1 - Check & download new input files in S3
				if (!terminate) {
					downloadNewInputFilesFromS3(awsObject);
				}


				// Step 2 - Check for new processed tweets and add to Data Structure
				addProcessedTweet(awsObject);

				if(localApplicationCollection.isEmpty()){
					//LOGGER_MANAGER.info("localApplicationCollection.isEmpty()==true");
					if (terminate)
					{
						deleteWorkers(awsObject);

						//Delete unused queues
						awsObject.SQSDeleteQueue(awsQueues.get(StaticVars.RAW_TWEETS_QUEUE_NAME));
						awsObject.SQSDeleteQueue(awsQueues.get(StaticVars.PROCESSED_TWEETS_QUEUE_NAME));

						LOGGER_MANAGER.info("terminate && localApplicationCollection.isEmpty()--Exiting the manager");
						break;
					}
				}
				//LOGGER_MANAGER.info("Going To Sleep -main loop");

				//To prevent intensive busy wait
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					LOGGER_MANAGER.warning("-----------------------------------------------EXCEPTION-----------------------------------------------------------");
					LOGGER_MANAGER.warning(e.getMessage());
				}

			}
			LOGGER_MANAGER.info("Sending Terminated string to SQS");
			awsObject.SQSSendMessage(awsQueues.get(StaticVars.URL_OUTPUT_QUEUE_NAME), StaticVars.TERMINATED_STRING);



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

	private static void deleteWorkers(AWSObject awsO) {
		if(workersInstancesIds.size()>0){
			awsO.EC2TerminateInstance(workersInstancesIds);
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

	private static void initializeAllQueues(AWSObject awsO) {
		ArrayList<Entry<String, String>> queues = new ArrayList<Entry<String,String>>();
		queues.add(new SimpleEntry<String, String>(StaticVars.URL_OUTPUT_QUEUE_NAME, "0"));
		queues.add(new SimpleEntry<String, String>(StaticVars.URL_INPUT_QUEUE_NAME, "0"));
		queues.add(new SimpleEntry<String, String>(StaticVars.PROCESSED_TWEETS_QUEUE_NAME, "0"));
		queues.add(new SimpleEntry<String, String>(StaticVars.RAW_TWEETS_QUEUE_NAME, "30"));
		awsQueues = awsO.SQSinitializeQueues(queues);

		LOGGER_MANAGER.config("Queues Initialized");
	}

	private static void addProcessedTweet(AWSObject awsO) throws IOException, AmazonServiceException, NullPointerException {
		//LOGGER_MANAGER.info("retrieving new messages from SQS");
		// Step 2 - Check for new processed tweets and add to Data Structure

		List<Message> processedTweets = awsO.SQSReceiveMessages(awsQueues.get(StaticVars.PROCESSED_TWEETS_QUEUE_NAME));

		// - Add tweets to Data Structure and delete them from queue
		String applicationId;
		String processedAnswer;
		boolean localAppIsFull = false;

		//LOGGER_MANAGER.info("starting for (Message message : processedTweets)");
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
				//LOGGER_MANAGER.info("message added to data structure");

			}
			else
			{
				//LOGGER_MANAGER.info("--------localApp is null---------");
			}
			//If one localApplication is done, upload file to S3 and update the relevant outputQueue
			if (localAppIsFull) {
				//LOGGER_MANAGER.info("localAppIsFull==true");
				File outputFile = createOutputFile(applicationId);
				awsO.S3UploadFile(StaticVars.OUTPUT_BUCKET_NAME, applicationId, outputFile);
				awsO.SQSSendMessage(awsQueues.get(StaticVars.URL_OUTPUT_QUEUE_NAME), applicationId);
				LOGGER_MANAGER.info("Application: "+applicationId+" is full, file uploaded to S3");
				localAppIsFull=false;
			}

			//Delete message from SQS
			String messageRecieptHandle = message.getReceiptHandle();
			awsO.SQSDeleteMessage(awsQueues.get(StaticVars.PROCESSED_TWEETS_QUEUE_NAME),messageRecieptHandle);
			LOGGER_MANAGER.info("message deleted from SQS");
		}

		//LOGGER_MANAGER.info("exiting addProcessedTweet() --for ended");

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

	private static void downloadNewInputFilesFromS3(AWSObject awsO) {
		// - Check if there is a new input file to download from S3
		String queueURL = awsQueues.get(StaticVars.URL_INPUT_QUEUE_NAME);
		List<Message> messages = awsO.SQSReceiveMessages(queueURL);

		if(!messages.isEmpty()){
			Message message = messages.get(0);
			String[] parsedMessages = parseMessage(message);


			String localApplicationId = parsedMessages[0];
			LOGGER_MANAGER.info("Received message from: " +localApplicationId);

			// - Download the input file
			List<String> tweets = downloadInput(awsO, localApplicationId);
			LOGGER_MANAGER.info("Input File downloaded, Creating object with: " + tweets.size() + " tweets");

			//get amount of tweets per worker
			int tweetsPerWorker = Integer.parseInt(parsedMessages[2]);
			LOGGER_MANAGER.info("Tweets per Worker = "+ tweetsPerWorker);

			//start worker instances
			initiateWorkers(awsO, tweetsPerWorker, tweets.size());

			//Create object with the relevant information from the local application that uploaded the file
			localApplicationCollection.put(localApplicationId, new LocalApplicationsDataStructure(localApplicationId, tweets.size()));

			LOGGER_MANAGER.info("localApplicationCollection object was created \nsending to SQS");

			// - Iterate over each tweet and send a message to SQS "rawTweetsQueue"
			SendToRawTweetsQueue(awsO, tweets, localApplicationId);

			// - Check if there's need to terminate
			if(parsedMessages[1].equalsIgnoreCase("true")){
				terminate = true;
				LOGGER_MANAGER.info("terminate = TRUE");
			}



			//Delete message from the Queue
			String messageRecieptHandle = message.getReceiptHandle();
			awsO.SQSDeleteMessage(queueURL, messageRecieptHandle);
			LOGGER_MANAGER.info("waiting for some action...");
		}
	}

	private static void initiateWorkers(AWSObject awsObject, int tweetsPerWorker, int amountOfTweets) {
		//The manager should create a worker for every n messages, if there are no running workers.
		int neccessaryWorkers =(int) ((double)amountOfTweets/tweetsPerWorker+0.999);
		int newWorkers = neccessaryWorkers-currentAmountOfWorkers;

		while(newWorkers>0){
			//We dont want to create more instances in EC2 than the necessary (20 is the maximum)
			int workersToCreate = Math.min(20, newWorkers);

			workersInstancesIds = awsObject.EC2initiateInstance("ami-b66ed3de", workersToCreate, workersToCreate,
					InstanceType.T2Small.toString(), StaticVars.WORKER_SCRIPT, StaticVars.INSTANCE_KEY_NAME, WORKER_TAG);

			currentAmountOfWorkers += workersToCreate;
			newWorkers -= workersToCreate;
		}
	}

	private static void SendToRawTweetsQueue(AWSObject awsO, List<String> tweetsList, String localApplicationId) {
		String queueURL = awsQueues.get(StaticVars.RAW_TWEETS_QUEUE_NAME);
		String message = null;

		for (String tweet : tweetsList) {
			message = localApplicationId+StaticVars.SQS_MESSAGE_DELIMETER+tweet;
			awsO.SQSSendMessage(queueURL, message);
		}
	}

	private static List<String> downloadInput(AWSObject awsO, String localApplicationId) {

		//Download summary file from S3 Bucket
		S3Object object = awsO.S3DownloadFile(StaticVars.INPUT_BUCKET_NAME, localApplicationId);



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


		awsO.S3DeleteObject(StaticVars.INPUT_BUCKET_NAME, localApplicationId);
		try{
			//awsO.S3DeleteBucket(staticVars.INPUT_BUCKET_NAME);
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

		return sqsMessage.getBody().split(StaticVars.SQS_MESSAGE_DELIMETER);
	}

}
