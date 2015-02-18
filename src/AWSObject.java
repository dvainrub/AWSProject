
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.AbstractMap.SimpleEntry;






import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceState;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.util.Base64;

/**
 * 
 * Object containing all the AWS functions we are going to use.
 *
 */
public class AWSObject {

	private AWSCredentials credentials; 
	private AmazonEC2 ec2;
	private AmazonS3 s3;
	private AmazonSQS sqs;


	//************Constructors and Getters****************
	/**
	 * Get your credentials from the "credentials" file inside you .aws folder
	 */
	public AWSObject(){
		credentials = new ProfileCredentialsProvider().getCredentials();

	}

	/**
	 * Get your credentials by hard-coded code
	 */
	public AWSObject(String accesKey, String secretKey)
	{
		credentials = new BasicAWSCredentials(accesKey, secretKey);
	}

	public AWSCredentials getCredentials(){
		return credentials;
	}

	public void AWSInitAllServices(){
		initEC2();
		initS3();
		initSQS();
	}

	//************EC2 METHODS*******************
	/**
	 * initialize EC2 service
	 */
	public void initEC2(){
		ec2 = new AmazonEC2Client(credentials);
	}

	/**
	 * 
	 * @param tag to identify the instance we are looking for
	 * @param state of the instance (ex. running, stopped)
	 * @return id of instance found by Tag and state
	 */
	public String EC2SearchInstanceByTagForID(Tag tag, String state){
		//Get a list with all the instances in EC2
		DescribeInstancesResult describeInstancesRequest = ec2.describeInstances();
		List<Reservation> reservations = describeInstancesRequest.getReservations();

		//add all instances to a Set.
		Set<Instance> instances = new HashSet<Instance>();
		for (Reservation reservation : reservations) {
			instances.addAll(reservation.getInstances());
		}


		for (Instance ins : instances){
			// instance state
			InstanceState instanceState = ins.getState();
			//instance's tags
			List<Tag> isTags = ins.getTags();

			if(isTags.contains(tag))
			{
				if(instanceState.getName().equals(state))
				{
					return ins.getInstanceId();

				}
			}

		}

		return null;
	}

	/**
	 * 
	 * @param instanceImageId (for ex. "ami-b66ed3de")
	 * @param minInstancesCount
	 * @param maxInstancesCount
	 * @param instanceType (ex. T2Small)
	 * @param userDataFile (txt file containing the script for the instance to run when started)
	 * @param instanceKeyName (name for the new instance)
	 * @param tag
	 * @return list with all the instance's id created
	 */
	public ArrayList<String> EC2initiateInstance(String instanceImageId, int minInstancesCount, int maxInstancesCount, String instanceType, String userDataFile, String instanceKeyName, Tag tag){
		ArrayList<String> idOfAllInstances = new ArrayList<String>();
		String script = null;
		try {
			script = getScript(userDataFile);
//			System.out.println("Script: " + script);
		} catch (IOException e) {
			e.printStackTrace();
		}


		//Create new instance in EC2
		RunInstancesRequest request = new RunInstancesRequest(instanceImageId, minInstancesCount, maxInstancesCount)
		.withKeyName(instanceKeyName).withUserData(script);
		request.setInstanceType(instanceType);

		List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();

		String instanceID;

		//Add tag to recently created instances
		List<Tag> tags = new ArrayList<Tag>();
		tags.add(tag);
		CreateTagsRequest ctr = new CreateTagsRequest();
		ctr.setTags(tags);

		for (Instance instance : instances) {
			try {
				//Create a tag request for every instance (with their id)
				instanceID = instance.getInstanceId();
				ctr.withResources(instanceID);
				ec2.createTags(ctr);
				idOfAllInstances.add(instanceID);
			} catch (AmazonServiceException ase) {
				/*LOGGER_MANAGER.warning("Caught Exception: " + ase.getMessage());
				LOGGER_MANAGER.warning("Reponse Status Code: " + ase.getStatusCode());
				LOGGER_MANAGER.warning("Error Code: " + ase.getErrorCode());
				LOGGER_MANAGER.warning("Request ID: " + ase.getRequestId());*/
			} 

		}
		return idOfAllInstances;
	}



	/**
	 * @param instanceId to terminate
	 */
	public void EC2TerminateInstance(String instanceId){
		TerminateInstancesRequest tir = new TerminateInstancesRequest().withInstanceIds(instanceId);
		ec2.terminateInstances(tir);
	}

	/** @param instanceId to terminate
	 */
	public void EC2TerminateInstance(Collection<String> instanceId){
		TerminateInstancesRequest tir = new TerminateInstancesRequest().withInstanceIds(instanceId);
		ec2.terminateInstances(tir);
	}

	public void EC2TerminateAllInstances(){
		//Get a list with all the instances in EC2
		DescribeInstancesResult describeInstancesRequest = ec2.describeInstances();
		List<Reservation> reservations = describeInstancesRequest.getReservations();

		//add all instances to a Set.
		Set<Instance> instances = new HashSet<Instance>();
		for (Reservation reservation : reservations) {
			instances.addAll(reservation.getInstances());
		}
		
		ArrayList<String> instancesId = new ArrayList<String>();

		for (Instance ins : instances){
			instancesId.add(ins.getInstanceId());
		}
		
		EC2TerminateInstance(instancesId);
	}

	//**************S3 METHODS******************
	/**
	 * initialize S3 services	
	 */
	public void initS3(){
		s3 = new AmazonS3Client(credentials);
	}

	/**
	 * @return S3 url of the uploaded file
	 */
	public String S3UploadFile(String bucketName, String key, File inputFile){
		//Open connection with the S3 client
		s3.createBucket(bucketName);

		//Upload the input file to the bucket
		s3.putObject(new PutObjectRequest(bucketName, key, inputFile));

		//Return the url of the uploaded file
		return "https://s3.amazonaws.com/"+bucketName+"/"+key;

	}

	/**
	 * @return object at the bucket and with the key specified
	 */
	public S3Object S3DownloadFile(String bucketName, String key){
		return s3.getObject(new GetObjectRequest(bucketName, key));
	}

	/**
	 * Delete object from the bucket
	 */
	public void S3DeleteObject(String bucketName, String key){
		s3.deleteObject(new DeleteObjectRequest(bucketName, key));
	}


	/**
	 * A bucket must be completely empty before it can be deleted
	 * @param bucketName to delete
	 */
	public void S3DeleteBucket(String bucketName) {
		s3.deleteBucket(bucketName);
	}

	/**
	 * Deletes a bucket and all the files inside
	 */
	public void S3ForceDeleteBucket(String bucketName){
		ObjectListing objectListing = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName));
		for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
			S3DeleteObject(bucketName, objectSummary.getKey());
		}

		S3DeleteBucket(bucketName);
	}

	//***************SQS METHODS******************
	/**
	 * initialize SQS services
	 */
	public void initSQS(){
		sqs = new AmazonSQSClient(credentials);
	}

	/**
	 * Initialize a list of queues
	 * @param queues list of queues' names and their visibility timeout to be initialized
	 * @return list of each queue's URL
	 */
	public HashMap<String, String> SQSinitializeQueues(ArrayList<Entry<String, String>> queues){
		String queueURL = null;
		HashMap<String, String> queuesURLs = new HashMap<String, String>();

		for (Entry<String, String> pair : queues) {
			String queueName = pair.getKey();

			try
			{
				queueURL = sqs.getQueueUrl(queueName).getQueueUrl();
			}
			catch(AmazonServiceException ase) {
				if (ase.getStatusCode() == 400) //The specified queue does not exist
				{
					CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
					Map<String, String> attributes = new HashMap<String, String>();
					attributes.put("VisibilityTimeout", pair.getValue());

					createQueueRequest.setAttributes(attributes);
					queueURL = sqs.createQueue(createQueueRequest).getQueueUrl();
					System.out.println("QUEUE CREATED: " + queueURL);
				}
				else {
					System.out.println("Caught Exception: " + ase.getMessage());
					System.out.println("Reponse Status Code: " + ase.getStatusCode());
					System.out.println("Error Code: " + ase.getErrorCode());
					System.out.println("Request ID: " + ase.getRequestId());
				}

			}

			queuesURLs.put(queueName, queueURL);
		}

		return queuesURLs;

	}

	/**
	 * Initialize only one queue
	 * @param queueName to initialize
	 * @return URL of the queue
	 */
	public String SQSinitializeQueues(String queueName, String visibilityTimeout){
		ArrayList<Entry<String, String>> queue = new ArrayList<Entry<String,String>>();
		queue.add(new SimpleEntry<String, String>(queueName, visibilityTimeout));

		return SQSinitializeQueues(queue).get(queueName);
	}

	/**
	 * 
	 * @param queueURL URL of the queue to which we want to send the message
	 * @param message
	 */
	public void SQSSendMessage(String queueURL, String message){
		sqs.sendMessage(new SendMessageRequest(queueURL, message));
	}

	/**
	 * 
	 * @param queueURL URL of the queue we want to pull messages from
	 * @return list of all the messages in the queue
	 */
	public List<Message> SQSReceiveMessages(String queueURL){
		// Create request to retrieve a list of messages in the SQS queue
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueURL);

		return sqs.receiveMessage(receiveMessageRequest).getMessages();
	}

	/**
	 * 
	 * @param receiveMessageRequest get messages with a personalized request
	 * @return list of all messages received
	 */
	public List<Message> SQSReceiveMessages(ReceiveMessageRequest receiveMessageRequest){
		return sqs.receiveMessage(receiveMessageRequest).getMessages();
	}

	/**
	 * @param queueName from which to delete
	 * @param messageRecieptHandle of the message to delete
	 */
	public void SQSDeleteMessage(String queueName, String messageRecieptHandle) {
		sqs.deleteMessage(new DeleteMessageRequest(queueName, messageRecieptHandle));
	}

	/** @param queueName to delete
	 */
	public void SQSDeleteQueue(String queueName) {
		sqs.deleteQueue(new DeleteQueueRequest(queueName));
	}

	/**
	 * Delete all the Queues in SQS
	 */
	public void SQSDeleteAllQueues(){
		for (String queueUrl : sqs.listQueues().getQueueUrls()) 
		{
			SQSDeleteQueue(queueUrl);
		}
	}

	//**************HELPER FUNCTIONS************
	/**
	 * 
	 * @param userDataFile file containing the script
	 * @return script encoded in base64
	 * @throws IOException
	 */
	private String getScript(String userDataFile) throws IOException {
		//Download script from S3
		S3Object object = S3DownloadFile(StaticVars.APPLICATION_CODE_BUCKET_NAME, userDataFile);
		InputStream input = object.getObjectContent();
		
		String script = null;
		BufferedReader br = new BufferedReader(new InputStreamReader(input));
		try {
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();

			while (line != null) {
				sb.append(line);
				sb.append(System.lineSeparator());
				line = br.readLine();
			}
			script = sb.toString();
		} finally {
			br.close();
		}

		return new String(Base64.encode(script.getBytes()));
	}


}

