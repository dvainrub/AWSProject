


public class StaticVars {
	

	//Delimeters&Strings for SQS Queues
	static final String SQS_MESSAGE_DELIMETER = "##karish##";
	static final String TWEET_MESSAGE_DELIMETER = "#tiburon#";
	static final String TERMINATED_STRING = "TERMINATED"; //Tells the localApp it can terminate the Manager Instance

	//SCRIPTS
	static final String MANAGER_SCRIPT = "managerscript.txt";
	static final String WORKER_SCRIPT = "workerscript.txt";
	
	//URL of Queues
	static final String URL_INPUT_QUEUE_NAME = "urlInputQueue";
	static final String URL_OUTPUT_QUEUE_NAME = "urlOutputQueue";

	
	//Name of Queues
	static final String RAW_TWEETS_QUEUE_NAME = "rawTweetsQueue";
	static final String PROCESSED_TWEETS_QUEUE_NAME = "processedTweetsQueue";

	//Name of Buckets
	static final String INPUT_BUCKET_NAME = "input-bucket-ds-151-elidor";
	static final String OUTPUT_BUCKET_NAME = "output-bucket-ds-151-elidor";
	//Bucket with all the jars and scripts
	static final String APPLICATION_CODE_BUCKET_NAME = "applicationcode-ds-151-elidor";

	//Key name of the .pem file that contains identifiers to access the instances
	static final String INSTANCE_KEY_NAME = "first_instance_ds";

	
	
}
