import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

//TODO receive terminate message
public class Worker {
	static StanfordCoreNLP sentimentPipeline, NERPipeline;
	private static final String RAW_TWEETS_QUEUE_NAME = "rawTweetsQueue";
	private static final String PROCESSED_TWEETS_QUEUE_NAME = "processedTweetsQueue";
	private static final String TWEET_MESSAGE_DELIMETER = "#tiburon#";
	private static final String SQS_MESSAGE_DELIMETER = "##karish##";
	private static String rawTweetsQueueUrl;
	private static BasicAWSCredentials credentials;
	private static AmazonSQS sqs;
	private static boolean terminate = false;

	public static void main(String[] args) {

		try {
			//Initialize all AWS like SQS
			initAWS();

			while(!terminate)
			{

				// - Get One message from S3
				List<Message> messsages = new ArrayList<Message>();
				while(messsages.isEmpty())
				{
					messsages = retrieveFromSQS();

					try {
						Thread.sleep(2000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

				}

				// - Perform Sentiment Analysis + Entity recognition on received tweet
				initStanfordProperties();
				Message message = messsages.get(0);
				String[] messageBody = parseMessage(message);
				String localApplicationID = messageBody[0];
				String tweet = messageBody[1];
				int sentiment = findSentiment(tweet);
				List<String> entitiesList = findEntitiesList(tweet);

				// - Put the message in the SQS queue: tweet#tiburon#sentimentNumber#tiburon#entititesList
				sendToSQS(localApplicationID,tweet, sentiment, entitiesList);

				//remove the processed message from the SQS queue
				String messageRecieptHandle = message.getReceiptHandle();
				sqs.deleteMessage(new DeleteMessageRequest(rawTweetsQueueUrl,messageRecieptHandle));


				/*		String[] tweets = {"Currently watching a man eat a hamburger sandwiched between two grilled cheeses #america","Hope I don't get sick from Whataburger.  The lady making my burger was also putting raw hamburger meat on the grill at the same time.","RT @AlexisMateo79: Tonight at Hamburger Marys Tampa http://t.co/5cj4eRB0RV","Booty looking like Hamburger buns for a Big Mac \"@CheeksForWeekz: Perfect Cheeks ?? http://t.co/kByf3uIHyy\"","@Simberg_Space @LifeExtension I agree with you 100%. That's why never hardlyhave anything with a bun.  I haven't had a hamburger for a year","RT @OGTREEZ: Booty looking like Hamburger buns for a Big Mac \"@CheeksForWeekz: Perfect Cheeks ?? http://t.co/kByf3uIHyy\"","Honestly, I'm a loser. *eats hamburger*","@KingBach need a big hamburger now","Kansas boom town hamburger stand only sudatorium remodelers are the undo professionals inasmuch as remodeling ideas http://t.co/NdlxGWOPCf","Deep Fried Hamburger Helper Burger Recipe - HellthyJunkFood http://t.co/o2pyv9d4O2"};

			for (String string : tweets) {
				System.out.println("Sentiment: "+findSentiment(string));
				printEntities(string);

			}*/

			}

		} catch (AmazonServiceException ase) {
			System.out.println("Caught Exception: " + ase.getMessage());
			System.out.println("Reponse Status Code: " + ase.getStatusCode());
			System.out.println("Error Code: " + ase.getErrorCode());
			System.out.println("Request ID: " + ase.getRequestId());
		}
		System.out.println("--");
	}

	private static void sendToSQS(String localApplicationID, String tweet, int sentiment, List<String> entitiesList) {
		//--PROCESSED_TWEETS_QUEUE_NAME
		//String[0] - LocalApplicationID
		//String[1] - tweet#tiburon#sentimentNumber#tiburon#entititesList

		String message = localApplicationID+SQS_MESSAGE_DELIMETER+tweet+TWEET_MESSAGE_DELIMETER+sentiment+TWEET_MESSAGE_DELIMETER+entitiesList;

		//Send URL of input file to the Queue
		sqs.sendMessage(new SendMessageRequest(PROCESSED_TWEETS_QUEUE_NAME, message));

	}

	private static void initStanfordProperties() {
		Properties props = new Properties();
		props.put("annotators", "tokenize, ssplit, parse, sentiment");
		sentimentPipeline = new StanfordCoreNLP(props);

		Properties props2 = new Properties();
		props2.put("annotators", "tokenize , ssplit, pos, lemma, ner");
		NERPipeline =  new StanfordCoreNLP(props2);
	}

	private static List<Message> retrieveFromSQS() { 
		// Create request to retrieve a list of messages in the SQS queue
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(rawTweetsQueueUrl);
		//We retrieve only one tweet at a time
		receiveMessageRequest.setMaxNumberOfMessages(1);
		//Make the current message invisible while we work on it (around 20 seconds)
		receiveMessageRequest.setVisibilityTimeout(20);

		// Receive message(tweet) in queue 
		List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

		return messages;
	}

	private static void initAWS() {
		credentials = new BasicAWSCredentials("AKXXXXXXXXXXXXXX",
				"YYYYYYYYYYYYYYYYYYYYYYYY");
		sqs = new AmazonSQSClient(credentials);

		rawTweetsQueueUrl = initializeRawTweetQueue(RAW_TWEETS_QUEUE_NAME);
	}

	public static int findSentiment(String tweet) {

		int mainSentiment = 0;
		if (tweet != null && tweet.length() > 0) {
			int longest = 0;    
			Annotation annotation = sentimentPipeline.process(tweet);
			for (CoreMap sentence : annotation
					.get(CoreAnnotations.SentencesAnnotation.class)) {
				Tree tree = sentence
						.get(SentimentCoreAnnotations.AnnotatedTree.class);
				int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
				String partText = sentence.toString();
				if (partText.length() > longest) {
					mainSentiment = sentiment;
					longest = partText.length();
				}

			}
		}
		return mainSentiment;
	}

	public static List<String> findEntitiesList(String tweet){
		List<String> entitiesList = new ArrayList<String>();


		// create an empty Annotation just with the given text
		Annotation document = new Annotation(tweet);

		// run all Annotators on this text
		NERPipeline.annotate(document);

		// these are all the sentences in this document
		// a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
		List<CoreMap> sentences = document.get(SentencesAnnotation.class);

		for(CoreMap sentence: sentences) {
			// traversing the words in the current sentence
			// a CoreLabel is a CoreMap with additional token-specific methods
			for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
				// this is the text of the token
				String word = token.get(TextAnnotation.class);
				// this is the NER label of the token
				String ne = token.get(NamedEntityTagAnnotation.class);
				if(!ne.equals("O"))
				{
					if(ne.equals("PERSON") || ne.equals("LOCATION") || ne.equals("ORGANIZATION")){
						System.out.println("\t-" + word + ":" + ne);
						entitiesList.add(word + ":" + ne);
					}
				}
			}
		}

		return entitiesList;

	}

	private static String[] parseMessage(Message sqsMessage){
		//--RAW_TWEETS_QUEUE_NAME
		//String[0] - LocalApplicationID
		//String[1] - tweet

		return sqsMessage.getBody().split(SQS_MESSAGE_DELIMETER);
	}

	private static String initializeRawTweetQueue(String queueName) {
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

		System.out.println("QUEUE CREATED: " + myQueueUrl);

		return myQueueUrl;
	}


}
