import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

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

public class Worker {
	static StanfordCoreNLP sentimentPipeline, NERPipeline;
	private static boolean terminate = false;

	public static void main(String[] args) {

		try {
			//Initialize all AWS services
			AWSObject awsObject = new AWSObject("WRITE YOUR OWN ACCES KEY", "WRITE YOUR OWN SECRET KEY");
			awsObject.initSQS();
			String rawTweetsQueueUrl = awsObject.SQSinitializeQueues(StaticVars.RAW_TWEETS_QUEUE_NAME, "30");

			while(!terminate)
			{

				// - Get One message from S3
				List<Message> messsages = new ArrayList<Message>();
				while(messsages.isEmpty())
				{
					messsages = retrieveFromSQS(awsObject, rawTweetsQueueUrl);

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
				sendToSQS(awsObject, localApplicationID,tweet, sentiment, entitiesList);

				//remove the processed message from the SQS queue
				String messageRecieptHandle = message.getReceiptHandle();
				awsObject.SQSDeleteMessage(rawTweetsQueueUrl, messageRecieptHandle);

			}

		} catch (AmazonServiceException ase) {
			System.out.println("Caught Exception: " + ase.getMessage());
			System.out.println("Reponse Status Code: " + ase.getStatusCode());
			System.out.println("Error Code: " + ase.getErrorCode());
			System.out.println("Request ID: " + ase.getRequestId());
		}
		System.out.println("--");
	}

	//*********AWS METHODS*******************
	private static void sendToSQS(AWSObject awsO, String localApplicationID, String tweet, int sentiment, List<String> entitiesList) {

		//--PROCESSED_TWEETS_QUEUE_NAME
		//String[0] - LocalApplicationID
		//String[1] - tweet#tiburon#sentimentNumber#tiburon#entititesList

		String message = localApplicationID+StaticVars.SQS_MESSAGE_DELIMETER+tweet+StaticVars.TWEET_MESSAGE_DELIMETER+sentiment+StaticVars.TWEET_MESSAGE_DELIMETER+entitiesList;

		//Send URL of input file to the Queue
		awsO.SQSSendMessage(StaticVars.PROCESSED_TWEETS_QUEUE_NAME, message);

	}

	private static List<Message> retrieveFromSQS(AWSObject awsO, String rawTweetsQueueUrl) { 
		
		// Create request to retrieve a list of messages in the SQS queue
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(rawTweetsQueueUrl);
		
		//We retrieve only one tweet at a time
		receiveMessageRequest.setMaxNumberOfMessages(1);
		//Make the current message invisible while we work on it (around 20 seconds)
		receiveMessageRequest.setVisibilityTimeout(20);

		// Receive message(tweet) in queue 
		List<Message> messages = awsO.SQSReceiveMessages(receiveMessageRequest);

		return messages;
	}

	//************HELPER METHODS***************
	private static String[] parseMessage(Message sqsMessage){
		//--RAW_TWEETS_QUEUE_NAME
		//String[0] - LocalApplicationID
		//String[1] - tweet

		return sqsMessage.getBody().split(StaticVars.SQS_MESSAGE_DELIMETER);
	}

	
	//*********STANFORD METHODS**************
	private static void initStanfordProperties() {
		Properties props = new Properties();
		props.put("annotators", "tokenize, ssplit, parse, sentiment");
		sentimentPipeline = new StanfordCoreNLP(props);

		Properties props2 = new Properties();
		props2.put("annotators", "tokenize , ssplit, pos, lemma, ner");
		NERPipeline =  new StanfordCoreNLP(props2);
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
	
}
