
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;


public class TextToHtml {
	private final static String DELIMETER = "#tiburon#"; 
	
	//sentiments we use to change the color of the line
	static String[] sentiments = {"DarkRed","Red", "Black", "LightGreen", "DarkGreen"};

	private static String tweetToHtml(String processedTweet) {

		String htmlLine = "<p> <b><font color=\"";
		
		// parsedProcessedTweet[0] = tweet message
		// parsedProcessedTweet[1] = sentiment Number
		// parsedProcessedTweet[2] = entity's list
		String[] parsedProcessedTweet = parse(processedTweet);
		int sentimentNumber = Integer.parseInt(parsedProcessedTweet[1]);
		htmlLine += sentiments[sentimentNumber];
		htmlLine += "\">";
		
		//adding the tweet message
		htmlLine += parsedProcessedTweet[0] + "</font></b>";

		//adding the entity's list
		htmlLine += parsedProcessedTweet[2] + "</p>";
		return htmlLine;
	}

	public static void listToHtml(List<String> processedTweets, String outputFilename) throws IOException {
		
		//create new file to put their the output - processed tweets in HTML format
	//	File htmlTweets = File.createTempFile(outputFilename, ".html");
//		htmlTweets.deleteOnExit();
	//	Writer writer = new OutputStreamWriter(new FileOutputStream(htmlTweets));
		PrintWriter writer = new PrintWriter(outputFilename + ".html", "UTF-8");
//		writer.write("<html>\n<body>");
		writer.println("<html>\n<body>");
		
		//adding to the file each processed Tweet in HTML format
		for (String processedTweet : processedTweets) {
//			writer.write(tweetToHtml(processedTweet));
			writer.println(tweetToHtml(processedTweet));
		}

//		writer.write("</body>\n</html>");
		writer.println("</body>\n</html>");
		writer.close();

	}
	
	
	private static String[] parse(String processedTweet) {
		return processedTweet.split(DELIMETER);
	}
}

