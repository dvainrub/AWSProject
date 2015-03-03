import java.util.ArrayList;
import java.util.List;


public class LocalApplicationsDataStructure {
	String localApplicationId;
	int maxSize;
	List<String> processedTweets;
	
	public LocalApplicationsDataStructure() {
		
	}
	
	public LocalApplicationsDataStructure(String localApplicationId, int maxSize) {
		this.localApplicationId = localApplicationId;
		this.maxSize = maxSize;
		processedTweets = new ArrayList<String>();
	}

	public List<String> getProcessedTweets() {
		return processedTweets;
	}

	public boolean addProcessedTweets(String processedTweet) {
		processedTweets.add(processedTweet);
//		System.out.println("--added tweet");
		return isFull();
	}


	public String getLocalApplicationId() {
		return localApplicationId;
	}

	public int getSize() {
		return processedTweets.size();
	}

	public boolean isFull() {
//		System.out.println("isFull()-->size is: "+processedTweets.size()+", maxSize is: " +maxSize);
		return maxSize == processedTweets.size();
	}
	
	public int getMaxSize() {
		return maxSize;
	}

}
