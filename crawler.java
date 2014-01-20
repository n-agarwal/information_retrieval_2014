/**
 * Course: Information Retrieval 
 * College: CCIS, Northeatern University
 * Author: Nishant Agarwal
 * Date: 01/18/2014
 */

package hw1;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.trigonic.jrobotx.RobotExclusion;

public class Crawler {

	// Object Attributes
	// defines the base URL from where the crawler starts crawling
	private URL baseURL;
	// defines the maximum number of pages to be visited
	private int threshold;
	// defines the agent attribute
	private String userAgent;
	// defines the valid content types for this Crawler object
	private Set<String> validContents = new HashSet<String>();
	// defines the delay (in seconds) between each visit for this object
	private int delay;
	// defines the accepted domains for the URL's
	private Set<String> domains = new HashSet<String>();
	// defines the output file to write output
	private String outputFile;

	// Class attributes
	// defines Document select type
	private final String selectType = "a[href]";
	// defines Element attribute
	private final String attribute = "href";
	// defines the separation character
	private final char separationCharacter = ' ';
	// defines timeout wait
	private final int timeout = 10 * 1000;

	/**
	 * returns an Object of Crawler type initialized with input values
	 * 
	 * @param baseURL
	 *            defines the base URL from where the crawler starts crawling
	 * @param threshold
	 *            defines the maximum number of pages to be visited
	 * @param userAgent
	 *            defines the agent attribute
	 * @param validContents
	 *            defines the valid content types for this Crawler object
	 * @param delay
	 *            defines the delay (in seconds) between each visit for this
	 *            object
	 * @param domains
	 *            defines the accepted domains for the URL's
	 * @param outputFile
	 *            defines the output file to write output
	 */

	public Crawler(URL baseURL, int threshold, String userAgent,
			Set<String> validContents, int delay, Set<String> domains,
			String outputFile) {
		super();
		this.baseURL = baseURL;
		this.threshold = threshold;
		this.userAgent = userAgent;
		this.validContents = validContents;
		this.delay = delay;
		this.domains = domains;
		this.outputFile = outputFile;
	}

	/**
	 * Starts crawling the web from [baseURL] and visits only those pages which
	 * qualify the [contents] using agent as [userAgent] maintaining a delay of
	 * [delay] second(s) between each visit. The crawler stops either when the
	 * complete tree is visited or [threshold] number of pages are visited
	 * whichever comes first
	 */
	public void crawl() {
		// maintains a canonical form set of already visited URLs
		Set<String> visitedURLCanonical = new HashSet<String>();
		// maintains a canonical form queue of URLs to be visited
		Queue<String> toVisitURLCanonical = new LinkedList<String>();
		// maintains a original URLs queue of to be visited
		Queue<URL> toVisitURLOriginal = new LinkedList<URL>();
		// maintains a set of URLs listed in the current document
		Set<URL> containsURLSet = new HashSet<URL>();
		// defines when to stop crawling
		boolean keepCrawling = true;
		// start crawling from base URL
		toVisitURLCanonical.add(getCanonicalForm(baseURL));
		toVisitURLOriginal.add(baseURL);
		// Map to store is the URL can be visited to increase performance
		Map<URL, Boolean> visitMap = new HashMap<URL, Boolean>();
		//
		Document document = null;
		//
		int delaySeconds = delay * 1000;
		//
		int counter = 0;
		//
		File file = new File(outputFile);
		FileWriter fileWriter = null;
		try {
			fileWriter = new FileWriter(file, false);
			fileWriter.write("");
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		//
		if (!canBeVisited(baseURL)) {
			keepCrawling = false;
		}
		//
		try {
			while (keepCrawling) {
				// fetches the next in queue URL to be visited
				String currentURLCanonical = toVisitURLCanonical.remove();
				URL currentURLOriginal = toVisitURLOriginal.remove();
				// continue iff the current url is not already visited
				if (!visitedURLCanonical.contains(currentURLCanonical)) {
					// append to file
					// System.out.print(currentURLCanonical);
					fileWriter.append(currentURLCanonical);
					// add in visited URL list
					visitedURLCanonical.add(currentURLCanonical);
					// read the document
					document = Jsoup.connect(currentURLOriginal.toString())
							.userAgent(userAgent).timeout(timeout).get();
					Elements elements = document.select(selectType);
					// clear the list of URL read from current page
					containsURLSet.clear();
					// check the valid elements in the document
					for (Element element : elements) {
						try {
							URL containsURL = new URL(element.attr(attribute));
							String containsUrlCanonical = getCanonicalForm(containsURL);
							// check for duplicates and rules
							if (!containsUrlCanonical
									.equals(currentURLCanonical)
									&& !containsURLSet.contains(containsURL)
									&& ((visitMap.containsKey(containsURL) && visitMap
											.get(containsURL)) || canBeVisited(containsURL))) {
								//
								visitMap.put(containsURL, true);
								containsURLSet.add(containsURL);
								// System.out.print(separationCharacter
								// + containsUrlCanonical);
								fileWriter.append(separationCharacter
										+ containsUrlCanonical);
								// if not already in the to visit queue add it
								// or visited set
								if (!toVisitURLCanonical
										.contains(containsUrlCanonical)) {
									toVisitURLCanonical
											.add(containsUrlCanonical);
									toVisitURLOriginal.add(containsURL);
								}
							}

						} catch (Exception e) {
							// ignore
						}
					}

					// This part ensures that crawler visit's at most one page
					// per delay seconds to avoid server overloading
					try {
						Thread.sleep(delaySeconds);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					counter++;
					// exit if threshold is reached or
					// there are no more URL's left to be visited
					if (counter >= threshold || toVisitURLCanonical.isEmpty())
						keepCrawling = false;

					if (keepCrawling)
						fileWriter.append("\n");
					// System.out.println();
				}
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			try {
				fileWriter.close();
			} catch (IOException e) {
				System.out.println("IOException | Cannot close the file.");
			}
		}
	}

	/**
	 * returns true iff the input url current passes all the qualifying criteria
	 * i.e. 1. It of a valid domain 2. Is of an accepted form 3. Respect the
	 * robots.txt
	 * 
	 * @param url
	 *            url to be verified
	 * @return
	 */
	private boolean canBeVisited(URL url) {
		if (!isAcceptedForm(url))
			return false;
		else if (!isRobotsRespected(url))
			return false;
		else if (!isValidDomain(url))
			return false;
		return true;
	}

	/**
	 * returns true iff the URL is of accepted domain format
	 * 
	 * @param url
	 *            url to be verified
	 * @return
	 */
	private boolean isValidDomain(URL url) {
		String host = url.getHost();
		for (String domain : domains) {
			if (host.contains(domain))
				return true;
		}
		return false;
	}

	/**
	 * returns true iff the input url respected robots.txt
	 * 
	 * @param url
	 *            url to be verified
	 * @return
	 */
	private boolean isRobotsRespected(URL url) {
		RobotExclusion robot = new RobotExclusion();
		return robot.allows(url, userAgent);
	}

	/**
	 * returns true iff the input url is of one of an accepted [validContents]
	 * 
	 * @param url
	 *            url to be verified
	 * @return
	 */
	private boolean isAcceptedForm(URL url) {

		try {
			HttpURLConnection connection = (HttpURLConnection) url
					.openConnection();
			connection.setRequestMethod("HEAD");
			connection.connect();
			String contentType = connection.getContentType();
			connection.disconnect();

			// Check the Content-type
			if (contentType == null)
				return false;

			for (String content : validContents) {
				if (!contentType.contains(content))
					return true;
			}

		} catch (IOException e) {
			// Ignore cases were URL is malformed
			return false;
		}

		return false;
	}

	/**
	 * Returns a canonical for of the input url by fetching Host and Path part
	 * of the input URL and converting into lowercase Ex:
	 * https://www.ABCD.com/ajshakjsh#, https://www.ABCD.com/ajshakjsh,
	 * http://www.ABCD.com/ajshakjsh, http://www.abcd.com/ajshakjsh all these
	 * above 4 URL's will be returned as www.abcd.com/ajshakjsh
	 * 
	 * @param url
	 *            url to be converted
	 * @return
	 */
	private String getCanonicalForm(URL url) {
		String canonical = url.getHost().toLowerCase().toString()
				+ url.getPath().toLowerCase().toString();
		return canonical;
	}

	/**
	 * 
	 * @param args
	 * @throws MalformedURLException
	 */
	public static void main(String[] args) throws MalformedURLException {
		URL baseURL = new URL("http://www.ccs.neu.edu");
		int threshold = 100;
		String userAgent = "Mozilla/5.0";
		Set<String> validContents = new HashSet<String>(Arrays.asList(
				"text/html", "application/pdf"));
		int delay = 5;
		Set<String> domains = new HashSet<String>(Arrays.asList(
				"northeastern.edu", "neu.edu"));
		Crawler crawler = new Crawler(baseURL, threshold, userAgent,
				validContents, delay, domains, "output.txt");
		crawler.crawl();
	}

}
