package it.gcap.elasticsearch.river.reddit;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.threadpool.ThreadPool;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 *
 */
public class RedditRiver extends AbstractRiverComponent implements River {

	protected final ThreadPool threadPool;
	private final Client client;
	private AtomicInteger onGoingBulks = new AtomicInteger();
	private volatile BulkRequestBuilder currentRequest;
	private volatile Thread thread;
	private volatile boolean closed = false;

	// ES parameters
	private String indexName;
	private String typeName;
	private int bulkSize;
	private int dropThreshold;

	// Reddit parameters
	private final String subReddit;
	private final long sleepTime;
	private String serverAddress;
	private int limit = 100;

	private String mostRecentId;
	private String leastRecentId;

	@SuppressWarnings({ "unchecked" })
	@Inject
	public RedditRiver(RiverName riverName, RiverSettings settings, Client client, ThreadPool threadPool) {
		super(riverName, settings);
		this.client = client;
		this.threadPool = threadPool;

		// reddit settings
		if (settings.settings().containsKey("reddit")) {
			Map<String, Object> redditSettings = (Map<String, Object>) settings.settings().get("reddit");
			subReddit = XContentMapValues.nodeStringValue(redditSettings.get("subreddit"), "reactiongifs");

			sleepTime = XContentMapValues.nodeLongValue(redditSettings.get("sleeptime"), 60000);

			limit = XContentMapValues.nodeIntegerValue(redditSettings.get("sleeptime"), 100);
		} else {
			subReddit = "reactionGifs";
			sleepTime = 60000;
			limit = 100;
		}

		serverAddress = "http://www.reddit.com/r/" + subReddit + "/new.json?sort=new&limit=" + limit;

		logger.info("creating reddit river for subreddit [{}]", subReddit);

		// index settings
		if (settings.settings().containsKey("index")) {
			Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
			indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), riverName.name());
			typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), "reddit_story");
			this.bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
			this.dropThreshold = XContentMapValues.nodeIntegerValue(indexSettings.get("drop_threshold"), 10);
		} else {
			indexName = riverName.name();
			typeName = "reddit";
			bulkSize = 100;
			dropThreshold = 10;
		}

	}

	@Override
	public void start() {
		logger.info("starting reddit stream");
		try {
			String mapping = XContentFactory.jsonBuilder().startObject().startObject(typeName)
					.startObject("properties").startObject("subreddit").field("type", "string")
					.field("index", "not_analyzed").endObject().startObject("title").field("type", "string")
					.field("index", "analyzed").endObject().startObject("thumbnail").field("type", "string")
					.field("index", "not_analyzed").endObject().startObject("permalink").field("type", "string")
					.field("index", "not_analyzed").endObject().startObject("created_utc").field("type", "long")
					.endObject().startObject("url").field("type", "string").field("index", "not_analyzed").endObject()
					.endObject().endObject().endObject().string();
			client.admin().indices().prepareCreate(indexName).addMapping(typeName, mapping).execute().actionGet();
		} catch (Exception e) {
			if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
				// that's fine, get the first and last ids
				retrieveIndicizedIds();
			} else if (ExceptionsHelper.unwrapCause(e) instanceof ClusterBlockException) {
				// ok, not recovered yet..., lets start indexing and hope we
				// recover by the first bulk
				// TODO: a smarter logic can be to register for cluster event
				// listener here, and only start sampling when the block is
				// removed...
			} else {
				logger.warn("failed to create index [{}], disabling river...", e, indexName);
				return;
			}
		}
		currentRequest = client.prepareBulk();

		thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "reddit_slurper").newThread(
				new RedditCrawler());
		thread.start();
	}

	@Override
	public void close() {
		this.closed = true;
		logger.info("closing reddit river");
		if (thread != null)
			thread.interrupt();
	}

	private void retrieveIndicizedIds() {
		SearchResponse resp = new SearchRequestBuilder(client).setIndices(indexName).setSize(1)
				.addSort("id", SortOrder.ASC).execute().actionGet();
		SearchHit[] hits = resp.getHits().getHits();

		if (hits.length > 0) {
			leastRecentId = hits[0].getId();
			logger.info("Least Recent id in index: {}", leastRecentId);
		}

		resp = new SearchRequestBuilder(client).setSize(1).setIndices(indexName).addSort("id", SortOrder.DESC)
				.execute().actionGet();

		hits = resp.getHits().getHits();
		if (hits.length > 0) {
			mostRecentId = hits[0].getId();
			logger.info("Most Recent id in index: {}", mostRecentId);
		}

	}

	public class RedditCrawler implements Runnable {
		HttpURLConnection connection = null;
		// when we have them all..
		boolean stopGoingBackwards = false;
		
		@Override
		public void run() {
			while (true) {
				if (closed)
					return;

				try {
					// from most recent
					RetrievedInterval retrieved = retrieveStories(mostRecentId, true);
					mostRecentId = retrieved.getFirstId();
					if (leastRecentId == null)
						leastRecentId = retrieved.getLastId();

					// until least recent
					if (leastRecentId != null && !stopGoingBackwards) {
						retrieved = retrieveStories(leastRecentId, false);
						if (retrieved.getCount() == 0) {
							// last backward search has 0 results
							logger.info("Stopped going backwards, we have them all!");
							stopGoingBackwards = true;
						} else
							leastRecentId = retrieved.getLastId();
					}

					logger.trace("New mostRecentId: {} - New leastRecentId: {}", mostRecentId, leastRecentId);

					logger.debug("sleeping {}", sleepTime);
					Thread.sleep(sleepTime);
				} catch (Exception e) {
					if (closed)
						return;

					logger.error("failed to parse stream", e);
				}
			}
		}

		private RetrievedInterval retrieveStories(String fromId, boolean forward) throws MalformedURLException,
				IOException {
			String currentAddress = serverAddress;
			if (fromId != null && forward)
				currentAddress += "&before=" + fromId;
			else if (fromId != null && !forward)
				currentAddress += "&after=" + fromId;

			InputStreamReader inputStreamReader = null;
			HttpURLConnection connection = null;

			try {
				logger.debug("Requesting Reddit stories at url {} ", currentAddress);
				connection = (HttpURLConnection) new URL(currentAddress).openConnection();
				connection.addRequestProperty("User-Agent",
						"gcap's reddit crawler - email me at paradigma@gmail.com if there are any problems!");
				connection.setReadTimeout(30000);
				connection.connect();

				inputStreamReader = new InputStreamReader(connection.getInputStream());

				JSONObject data = (JSONObject) (((JSONObject) JSONValue.parse(inputStreamReader)).get("data"));
				JSONArray stories = (JSONArray) data.get("children");

				String firstId = null, lastId = null;
				for (int i = 0; i < stories.size(); i++) {
					JSONObject story = (JSONObject) ((JSONObject) stories.get(i)).get("data");

					XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
					String id = (String) story.get("name");

					// save the first id as most recent, "before" field could be
					// null when getting the newest stories
					if (i == 0)
						firstId = id;
					lastId = id;

					builder.field("id", id);
					builder.field("subreddit", story.get("subreddit"));
					builder.field("title", story.get("title"));
					builder.field("thumbnail", story.get("thumbnail"));
					builder.field("permalink", story.get("permalink"));
					builder.field("created_utc", story.get("created_utc"));
					builder.field("url", story.get("url"));
					builder.endObject();

					if (logger.isTraceEnabled())
						logger.trace("adding to index {}, type {} with id {}: {}", indexName, typeName, id,
								builder.string());

					currentRequest.add(Requests.indexRequest(indexName).type(typeName).id(id).create(true)
							.source(builder));
				}
				if (stories.size() > 0)
					logger.info("Got {} {} stories", stories.size(), forward ? "new" : "old");
				else
					logger.debug("Got 0 {} stories", forward ? "new" : "old");

				processBulkIfNeeded();

				return new RetrievedInterval(firstId, lastId, stories.size());
			} finally {

				if (inputStreamReader != null) {
					try {
						inputStreamReader.close();
					} catch (IOException e) {
					}
				}
				if (connection != null)
					connection.disconnect();
				connection = null;
			}

		}

		private void processBulkIfNeeded() {
			if (logger.isDebugEnabled())
				logger.debug("currentRequest.numberOfActions={} - bulkSize={}", currentRequest.numberOfActions(),
						bulkSize);

			if (currentRequest.numberOfActions() >= bulkSize) {
				// execute the bulk operation
				int currentOnGoingBulks = onGoingBulks.incrementAndGet();

				if (currentOnGoingBulks > dropThreshold) {
					onGoingBulks.decrementAndGet();
					logger.warn("dropping bulk, [{}] crossed threshold [{}]", onGoingBulks, dropThreshold);
				} else {
					try {
						logger.debug("executing bulk request");
						currentRequest.execute(new ActionListener<BulkResponse>() {
							@Override
							public void onResponse(BulkResponse bulkResponse) {
								onGoingBulks.decrementAndGet();
							}

							@Override
							public void onFailure(Throwable e) {
								logger.warn("failed to execute bulk");
							}
						});
					} catch (Exception e) {
						logger.warn("failed to process bulk", e);
					}
				}
				currentRequest = client.prepareBulk();
			}
		}
	}

}
