package site.ycsb.db;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * Elasticsearch client for YCSB framework.
 *
 * <p>
 * Default properties to set:
 * </p>
 * <ul>
 * <li>cluster.name = es.ycsb.cluster
 * <li>es.index.key = es.ycsb
 * <li>es.number_of_shards = 1
 * <li>es.number_of_replicas = 0
 * </ul>
 */
public class ElasticSearchClient7 extends DB {
  private static final String DEFAULT_CLUSTER_NAME = "es.ycsb.cluster";
  private static final String DEFAULT_INDEX_KEY = "es.ycsb";
  private static final String DEFAULT_REMOTE_HOST = "localhost:9300";
  private static final int NUMBER_OF_SHARDS = 1;
  private static final int NUMBER_OF_REPLICAS = 0;
  private RestHighLevelClient client;
  private String indexKey;
  private Boolean remoteMode;

  /*
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    final Properties props = getProperties();
    // Check if transport client needs to be used (To connect to multiple
    // elasticsearch nodes)
    remoteMode = Boolean.parseBoolean(props.getProperty("es.remote", "false"));
    final String pathHome = props.getProperty("path.home");
    // when running in embedded mode, require path.home
    if (!remoteMode && (pathHome == null || pathHome.isEmpty())) {
      throw new IllegalArgumentException("path.home must be specified when running in embedded mode"
        + "");
    }
    this.indexKey = props.getProperty("es.index.key", DEFAULT_INDEX_KEY);
    int numberOfShards = parseIntegerProperty(props, "es.number_of_shards", NUMBER_OF_SHARDS);
    int numberOfReplicas = parseIntegerProperty(props, "es.number_of_replicas", NUMBER_OF_REPLICAS);
    
    Boolean newdb = Boolean.parseBoolean(props.getProperty("es.newdb", "false"));
    Builder settings = Settings.builder().put("cluster.name", DEFAULT_CLUSTER_NAME)
        .put("node.local", Boolean.toString(!remoteMode)).put("path.home", pathHome);
    // if properties file contains elasticsearch user defined properties
    // add it to the settings file (will overwrite the defaults).
    // settings.put(props);
    final String clusterName = settings.get("cluster.name");
    System.err.println("Elasticsearch starting node = " + clusterName);
    System.err.println("Elasticsearch node path.home = " + settings.get("path.home"));
    System.err.println("Elasticsearch Remote Mode = " + remoteMode);
    // Remote mode support for connecting to remote elasticsearch cluster
    if (remoteMode) {
      settings.put("client.transport.sniff", true).put("client.transport.ignore_cluster_name", false
      )
      .put("client.transport.ping_timeout", "30s").put("client.transport.nodes_sampler_interval",
      "30s");
      // Default it to localhost:9300
      String[] nodeList = props.getProperty("es.hosts.list", DEFAULT_REMOTE_HOST).split(",");
      HttpHost[] hosts = new HttpHost[nodeList.length];
      for (int i = 0; i < nodeList.length; i++) {
        String[] nodes = nodeList[i].split(":");
    		try {
    			hosts[i] = new HttpHost(InetAddress.getByName(nodes[0]), Integer.parseInt(nodes[1]), "http");
    		} catch (NumberFormatException e) {
    			throw new IllegalArgumentException("Unable to parse port number.", e);
    		} catch (UnknownHostException e) {
    			throw new IllegalArgumentException("Unable to Identify host.", e);
    		}
    	}

		System.out
				.println("Elasticsearch Remote Hosts = " + props.getProperty("es.hosts.list", DEFAULT_REMOTE_HOST));
		client = new RestHighLevelClient(RestClient.builder(hosts));

	} else {
		System.err.println("es.remote flag=false is not applicable");
	}
	GetIndexRequest request = new GetIndexRequest(indexKey);
	try {
		final boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
		if (exists && newdb) {
			DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexKey);
			client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
		}
		if (!exists || newdb) {
			client.indices().create(new CreateIndexRequest(indexKey).settings(Settings.builder()
					.put("index.number_of_shards", numberOfShards).put("index.number_of_replicas", numberOfReplicas)
					.put("index.mapping._id.indexed", true)), RequestOptions.DEFAULT);

		}
		client.cluster().health(new ClusterHealthRequest().waitForGreenStatus(), RequestOptions.DEFAULT);
	} catch (IOException e) {
		throw new DBException(e);
	}
  }

  @Override
  public void cleanup() throws DBException {
	if (!remoteMode) {

	} else {
		try {
			client.close();
		} catch (IOException e) {
			throw new DBException(e);
		}
	}
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
	try {
		GetRequest getRequest = new GetRequest(table, key);
		final GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
		if (response.isExists()) {
			if (fields != null) {
				for (String field : fields) {
					result.put(field, new StringByteIterator((String) response.getSource().get(field)));
				}
			} else {
				for (String field : response.getSource().keySet()) {
					result.put(field, new StringByteIterator((String) response.getSource().get(field)));
				}

			}
			return Status.OK;
		} else {
			return Status.NOT_FOUND;
		}
	} catch (Exception e) {
		return Status.ERROR;
	}
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
		Vector<HashMap<String, ByteIterator>> result) {
	try {
		HashMap<String, ByteIterator> each_result = new HashMap<String, ByteIterator>();
		GetRequest getRequest = new GetRequest(table, startkey);
		final GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
		if (response.isExists()) {
			if (fields != null) {
				for (String field : fields) {
					each_result.put(field, new StringByteIterator((String) response.getSource().get(field)));
				}
				result.add(each_result);
			} else {
				for (String field : response.getSource().keySet()) {
					each_result.put(field, new StringByteIterator((String) response.getSource().get(field)));
				}
				result.add(each_result);
			}
			return Status.OK;
		} else {
			return Status.NOT_FOUND;
		}
	} catch (Exception e) {
		return Status.ERROR;
	}
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
	try {
		GetRequest getRequest = new GetRequest(table, key);
		final GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
		if (response.isExists()) {
			for (Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
				response.getSource().put(entry.getKey(), entry.getValue());
			}
			UpdateRequest updateRequest = new UpdateRequest(table, key).doc(response.getSource());
			UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
			System.out.print("Update Index Response " + updateResponse.status());
			return Status.OK;
		} else {
			return Status.NOT_FOUND;
		}
	} catch (Exception e) {
		return Status.ERROR;
	}
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
	try {
		XContentBuilder doc;
		doc = jsonBuilder().startObject();
		for (Entry<String, String> entry : StringByteIterator.getStringMap(values).entrySet()) {
			doc.field(entry.getKey(), entry.getValue());
		}
		doc.endObject();
		IndexRequest indexRequest = new IndexRequest(table).id(indexKey).source(doc);
		IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
		System.out.print("Insert Index Response " + indexResponse.status());
		return Status.OK;
	} catch (Exception e) {
		return Status.ERROR;
	}

  }

  @Override
  public Status delete(String table, String key) {
	try {
		DeleteRequest deleteRequest = new DeleteRequest(table, indexKey);
		DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
		System.out.print("Delete Index Response " + deleteResponse.status());
		return Status.OK;
	} catch (Exception e) {
		return Status.ERROR;
	}
  }

  private int parseIntegerProperty(Properties properties, String key, int defaultValue) {
	String value = properties.getProperty(key);
	return value == null ? defaultValue : Integer.parseInt(value);
  }

}
