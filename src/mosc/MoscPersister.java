package mosc;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import backtype.storm.tuple.Tuple;

import com.mongodb.DB;
import com.mongodb.Mongo;

import scala.util.parsing.json.JSON;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.State;
import storm.trident.state.StateUpdater;
import storm.trident.state.map.MapState;
import storm.trident.tuple.TridentTuple;

public class MoscPersister extends BaseStateUpdater<MapState>
{
	DB db;
	Jongo jongo;
	
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		try {
			db = new Mongo().getDB("osdcmy");
			jongo = new Jongo(db);
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void updateState(MapState state, List<TridentTuple> tuples,
			TridentCollector collector) {
		for (TridentTuple t : tuples)
		{
			try {
				JSONObject o = (JSONObject) JSONValue.parse(t.getString(0));
				String collectionName = (String) o.get("collection");
				String operationName = (String) o.get("operation");
				if (collectionName == null)
				{
					// Malformed message.
					System.out.println("Malformed message, missing collection name: " + t.getString(0));
					continue;
				}
				
				if (operationName == null)
				{
					// Malformed message.
					System.out.println("Malformed message, missing operation name: " + t.getString(0));
					continue;					
				}
				
				MongoCollection c = jongo.getCollection(collectionName);

				if(operationName.equals("create"))
				{
					c.insert(o.get("data"));					
				}
				else
				if(operationName.equals("update"))
				{
					c.update("{\"_id\":{\"$oid\":#}}", o.get("id"))
						.upsert().merge(o.get("data"));
				}
				else
				{
					System.out.println("Unknown operation specified.");
				}
				
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}

		
	}

}
