using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace WebOfData
{
	public interface IQueryResponse {
		string NextDataToken { get; }
		IList<JObject> Data { get; }
		JObject Context { get; set; }
	}

	public interface IGetEntitiesResponse {
		string NextDataToken { get; }
		IList<JObject> Data {get; }
		JObject Context { get; }
	}

	public interface IGetChangesResponse {
		string NextDataToken { get; }
		IList<JObject> Data {get; }
		JObject Context { get; set; }
	}

	public interface IDataset {
		JObject Entity { get; }
		string  Name { get; }
	}

	public interface IStore {
		JObject Entity { get; }
		string  Name { get; }
	}

    public interface IServiceInfo
    {
        string Endpoint { get; }
        JObject Entity { get; }
    }

	public interface IWoDClient {
        Task<IServiceInfo> GetServiceInfo();
		Task<IEnumerable<IStore>> GetStores();
		Task DeleteStore(string storeName);
        Task<IStore> CreateStore(string name, JObject storeEntity);
		Task UpdateStore(string name, JObject storeEntity);
        Task<IStore> GetStore(string name);
		Task<IEnumerable<IDataset>> GetDatasets(string storeName);
        Task<IDataset> GetDataset(string storeName, string datasetName);
        Task<IDataset> CreateDataset(string storeName, string datasetName, JObject datasetEntity);
        Task UpdateDataset(string storeName, string datasetName, JObject datasetEntity);
        Task DeleteDataset(string storeName, string datasetName); // deletes all entities and the dataset itself.
		Task DeleteEntities(string storeName, string datasetName); // deletes all the data but leaves the dataset
        Task<IGetEntitiesResponse> GetEntities(string storeName, string datasetName, string nextDataToken = null, int take = -1);
        Task<List<string>> GetEntitiesPartitions(string storeName, string datasetName, int partitionCount);
        Task<IGetChangesResponse> GetChanges(string storeName, string datasetName, string nextDataToken = null, int take = -1);
		Task<List<string>> GetChangesPartitions(string storeName, string datasetName, int partitionCount);
        Task UpdateEntities(string storeName, string datasetName, IEnumerable<JObject> entities);
        Task UpdateEntities(string storeName, string datasetName, string wodJsonFilename);
        Task<IQueryResponse> GetRelatedEntities(string storeName, string subject, string property, bool inverse, List<string> datasets);
		Task<QueryResponse> GetEntity(string storeName, string subjectIdentifier, List<string> datasets);
	}
}
