using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace WebOfData
{
	public interface IGetEntitiesResponse {
		string NextDataToken { get; }
		IEnumerable<JObject> Data {get; }
	}

	public interface IGetChangesResponse {
		string NextDataToken { get; }
		IEnumerable<JObject> Data {get; }
	}

	public interface IDataset {
		JObject Entity { get; }
		string  Name { get; }
	}

    public interface IWoDService
    {
        JObject Entity { get; }
        string Name { get; }
    }

	public interface IStore {
		JObject Entity { get; }
		string  Name { get; }
	}

	public interface ITransaction {
		string TransactionId { get; }
		IEnumerable<JObject> Entities { get; set; }
	}

	public interface IWoDClient {
        IWoDService GetService();
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

        Task<IGetEntitiesResponse> GetEntities(string storeName, string datasetName, string nextDataToken = null);
        Task<List<string>> GetEntitiesPartitions(string storeName, string datasetName, string nextDataToken = null);

        Task<IGetChangesResponse> GetChanges(string storeName, string datasetName, string nextDataToken = null);
		Task<List<string>> GetChangesPartitions(string storeName, string datasetName, string nextDataToken = null);

		Task UpdateEntities(string storeName, string datasetName, IEnumerable<JObject> entities);
        Task MergeDatasets(string storeName, string targetDatasetName, string sourceDatasetName);

		Task<ITransaction> CreateTransaction(string storeName);
		Task<IEnumerable<ITransaction>> GetTransactions(string storeName);
		
        Task<IEnumerable<JObject>> Query(string storeName, string subject, string property, bool inverse);
	}
}
