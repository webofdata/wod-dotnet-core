
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Text;
using Newtonsoft.Json;
using System.IO;
using System;
using System.Threading.Tasks;

namespace WebOfData {

    public class CommunicationException : Exception {
        public CommunicationException(string msg) : base(msg) {
            
        }
    }

    public class WoDService : IWoDService
    {
        public JObject Entity { get; set; }

        public string Name { get; set; }
    }

    public class Store : IStore
    {
        public JObject Entity { get; set; }

        public string Name { get; set; }
    }

    public class Dataset : IDataset
    {
        public JObject Entity { get; set; }

        public string Name { get; set; }
    }

    public class GetEntitiesResponse : IGetEntitiesResponse
    {
        public string NextDataToken { get; set; }

        public IEnumerable<JObject> Data { get; set; }
    }

    public class GetChangesResponse : IGetChangesResponse
    {
        public string NextDataToken { get; set; }

        public IEnumerable<JObject> Data { get; set; }
    }

    public class Transaction : ITransaction
    {
        public string TransactionId { get; set; }

        public IEnumerable<JObject> Entities { get; set; }    
    }

    public class WoDClient : IWoDClient {

		private string _host;
		private int _port;
        private string _baseUrl;
        private string _jwtToken;

        public WoDClient(string host, int port=80, string jwtToken=null) {
            _host = host;
            _port = port;
            _jwtToken = jwtToken;
            if (port == 80)
            {
                _baseUrl = host + "/";
            }
            else
            {
                _baseUrl = host + ":" + _port.ToString() + "/";
            }
        }

        public IWoDService GetService() {
            return new WoDService();
        }

		public async Task<IEnumerable<IStore>> GetStores() {
            var req = (HttpWebRequest) WebRequest.Create(_baseUrl + "stores");
            var stores = new List<IStore>();
            using (var response = await req.GetResponseAsync())
            {
                var result = JToken.ReadFrom(new JsonTextReader(new StreamReader(response.GetResponseStream()))) as JArray;
                foreach (JObject s in result)
                {
                    var name = s["name"].ToString();
                    var store = new Store() { Name = name, Entity = s["entity"] as JObject };
                    stores.Add(store);
                }
            }
            return stores;
        }

        public async Task DeleteStore(string storeName) {
            var req = (HttpWebRequest)WebRequest.Create(_baseUrl + "stores/" + storeName);
            req.Method = "DELETE";
            using (var response = await req.GetResponseAsync() as HttpWebResponse)
            {
                if (response.StatusCode != HttpStatusCode.OK)
                {
                    throw new System.Exception("Error deleting store " + storeName);
                }
            }
        }
		
		public async Task<IStore> CreateStore(string name, JObject storeEntity) {
            JObject requestObj = new JObject();
            requestObj["name"] = name;
            requestObj["entity"] = storeEntity;
            var jsonDataBytes = Encoding.UTF8.GetBytes(requestObj.ToString());
            var dataLength = jsonDataBytes.Length;

            var req = (HttpWebRequest)WebRequest.Create(_baseUrl + "stores");
            req.Headers.Add("ContentType", "application/json; charset=utf-8");
            req.Method = "POST";
            req.Timeout = 20000;
            req.ContentLength = dataLength;

            using (var requestStream = await req.GetRequestStreamAsync())
            {                
                requestStream.Write(jsonDataBytes, 0, dataLength);
            }

            using (var response = await req.GetResponseAsync() as HttpWebResponse) {
                //  check return code
                if (response.StatusCode == HttpStatusCode.Created) {
                    return new Store() { Name = name , Entity = storeEntity};                
                }

                if (response.StatusCode == HttpStatusCode.BadRequest) {
                    throw new CommunicationException("Bad request in data " + requestObj);
                }

                throw new CommunicationException("Error in request " + response.StatusCode);
            }
		}

        public async Task UpdateStore(string name, JObject storeEntity) {


		}

        public async Task<IStore> GetStore(string name) {
            var req = (HttpWebRequest)WebRequest.Create(_baseUrl + "stores/" + name);
            using (var response = await req.GetResponseAsync())
            {
                var result = JToken.ReadFrom(new JsonTextReader(new StreamReader(response.GetResponseStream()))) as JObject;
                var store = new Store() { Name = name, Entity = result["entity"] as JObject };
                return store;
            }
        }

		public async Task<IEnumerable<IDataset>> GetDatasets(string storeName) {
            var req = (HttpWebRequest)WebRequest.Create(_baseUrl + "stores/" + storeName + "/datasets");
            var datasets = new List<IDataset>();
            using (var response = await req.GetResponseAsync())
            {
                var result = JToken.ReadFrom(new JsonTextReader(new StreamReader(response.GetResponseStream()))) as JArray;
                foreach (JObject s in result)
                {
                    var name = s["name"].ToString();
                    var dataset = new Dataset() { Name = name, Entity = s["entity"] as JObject };
                    datasets.Add(dataset);
                }
            }
            return datasets;
        }

		public async Task<IDataset> CreateDataset(string storeName, string datasetName, JObject datasetEntity) {
            var req = (HttpWebRequest)WebRequest.Create(_baseUrl + "stores/" + storeName + "/datasets");
            return null;
		}

        public async Task<IDataset> GetDataset(string storeName, string datasetName){
            var req = (HttpWebRequest)WebRequest.Create(_baseUrl + "stores/" + storeName + "/datasets/" + datasetName);
            using (var response = await req.GetResponseAsync())
            {
                var result = JToken.ReadFrom(new JsonTextReader(new StreamReader(response.GetResponseStream()))) as JObject;
                var dataset = new Dataset() { Name = datasetName, Entity = result["entity"] as JObject };
                return dataset;
            }
        }

        public async Task UpdateDataset(string storeName, string datasetName, JObject datasetEntity)
        {

        }

        public async Task MergeDatasets(string storeName, string targetDatasetName, string sourceDatasetName) {
            
        }

        public async Task DeleteDataset(string storeName, string datasetName) {

		} // deletes all entities and the dataset itself.
		
		public async Task DeleteEntities(string storeName, string datasetName) {

		} // deletes all the data but leaves the dataset
		
		public async Task<IGetEntitiesResponse> GetEntities(string storeName, string datasetName, string nextDataToken = null) {
            string url = _baseUrl + "stores/" + storeName + "/datasets/" + datasetName + "/entities";
            if (nextDataToken != null) {
                url += "?token=" + nextDataToken; // TODO: url encode
            }
            var req = (HttpWebRequest)WebRequest.Create(url);
            var resp = new GetEntitiesResponse();
            using (var response = await req.GetResponseAsync())
            {
                resp.NextDataToken = response.Headers["x-wod-next-data"];
                var result = JToken.ReadFrom(new JsonTextReader(new StreamReader(response.GetResponseStream()))) as JArray;
                resp.Data = result;
                return resp;
            }
		}

		public async Task<IGetChangesResponse> GetChanges(string storeName, string datasetName, string nextDataToken = null) {
            return new GetChangesResponse();
		}

		public async Task UpdateEntities(string storeName, string datasetName, IEnumerable<JObject> entities) {

		}

		public async Task<ITransaction> CreateTransaction(string storeName) {
            return new Transaction();
		}

		public async Task<IEnumerable<ITransaction>> GetTransactions(string storeName) {
            var req = (HttpWebRequest)WebRequest.Create(_baseUrl + "stores/" + storeName + "/transactions");
            var transactions = new List<ITransaction>();
            using (var response = await req.GetResponseAsync())
            {
                var result = JToken.ReadFrom(new JsonTextReader(new StreamReader(response.GetResponseStream()))) as JArray;
                foreach (JObject s in result)
                {
                    // add to list
                }
            }
            return transactions;
        }

        public async Task<IEnumerable<JObject>> Query(string storeName, string subject, string property, bool inverse)
        {
            throw new System.NotImplementedException();
        }

        public Task<List<string>> GetEntitiesPartitions(string storeName, string datasetName, string nextDataToken = null)
        {
            throw new NotImplementedException();
        }

        public Task<List<string>> GetChangesPartitions(string storeName, string datasetName, string nextDataToken = null)
        {
            throw new NotImplementedException();
        }
    }
}