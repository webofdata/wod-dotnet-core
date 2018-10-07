
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using System.Net;
using System.Text;
using Newtonsoft.Json;
using System.IO;
using System;
using System.Threading.Tasks;

namespace WebOfData {

    public class ServiceException : Exception {
        public ServiceException(string msg) : base(msg) {            
        }
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

    public class ServiceInfo : IServiceInfo {
        public string Endpoint { get; set; }
        public JObject Entity { get; set; }
    }

    public class GetEntitiesResponse : IGetEntitiesResponse
    {
        public string NextDataToken { get; set; }
        public IList<JObject> Data { get; set; }
        public JObject Context { get; set; }
    }

    public class QueryResponse : IQueryResponse {
        public string NextDataToken { get; set; }
        public IList<JObject> Data { get; set; }
        public JObject Context { get; set; }
    }

    public class GetChangesResponse : IGetChangesResponse
    {
        public string NextDataToken { get; set; }
        public IList<JObject> Data { get; set; }
        public JObject Context { get; set; }
    }

    public static class WoDUtils {
        public static string GetPrefixForNamespace(JObject context, string ns) {
            var namespaces = context["namespaces"] as JObject;
            if (namespaces == null) {
                throw new Exception("invalid context missing namespaces key");                
            }
            foreach (var kvp in namespaces) {
                if (kvp.Value.ToString() == ns) return kvp.Key;
            }
            return null;
        }

        public static string GetExpansionForPrefix(JObject context, string prefix) {
            var namespaces = context["namespaces"] as JObject;
            if (namespaces == null) {
                throw new Exception("invalid context missing namespaces key");                
            }
            var expansion = namespaces[prefix];
            return expansion.ToString();
        }

        public static string ResolveFullUri(string val, JObject context) {
            if (val.StartsWith("http://")) {
                return val;
            } else if (val.Contains(":")) {
                var bits = val.Split(':');
                var expansion = GetExpansionForPrefix(context, bits[0]);
                return expansion + bits[1];
            } else {
                var expansion = GetExpansionForPrefix(context, "_");
                return expansion + val;                
            }
        }

        public static string GetId(this JObject obj) {
            return obj["@id"].ToString();
        }

        public static string GetFullId(this JObject obj, JObject context){
            return ResolveFullUri(obj.GetId(), context);
        }
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


        public async Task<IServiceInfo> GetServiceInfo() {
            var req = (HttpWebRequest) WebRequest.Create(_baseUrl + "info");
            using (var response = await req.GetResponseAsync())
            {
                var result = JToken.ReadFrom(new JsonTextReader(new StreamReader(response.GetResponseStream()))) as JObject;
                var endpoint = result["endpoint"].ToString();
                var serviceInfo = new ServiceInfo() { Endpoint = endpoint, Entity = result["entity"] as JObject };
                return serviceInfo;
            }
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
		
        private async Task<HttpWebResponse> ExecuteRequest(HttpWebRequest request) {
            try
            {
                return await request.GetResponseAsync() as HttpWebResponse;
            }
            catch (WebException wex)
            {
                var response = wex.Response as HttpWebResponse;
                if (response.StatusCode == HttpStatusCode.BadRequest)
                {
                    var receiveStream = response.GetResponseStream();
                    StreamReader readStream = new StreamReader(receiveStream, Encoding.UTF8);
                    throw new ServiceException("Bad Request: " + readStream.ReadToEnd());
                }

                throw new ServiceException("Error in request " + response.StatusCode);
            }
        }

		public async Task<IStore> CreateStore(string name, JObject storeEntity = null) {
            JObject requestObj = new JObject();
            requestObj["name"] = name;
            if (storeEntity != null)
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

            using (var response = await ExecuteRequest(req))
            {
                //  check return code
                if (response.StatusCode == HttpStatusCode.Created)
                {
                    return new Store() { Name = name, Entity = storeEntity };
                }

                throw new ServiceException("Request accepted by server but wrong response code returned. Should be 201 Created but was: " + response.StatusCode);
            }
		}

        public async Task UpdateStore(string name, JObject storeEntity) {

            if (name == null || name.Length == 0) throw new Exception("name cannot be null or empty");
            if (storeEntity == null) throw new Exception("storeEntity cannot be null");

            var jsonDataBytes = Encoding.UTF8.GetBytes(storeEntity.ToString());
            var dataLength = jsonDataBytes.Length;

            var req = (HttpWebRequest)WebRequest.Create(_baseUrl + "stores/" + name);
            req.Headers.Add("ContentType", "application/json; charset=utf-8");
            req.Method = "PUT";
            req.Timeout = 20000;
            req.ContentLength = dataLength;

            using (var requestStream = await req.GetRequestStreamAsync())
            {
                requestStream.Write(jsonDataBytes, 0, dataLength);
            }

            using (var response = await ExecuteRequest(req))
            {
                //  check return code
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    return;
                }
            }
		}  

        public async Task<IStore> GetStore(string name) {
            var req = (HttpWebRequest) WebRequest.Create(_baseUrl + "stores/" + name);
            try {
                using (var response = await req.GetResponseAsync() as HttpWebResponse)
                {
                    var result = JToken.ReadFrom(new JsonTextReader(new StreamReader(response.GetResponseStream()))) as JObject;
                    var store = new Store() { Name = name, Entity = result["entity"] as JObject };
                    return store;
                }                
            } catch (System.Net.WebException webex) {
                var wr = (HttpWebResponse)webex.Response;
                if (wr.StatusCode == HttpStatusCode.NotFound) {
                    return null;
                }
                throw webex;
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

		public async Task<IDataset> CreateDataset(string storeName, string datasetName, JObject datasetEntity = null) {
            
            JObject requestObj = new JObject();
            requestObj["name"] = datasetName;
            if (datasetEntity != null)
                requestObj["entity"] = datasetEntity;
 
            var jsonDataBytes = Encoding.UTF8.GetBytes(requestObj.ToString());
            var dataLength = jsonDataBytes.Length;

            var req = (HttpWebRequest)WebRequest.Create(_baseUrl + "stores/" + storeName + "/datasets");
            req.Headers.Add("ContentType", "application/json; charset=utf-8");
            req.Method = "POST";
            req.Timeout = 20000;
            req.ContentLength = dataLength;

            using (var requestStream = await req.GetRequestStreamAsync())
            {
                requestStream.Write(jsonDataBytes, 0, dataLength);
            }

            using (var response = await ExecuteRequest(req))
            {
                return new Dataset() { Name = datasetName, Entity = datasetEntity };
            }
		}

        public async Task<IDataset> GetDataset(string storeName, string datasetName){
            var req = (HttpWebRequest)WebRequest.Create(_baseUrl + "stores/" + storeName + "/datasets/" + datasetName);
            try {
                using (var response = await req.GetResponseAsync())
                {
                    var result = JToken.ReadFrom(new JsonTextReader(new StreamReader(response.GetResponseStream()))) as JObject;
                    var dataset = new Dataset() { Name = datasetName, Entity = result["entity"] as JObject };
                    return dataset;
                }
            } catch (WebException wex) {
                var resp = wex.Response as HttpWebResponse;
                if (resp.StatusCode == HttpStatusCode.NotFound) {
                    return null;
                } else {
                    throw new ServiceException(resp.StatusDescription);
                }
            }
        }

        public async Task UpdateDataset(string storeName, string datasetName, JObject datasetEntity)
        {
            var jsonDataBytes = Encoding.UTF8.GetBytes(datasetEntity.ToString());
            var dataLength = jsonDataBytes.Length;

            var req = (HttpWebRequest)WebRequest.Create(_baseUrl + "stores/" + storeName + "/datasets/" + datasetName);
            req.Headers.Add("ContentType", "application/json; charset=utf-8");
            req.Method = "PUT";
            req.Timeout = 20000;
            req.ContentLength = dataLength;

            using (var requestStream = await req.GetRequestStreamAsync())
            {
                requestStream.Write(jsonDataBytes, 0, dataLength);
            }

            using (var response = await req.GetResponseAsync() as HttpWebResponse)
            {
                //  check return code
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    return;
                }
            }
        }

        public async Task DeleteDataset(string storeName, string datasetName) {
            var req = (HttpWebRequest)WebRequest.Create(_baseUrl + "stores/" + storeName + "/datasets/" + datasetName);
            req.Method = "DELETE";
            using (var response = await req.GetResponseAsync() as HttpWebResponse)
            {
                //  check return code
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    return;
                }
            }
		} 

		public async Task DeleteEntities(string storeName, string datasetName) {

		} // deletes all the data but leaves the dataset
		
		public async Task<IGetEntitiesResponse> GetEntities(string storeName, string datasetName, string nextDataToken = null, int takeCount = -1) {
            string url = _baseUrl + "stores/" + storeName + "/datasets/" + datasetName + "/entities";
            if (nextDataToken != null) {
                url += "?token=" + nextDataToken; // TODO: url encode
            }

            if (takeCount > 0) {
                if (nextDataToken != null) {
                    url += "&take=" + takeCount.ToString();
                } else {
                    url += "?take=" + takeCount.ToString();     
                }
            }

            var req = (HttpWebRequest)WebRequest.Create(url);
            var resp = new GetEntitiesResponse();
            resp.Data = new List<JObject>(); 
            using (var response = await req.GetResponseAsync())
            {                
                var result = JArray.ReadFrom(new JsonTextReader(new StreamReader(response.GetResponseStream())));
                foreach (JObject e in result.Children<JObject>()) {
                    if (e["@id"].ToString() == "@continuation"){
                        resp.NextDataToken = e["wod:next-data"].ToString();
                    } else if (e["@id"].ToString() == "@context") {
                        resp.Context = e;
                    } else {
                        resp.Data.Add(e);
                    }
                }
                return resp;
            }
		}

		public async Task<IGetChangesResponse> GetChanges(string storeName, string datasetName, string nextDataToken = null, int takeCount = -1) {

            string url = _baseUrl + "stores/" + storeName + "/datasets/" + datasetName + "/changes";
            if (nextDataToken != null) {
                url += "?token=" + nextDataToken; // TODO: url encode
            }

            if (takeCount > 0) {
                if (nextDataToken != null) {
                    url += "&take=" + takeCount.ToString();
                } else {
                    url += "?take=" + takeCount.ToString();     
                }
            }

            var req = (HttpWebRequest)WebRequest.Create(url);
            var resp = new GetChangesResponse();
            resp.Data = new List<JObject>(); 
            using (var response = await req.GetResponseAsync())
            {                
                var result = JArray.ReadFrom(new JsonTextReader(new StreamReader(response.GetResponseStream())));
                foreach (JObject e in result.Children<JObject>()) {
                    if (e["@id"].ToString() == "@continuation"){
                        resp.NextDataToken = e["wod:next-data"].ToString();
                    } else if (e["@id"].ToString() == "@context") {
                        resp.Context = e;
                    } else {
                        resp.Data.Add(e);
                    }
                }
                return resp;
            }
		}

		public async Task UpdateEntities(string storeName, string datasetName, IEnumerable<JObject> entities) {
            var req = (HttpWebRequest) WebRequest.Create(_baseUrl + "stores/" + storeName + "/datasets/" + datasetName + "/entities");
            req.Headers.Add("ContentType", "application/json; charset=utf-8");
            req.Method = "POST";
            req.Timeout = 600000;

            using (var requestStream = await req.GetRequestStreamAsync())
            {
                requestStream.WriteByte(Convert.ToByte('['));
                bool doneFirst = false;
                foreach (var e in entities) {
                    if (doneFirst) {
                        requestStream.WriteByte(Convert.ToByte(','));
                    } else {
                        doneFirst = true;
                    }
                    var jsonDataBytes = Encoding.UTF8.GetBytes(e.ToString());
                    requestStream.Write(jsonDataBytes, 0, jsonDataBytes.Length);
                }                
                requestStream.WriteByte(Convert.ToByte(']'));
            }

            using (var response = await ExecuteRequest(req))
            {
            }
		}

        public async Task UpdateEntities(string storeName, string datasetName, string wodJsonFilename) {
            using (WebClient client = new WebClient())
            {
                client.Headers.Add("Content-Type", "application/json");
                using (Stream fileStream = File.OpenRead(wodJsonFilename))
                using (Stream requestStream = client.OpenWrite(new Uri(_baseUrl + "stores/" + storeName + "/datasets/" + datasetName + "/entities"), "POST"))
                {
                    fileStream.CopyTo(requestStream);
                }
            }
        }

        public async Task<IQueryResponse> GetRelatedEntities(string storeName, string subject, string property, bool inverse, List<string> datasets) {
            string url = _baseUrl + "stores/" + storeName + "/query?subject=" + subject;
            if (property == null) {
                property = "*";
            }
            url += "&property=" + property + "&inverse=" + inverse.ToString();

            foreach (var ds in datasets) {
                url += "&dataset=" + ds;
            }

            var req = (HttpWebRequest)WebRequest.Create(url);
            var resp = new QueryResponse();
            resp.Data = new List<JObject>(); 
            using (var response = await req.GetResponseAsync())
            {                
                var result = JArray.ReadFrom(new JsonTextReader(new StreamReader(response.GetResponseStream())));
                foreach (JObject e in result.Children<JObject>()) {
                    if (e["@id"].ToString() == "@continuation"){
                        resp.NextDataToken = e["wod:next-data"].ToString();
                    } else if (e["@id"].ToString() == "@context") {
                        // expand
                        resp.Context = e;
                    } else {
                        resp.Data.Add(e);
                    }
                }
                return resp;
            }
        }
		
        public async Task<QueryResponse> GetEntity(string storeName, string subject, List<string> datasets){
            string url = _baseUrl + "stores/" + storeName + "/query?subject=" + subject;
            foreach (var ds in datasets) {
                url += "&dataset=" + ds;
            }

            var req = (HttpWebRequest)WebRequest.Create(url);
            var resp = new QueryResponse();
            resp.Data = new List<JObject>(); 
            using (var response = await req.GetResponseAsync())
            {                
                var result = JArray.ReadFrom(new JsonTextReader(new StreamReader(response.GetResponseStream())));
                foreach (JObject e in result.Children<JObject>()) {
                    if (e["@id"].ToString() == "@continuation"){
                        resp.NextDataToken = e["wod:next-data"].ToString();
                    } else if (e["@id"].ToString() == "@context") {
                        resp.Context = e;
                    } else {
                        resp.Data.Add(e);
                    }
                }
                return resp;
            }
        }

        public async Task<List<string>> GetEntitiesPartitions(string storeName, string datasetName, int partitionCount)
        {
            string url = _baseUrl + "stores/" + storeName + "/datasets/" + datasetName + "/entities/partitions";
            var req = (HttpWebRequest)WebRequest.Create(url);
            using (var response = await req.GetResponseAsync())
            {                
                var result = JArray.ReadFrom(new JsonTextReader(new StreamReader(response.GetResponseStream())));
                var resp = new List<string>();
                foreach (string token in result.Children()) {
                    resp.Add(token);
                }
                return resp;
            }
        }

        public async Task<List<string>> GetChangesPartitions(string storeName, string datasetName, int partitionCount)
        {
            string url = _baseUrl + "stores/" + storeName + "/datasets/" + datasetName + "/changes/partitions";
            var req = (HttpWebRequest)WebRequest.Create(url);
            using (var response = await req.GetResponseAsync())
            {                
                var result = JArray.ReadFrom(new JsonTextReader(new StreamReader(response.GetResponseStream())));
                var resp = new List<string>();
                foreach (string token in result.Children()) {
                    resp.Add(token);
                }
                return resp;
            }
        }
    }
}