using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using WebOfData;
using Newtonsoft.Json.Linq;

namespace WoDClientTests
{
    public class WoDClientTests
    {
        [Fact]
        public async void GetServiceTest()
        {
            var wc = new WoDClient("http://localhost", 8888);
            var serviceInfo = await wc.GetServiceInfo();
            Assert.NotNull(serviceInfo.Entity);
            Assert.NotNull(serviceInfo.Endpoint);
        }

        [Fact]
        public async void CannotCreateStoreWithExistingName() {
            var wc = new WoDClient("http://localhost", 8888);
            var storeName = Guid.NewGuid().ToString("N");

            await wc.CreateStore(storeName);

            ServiceException ex = await Assert.ThrowsAsync<ServiceException>(async () => { await wc.CreateStore(storeName); } );

            Assert.Equal("Bad Request: store name already exists", ex.Message);
        }

        [Fact]
        public async void CannotCreateDatasetWithExistingName()
        {
            var wc = new WoDClient("http://localhost", 8888);
            var storeName = Guid.NewGuid().ToString("N");

            await wc.CreateStore(storeName);

            await wc.CreateDataset(storeName, "people");

            ServiceException ex = await Assert.ThrowsAsync<ServiceException>(async () => { await wc.CreateDataset(storeName, "people"); });

            Assert.Equal("Bad Request: dataset name already exists", ex.Message);
        }

        [Fact]
        public async void DeleteDataset() {
            var wc = new WoDClient("http://localhost", 8888);
            var storeName = Guid.NewGuid().ToString("N");

            await wc.CreateStore(storeName);
            await wc.CreateDataset(storeName, "people");
            await wc.DeleteDataset(storeName, "people");

            var people = await wc.GetDataset(storeName, "people");

            Assert.Null(people);
        }

        [Fact]
        public async void StoreListTests()
        {
            var wc = new WoDClient("http://localhost", 8888);
            var storeName1 = Guid.NewGuid().ToString("N");
            var storeName2 = Guid.NewGuid().ToString("N");

            await wc.CreateStore(storeName2);

            var storeEntity = new JObject();
            storeEntity["@id"] = "http://data.example.com/stores/" + Guid.NewGuid().ToString("N");
            await wc.CreateStore(storeName1, storeEntity);

            var stores = await wc.GetStores();

            var s1 = stores.Where(s => s.Name == storeName1).FirstOrDefault();
            Assert.NotNull(s1);

            var s2 = stores.Where(s => s.Name == storeName2).FirstOrDefault();
            Assert.NotNull(s2);
        }

        [Fact]
        public async void StoreTests() {
            var wc = new WoDClient("http://localhost", 8888);
            var storeName = Guid.NewGuid().ToString("N");
            var storeEntity = new JObject();
            storeEntity["@id"] = "http://data.example.com/stores/" + Guid.NewGuid().ToString("N");
            // storeEntity["name"]
            await wc.CreateStore(storeName, storeEntity);

            // get list of stores
            var stores = await wc.GetStores();
            var exists = stores.Where(x => x.Name == storeName).FirstOrDefault();
            Assert.NotNull(exists);

            var s = await wc.GetStore(storeName);

            Assert.NotNull(s);
            Assert.Equal(storeName, s.Name);
            Assert.NotNull(s.Entity);
            Assert.NotNull(s.Entity["@id"]);

            storeEntity["description"] = "dns node for other nodes";

            // update store entity
            await wc.UpdateStore(storeName, storeEntity);

            s = await wc.GetStore(storeName);
            Assert.Equal("dns node for other nodes", s.Entity["description"]);

            // delete store
            await wc.DeleteStore(storeName);

            s = await wc.GetStore(storeName);
            Assert.Null(s);
        }

        [Fact]
        public async void ListStoreDatasets() {
            var wc = new WoDClient("http://localhost", 8888);
            var storeName = Guid.NewGuid().ToString("N");
            var storeEntity = new JObject();
            storeEntity["@id"] = "http://data.example.com/stores/" + Guid.NewGuid().ToString("N");
            var s = await wc.CreateStore(storeName, storeEntity);

            var datasetEntity = new JObject();
            datasetEntity["description"] = "people";

            await wc.CreateDataset(storeName, "people", datasetEntity);
            await wc.CreateDataset(storeName, "companies");
            await wc.CreateDataset(storeName, "products");
            await wc.CreateDataset(storeName, "documents");

            var datasets = await wc.GetDatasets(storeName);

            Assert.Equal(4, datasets.Count());

            var people = datasets.FirstOrDefault(ds => ds.Name == "people");
            Assert.NotNull(people);
            Assert.Equal("people", people.Entity["description"].ToString());

        }

        [Fact]
        public async void DatasetTests()
        {
            var wc = new WoDClient("http://localhost", 8888);
            var storeName = Guid.NewGuid().ToString("N");
            var storeEntity = new JObject();
            storeEntity["@id"] = "http://data.example.com/stores/" + Guid.NewGuid().ToString("N");
            var s = await wc.CreateStore(storeName, storeEntity);

            var people = await wc.CreateDataset(storeName, "people", null);

            var companiesDatasetEntity = new JObject();
            companiesDatasetEntity["@id"] = "http://data.example.com/datasets/" + Guid.NewGuid().ToString("N");
            var companies = await wc.CreateDataset(storeName, "companies", companiesDatasetEntity);

            Assert.NotNull(companies);
            Assert.NotNull(companies.Entity);
            Assert.NotNull(companies.Entity["@id"]);

            companies = await wc.GetDataset(storeName, "companies");
            Assert.NotNull(companies);
            Assert.NotNull(companies.Entity);
            Assert.NotNull(companies.Entity["@id"]);

            companies.Entity["description"] = "a nice dataset";

            await wc.UpdateDataset(storeName, "companies", companies.Entity);
            companies = await wc.GetDataset(storeName, "companies");
            Assert.NotNull(companies);
            Assert.NotNull(companies.Entity);
            Assert.NotNull(companies.Entity["@id"]);
            Assert.Equal("a nice dataset", companies.Entity["description"].ToString());

        }

        public List<JObject> GenerateSampleData(int count, int idstart = 0, int fieldCount = 0, int refCount = 0) {
            var res = new List<JObject>();
            int i = 1;

            var context = new JObject();
            context["@id"] = "@context";
            context["namespaces"] = new JObject();
            context["namespaces"]["schema"] = "http://data.webofdata.io/schema/";
            context["namespaces"]["_"] = "http://data.webofdata.io/things/";
            res.Add(context);

            while (i <= count) {
                var obj = new JObject();
                obj["@id"] = "object" + idstart;
                obj["schema:name"] = "Object " + idstart;

                int j = 0;
                while (j <= fieldCount) {
                    obj["field" + j] = "some value of j";
                    j++;
                }

                int k = 0;
                while (k <= refCount) {
                    obj["ref-field" + k] = "<object" + k + ">"; 
                    k++;
                }

                i++;
                idstart++;
                res.Add(obj);
            }

            return res;
        }

        [Fact]
        public async void UpdateEntities() {
            
            var wc = new WoDClient("http://localhost", 8888);
            var storeName = Guid.NewGuid().ToString("N");
            var storeEntity = new JObject();
            storeEntity["@id"] = "http://data.example.com/stores/" + Guid.NewGuid().ToString("N");
            var s = await wc.CreateStore(storeName, storeEntity);
            var people = await wc.CreateDataset(storeName, "people", null);

            await wc.UpdateEntities(storeName, "people", GenerateSampleData(100, 0, 10, 5));
        }

        [Fact]
        public async void GetEntityBySubjectIdentifier() {
            var wc = new WoDClient("http://localhost", 8888);
            var storeName = Guid.NewGuid().ToString("N");
            var s = await wc.CreateStore(storeName);
            var people = await wc.CreateDataset(storeName, "people", null);

            await wc.UpdateEntities(storeName, "people", GenerateSampleData(1, 0, 3, 5));

            var res = await wc.GetEntity(storeName, "http://data.webofdata.io/things/object1",  new List<string>() { "people" });

            Assert.Equal(1, res.Data.Count);
            Assert.NotNull(res.Context);
            Assert.Null(res.NextDataToken);

            var nsId = res.Data[0].GetId();
            Assert.Equal("ns2:object1", nsId);

            var fullId = res.Data[0].GetFullId(res.Context);            
            Assert.Equal("http://data.webofdata.io/things/object1", fullId);
        }

        [Fact]
        public async void ReadEntities() {
            
            var wc = new WoDClient("http://localhost", 8888);
            var storeName = Guid.NewGuid().ToString("N");
            var storeEntity = new JObject();
            storeEntity["@id"] = "http://data.example.com/stores/" + Guid.NewGuid().ToString("N");
            var s = await wc.CreateStore(storeName, storeEntity);
            var people = await wc.CreateDataset(storeName, "people", null);
            await wc.UpdateEntities(storeName, "people", GenerateSampleData(5, 0, 3, 1));

            var result = await wc.GetEntities(storeName, "people");
            Assert.Equal(5, result.Data.Count());            
        }

        [Fact]
        public async void ReadPagedEntities() {            
            var wc = new WoDClient("http://localhost", 8888);
            var storeName = Guid.NewGuid().ToString("N");
            var storeEntity = new JObject();
            storeEntity["@id"] = "http://data.example.com/stores/" + Guid.NewGuid().ToString("N");
            var s = await wc.CreateStore(storeName, storeEntity);
            var people = await wc.CreateDataset(storeName, "people", null);
            await wc.UpdateEntities(storeName, "people", GenerateSampleData(10, 0, 3, 1));

            var result = await wc.GetEntities(storeName, "people", null, 5);
            Assert.Equal(5, result.Data.Count());            

            var token = result.NextDataToken;
            Assert.NotNull(token);

            result = await wc.GetEntities(storeName, "people", token, 5);
            Assert.Equal(5, result.Data.Count());            

            token = result.NextDataToken;
            Assert.NotNull(token);

            result = await wc.GetEntities(storeName, "people", token, 5);
            Assert.Equal(0, result.Data.Count());            
        }

        [Fact]
        public async void WriteAndReadEntities100k() {
            
            var wc = new WoDClient("http://localhost", 8888);
            var storeName = Guid.NewGuid().ToString("N");
            var storeEntity = new JObject();
            storeEntity["@id"] = "http://data.example.com/stores/" + Guid.NewGuid().ToString("N");
            var s = await wc.CreateStore(storeName, storeEntity);
            var people = await wc.CreateDataset(storeName, "people", null);
            await wc.UpdateEntities(storeName, "people", GenerateSampleData(100000, 0, 3, 1));

            var result = await wc.GetEntities(storeName, "people");
            Assert.Equal(100000, result.Data.Count());            
        }

        [Fact]
        public async void ReadInParallel() {
            var wc = new WoDClient("http://localhost", 8888);
            var storeName = Guid.NewGuid().ToString("N");
            var storeEntity = new JObject();
            storeEntity["@id"] = "http://data.example.com/stores/" + Guid.NewGuid().ToString("N");
            var s = await wc.CreateStore(storeName, storeEntity);
            var people = await wc.CreateDataset(storeName, "people", null);
            await wc.UpdateEntities(storeName, "people", GenerateSampleData(20, 0, 3, 1));

            var readTokens = await wc.GetEntitiesPartitions(storeName, "people", 4);
            Assert.Equal(4, readTokens.Count());           

            var totalRead = 0;
            var res = await wc.GetEntities(storeName, "people", readTokens[0]);
            totalRead += res.Data.Count;

            res = await wc.GetEntities(storeName, "people", readTokens[1]);
            totalRead += res.Data.Count;

            res = await wc.GetEntities(storeName, "people", readTokens[2]);
            totalRead += res.Data.Count;

            res = await wc.GetEntities(storeName, "people", readTokens[3]);
            totalRead += res.Data.Count;

            Assert.Equal(20, totalRead);                
        }

        [Fact]
        public async void ReadChangesInPages() {
            var wc = new WoDClient("http://localhost", 8888);
            var storeName = Guid.NewGuid().ToString("N");
            var s = await wc.CreateStore(storeName);
            var people = await wc.CreateDataset(storeName, "people", null);
            var places = await wc.CreateDataset(storeName, "places", null);
            await wc.UpdateEntities(storeName, "people", GenerateSampleData(10, 0, 3, 1));
            await wc.UpdateEntities(storeName, "places", GenerateSampleData(10, 0, 3, 1));

            var changes = await wc.GetChanges(storeName, "people", null, 5);
            Assert.NotNull(changes.NextDataToken);
            Assert.Equal(5, changes.Data.Count());

            changes = await wc.GetChanges(storeName, "people", changes.NextDataToken, 3);
            Assert.NotNull(changes.NextDataToken);
            Assert.Equal(3, changes.Data.Count());

            changes = await wc.GetChanges(storeName, "people", changes.NextDataToken, 5);
            Assert.NotNull(changes.NextDataToken);
            Assert.Equal(2, changes.Data.Count());

            var ctok = changes.NextDataToken;

            changes = await wc.GetChanges(storeName, "people", changes.NextDataToken, 5);
            Assert.NotNull(changes.NextDataToken);
            Assert.Equal(0, changes.Data.Count());

            Assert.Equal(ctok, changes.NextDataToken);
        }

        [Fact]
        public async void ReadAllChanges() {
            var wc = new WoDClient("http://localhost", 8888);
            var storeName = Guid.NewGuid().ToString("N");
            var s = await wc.CreateStore(storeName);
            var people = await wc.CreateDataset(storeName, "people", null);
            var places = await wc.CreateDataset(storeName, "places", null);
            await wc.UpdateEntities(storeName, "people", GenerateSampleData(10, 0, 3, 1));
            await wc.UpdateEntities(storeName, "places", GenerateSampleData(10, 0, 3, 1));

            var changes = await wc.GetChanges(storeName, "people", null, -1);
            Assert.NotNull(changes.NextDataToken);
            Assert.Equal(10, changes.Data.Count());

            var ctok = changes.NextDataToken;

            changes = await wc.GetChanges(storeName, "people", changes.NextDataToken, 5);
            Assert.NotNull(changes.NextDataToken);
            Assert.Equal(0, changes.Data.Count());

            Assert.Equal(ctok, changes.NextDataToken);
        }

        [Fact]
        public async void ReadChangesInParallel() {
            var wc = new WoDClient("http://localhost", 8888);
            var storeName = Guid.NewGuid().ToString("N");
            var s = await wc.CreateStore(storeName);
            var people = await wc.CreateDataset(storeName, "people", null);
            var places = await wc.CreateDataset(storeName, "places", null);
            await wc.UpdateEntities(storeName, "people", GenerateSampleData(10, 0, 3, 1));
            await wc.UpdateEntities(storeName, "places", GenerateSampleData(10, 0, 3, 1));

            var changeShardTokens = await wc.GetChangesPartitions(storeName, "people", 4);

            var totalRead = 0;
            var res = await wc.GetChanges(storeName, "people", changeShardTokens[0]);
            totalRead += res.Data.Count;

            res = await wc.GetChanges(storeName, "people", changeShardTokens[1]);
            totalRead += res.Data.Count;

            res = await wc.GetChanges(storeName, "people", changeShardTokens[2]);
            totalRead += res.Data.Count;

            res = await wc.GetChanges(storeName, "people", changeShardTokens[3]);
            totalRead += res.Data.Count;

            Assert.Equal(10, totalRead);                

            // make some more changes and read again...
            await wc.UpdateEntities(storeName, "people", GenerateSampleData(10, 10, 3, 1));

            changeShardTokens = await wc.GetChangesPartitions(storeName, "people", 4);

            totalRead = 0;
            res = await wc.GetChanges(storeName, "people", changeShardTokens[0]);
            totalRead += res.Data.Count;

            res = await wc.GetChanges(storeName, "people", changeShardTokens[1]);
            totalRead += res.Data.Count;

            res = await wc.GetChanges(storeName, "people", changeShardTokens[2]);
            totalRead += res.Data.Count;

            res = await wc.GetChanges(storeName, "people", changeShardTokens[3]);
            totalRead += res.Data.Count;

            Assert.Equal(20, totalRead);               

            // make some more changes and read again...
            await wc.UpdateEntities(storeName, "people", GenerateSampleData(10, 10, 4, 1));

            changeShardTokens = await wc.GetChangesPartitions(storeName, "people", 4);

            totalRead = 0;
            res = await wc.GetChanges(storeName, "people", changeShardTokens[0]);
            totalRead += res.Data.Count;

            res = await wc.GetChanges(storeName, "people", changeShardTokens[1]);
            totalRead += res.Data.Count;

            res = await wc.GetChanges(storeName, "people", changeShardTokens[2]);
            totalRead += res.Data.Count;

            res = await wc.GetChanges(storeName, "people", changeShardTokens[3]);
            totalRead += res.Data.Count;

            Assert.Equal(30, totalRead);                

        }

        [Fact]
        public async void UpdateEntities100k()
        {
            var wc = new WoDClient("http://localhost", 8888);
            var storeName = Guid.NewGuid().ToString("N");
            var storeEntity = new JObject();
            storeEntity["@id"] = "http://data.example.com/stores/" + Guid.NewGuid().ToString("N");
            var s = await wc.CreateStore(storeName, storeEntity);
            var people = await wc.CreateDataset(storeName, "people", null);
            await wc.UpdateEntities(storeName, "people", "/tmp/data/sample100k.json");
        }

        [Fact]
        public async void UpdateEntities1M()
        {
            var start = DateTime.UtcNow;
            var wc = new WoDClient("http://localhost", 8888);
            var storeName = Guid.NewGuid().ToString("N");
            var storeEntity = new JObject();
            storeEntity["@id"] = "http://data.example.com/stores/" + Guid.NewGuid().ToString("N");
            var s = await wc.CreateStore(storeName, storeEntity);
            var people = await wc.CreateDataset(storeName, "people", null);
            await wc.UpdateEntities(storeName, "people", "/tmp/data/sample1M.json");
            var end = DateTime.UtcNow;
            var duration = end - start;
        }

        [Fact]
        public async void UpdateEntitiesInParallel1M()
        {
            var start = DateTime.UtcNow;
            var wc = new WoDClient("http://localhost", 8888);
            var storeName = Guid.NewGuid().ToString("N");
            var storeEntity = new JObject();
            storeEntity["@id"] = "http://data.example.com/stores/" + Guid.NewGuid().ToString("N");
            var s = await wc.CreateStore(storeName, storeEntity);
            var people = await wc.CreateDataset(storeName, "people", null);

            var tasks = new List<Task>();

            for (int i=0; i < 10;i++) {
                var t = new Task(() => { wc.UpdateEntities(storeName, "people", "/tmp/data/sample100k.json").Wait(); });
                tasks.Add(t);
                t.Start();
            }

            Task.WaitAll(tasks.ToArray());
            var end = DateTime.UtcNow;
            var duration = end - start;
        }
    }
}
