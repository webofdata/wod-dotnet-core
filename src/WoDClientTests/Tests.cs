using System;
using System.Linq;
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
            var service = wc.GetService();
            Assert.NotNull(service.Entity);
            Assert.NotNull(service.Name);
        }

        [Fact]
        public async void StoreTests() {
            var wc = new WoDClient("http://localhost", 8888);
            var storeName = Guid.NewGuid().ToString("N");
            var storeEntity = new JObject();
            storeEntity["@id"] = "http://data.example.com/stores/" + Guid.NewGuid().ToString("N");
            await wc.CreateStore(storeName, storeEntity);

            // get list of stores
            var stores = await wc.GetStores();
            var exists = stores.Where(x => x.Name == storeName).FirstOrDefault();
            Assert.NotNull(exists);

            // update store entity
            // wc.UpdateStore(storeName, storeEntity);

            var s = wc.GetStore(storeName);

            // delete store
            // wc.DeleteStore(storeName);
        }



    }
}
