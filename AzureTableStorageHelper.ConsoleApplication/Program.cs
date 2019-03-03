using AzureTableStorageHelper.Library;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace AzureTableStorageHelper.ConsoleApplication
{
    class Program
    {
        static readonly string connectionString = "<Connection String Here>";
        static readonly string cloudTableName = "housetest";
        static readonly string partitionKey = "partition1";
        static readonly string rowKey = "row1";
        static readonly int houseNumber = 10;
        static readonly int houseNumberUpdated = 3;

        static async Task Main(string[] args)

        {
            try
            {
                await Insert();
                await InsertBatch();
                await Retrieve();
                await RetrieveMany();
                await Update();
                await Delete();
                await DropTable();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error : {ex.Message}");
                await DropTable();
            }

            Console.ReadLine();
        }

        private static async Task Insert()
        {
            var entity = new HouseEntity(partitionKey, rowKey, houseNumber);
            var result = await TableStorageHelper.Insert(connectionString, cloudTableName, entity, true);
            Console.WriteLine($"Inserted: {result.HttpStatusCode}");
        }

        private static async Task InsertBatch()
        {
            var entities = new HouseEntity[]
            {
                new HouseEntity("partition1", "row2", 5),
                new HouseEntity("partition1", "row3", 7),
                new HouseEntity("partition1", "row4", 13),
                new HouseEntity("partition1", "row5", 23),
                new HouseEntity("partition1", "row6", 33),
            };

            await TableStorageHelper.InsertBatch(connectionString, cloudTableName, entities);
        }

        private static async Task Retrieve()
        {
            var result = await TableStorageHelper.RetrieveSingle<HouseEntity>(connectionString, cloudTableName, partitionKey, rowKey);
            Console.WriteLine($"Retrieved Single: HouseNumber {result.HouseNumber}");
        }

        private static async Task RetrieveMany()
        {
            var result = await TableStorageHelper.RetrieveManyByPartition<HouseEntity>(connectionString, cloudTableName, partitionKey);
            Console.WriteLine($"Retrieved Many By Partition: {result.Count()}");
        }

        private static async Task Update()
        {
            var houseEntity = TableStorageHelper.RetrieveSingle<HouseEntity>(connectionString, cloudTableName, partitionKey, rowKey).GetAwaiter().GetResult();
            houseEntity.HouseNumber = houseNumberUpdated;

            var result = await TableStorageHelper.Update<HouseEntity>(connectionString, cloudTableName, houseEntity);
            Console.WriteLine($"Updated: {result.HttpStatusCode}");
        }

        private static async Task Delete()
        {
            var result = await TableStorageHelper.Delete<HouseEntity>(connectionString, cloudTableName, partitionKey, rowKey);
            Console.WriteLine($"Deleted: {result.HttpStatusCode}");
        }

        private static async Task TableExists()
        {
            var tableExists = await TableStorageHelper.TableExists(connectionString, cloudTableName);
            Console.WriteLine($"Table Exists: {tableExists}");
        }

        private static async Task DropTable()
        {
            var result = await TableStorageHelper.DropTable(connectionString, cloudTableName);
            Console.WriteLine($"Table Dropped: {result}");
        }
    }

    public class HouseEntity : TableEntity
    {
        public int HouseNumber { get; set; }

        public HouseEntity() { }

        public HouseEntity(string partitionKey, string rowKey, int houseNumber)
        {
            PartitionKey = partitionKey;
            RowKey = rowKey;
            HouseNumber = houseNumber;
        }
    }
}
