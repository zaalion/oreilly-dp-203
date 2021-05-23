using Microsoft.Azure.Cosmos.Table;
using System;

namespace CosmosTable
{
    /// <summary>
    /// See https://docs.microsoft.com/en-us/azure/cosmos-db/tutorial-develop-table-dotnet
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            var connectionString = 
                "DefaultEndpointsProtocol=https;AccountName=cosmos200ortable;AccountKey=zOUfUIWa2RVM8lnb9fqeX8v7bUbJi3meX2LJDGAOUFyXODlge6Q2f8vWIdhsb1nEc1yOJuMQvdr3kJrMaCmptw==;TableEndpoint=https://cosmos200ortable.table.cosmos.azure.com:443/;";
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            // Create a table client for interacting with the table service 
            CloudTable table = tableClient.GetTableReference("People");
            if (table.CreateIfNotExistsAsync().Result)
            {
                Console.WriteLine("Created Table named: People");
            }
            else
            {
                Console.WriteLine("Table People already exists");
            }

            // insert a new person
            try
            {
                var entity = new Person("Reza", "Salehi")
                {
                    Email = "reza@test.com",
                    PhoneNumber = "555-555-5555"
                };

                // Create the InsertOrReplace table operation
                TableOperation insertOrMergeOperation = TableOperation.InsertOrMerge(entity);

                // Execute the operation.
                TableResult result = table.ExecuteAsync(insertOrMergeOperation).Result;
                Person insertedPerson = result.Result as Person;

                if (result.RequestCharge.HasValue)
                {
                    Console.WriteLine("Request Charge of InsertOrMerge Operation: " + result.RequestCharge);
                }
            }
            catch (StorageException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }
    }
}