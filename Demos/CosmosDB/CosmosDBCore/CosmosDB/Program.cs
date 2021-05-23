using Microsoft.Azure.Cosmos;
using System;
using System.Threading.Tasks;

namespace CosmosDB
{
    /// <summary>
    /// See https://docs.microsoft.com/en-us/azure/cosmos-db/sql-api-dotnet-v3sdk-samples
    /// </summary>
    class Program
    {
        private static readonly string databaseId = "samples";
        private static Database _database = null;
        private static Container _container = null;

        static void Main(string[] args)
        {
            try
            {
                string endpoint = "https://cosmos200orcore.documents.azure.com:443/";
                if (string.IsNullOrEmpty(endpoint))
                {
                    throw new 
                        ArgumentNullException("Please specify a valid endpoint");
                }

                string authKey =
                    "goLhH5osRiLqksmgaZAWZBJ7UUJ7eXJEPT7yEhEIWTZmSgSqfIOnRBpvHoHaJaP0PLF1JEt6fPiFhwOgry11Og==";

                if (string.IsNullOrEmpty(authKey) || string.Equals(authKey, "Super secret key"))
                {
                    throw new 
                        ArgumentException("Please specify a valid AuthorizationKey");
                }

                //Read the Cosmos endpointUrl and authorisationKeys from configuration
                //These values are available from the Azure Management Portal on the Cosmos Account Blade under "Keys"
                //NB > Keep these values in a safe & secure location. Together they provide Administrative access to your Cosmos account
                using (CosmosClient client = new CosmosClient(endpoint, authKey))
                {
                     RunDatabaseDemo(client);
                    CreateItem();
                }
            }
            catch (CosmosException cre)
            {
                Console.WriteLine(cre.ToString());
            }
            catch (Exception e)
            {
                Exception baseException = e.GetBaseException();
                Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }

        /// <summary>
        /// See https://docs.microsoft.com/en-us/azure/cosmos-db/sql-api-dotnet-v3sdk-samples
        /// </summary>
        /// <param name="client"></param>
        private static void RunDatabaseDemo(CosmosClient client)
        {
            // An object containing relevant information about the response
            DatabaseResponse databaseResponse = 
                client.CreateDatabaseIfNotExistsAsync(databaseId, 400).Result;

            // A client side reference object that allows additional operations like ReadAsync
            Database database = databaseResponse;

            _database = database; 

            // The response from Azure Cosmos
            DatabaseProperties properties = databaseResponse;

            Console.WriteLine($"\n1. Create a database resource with id: " +
                $"{properties.Id} and last modified time stamp: {properties.LastModified}");
            Console.WriteLine($"\n2. Create a database resource request charge: " +
                $"{databaseResponse.RequestCharge} and Activity Id: {databaseResponse.ActivityId}");

            // Read the database from Azure Cosmos
            DatabaseResponse readResponse = database.ReadAsync().Result;
            Console.WriteLine($"\n3. Read a database: {readResponse.Resource.Id}");

            _container = 
                readResponse.Database.CreateContainerAsync("testContainer", "/city").Result;

            // Get the current throughput for the database
            int? throughputResponse = database.ReadThroughputAsync().Result;
            if (throughputResponse.HasValue)
            {
                Console.WriteLine($"\n4. Read a database throughput: {throughputResponse}");

                // Update the current throughput for the database
                database.ReplaceThroughputAsync(11000).Wait();
            }

            Console.WriteLine("\n5. Reading all databases resources for an account");
            using (FeedIterator<DatabaseProperties> iterator = 
                client.GetDatabaseQueryIterator<DatabaseProperties>())
            {
                while (iterator.HasMoreResults)
                {
                    foreach (DatabaseProperties db in iterator.ReadNextAsync().Result)
                    {
                        Console.WriteLine(db.Id);
                    }
                }
            }

            // Delete the database from Azure Cosmos.
            //database.DeleteAsync().Wait();
            //Console.WriteLine($"\n6. Database {database.Id} deleted.");
        }
        // </RunDatabaseDemo>

        private static Person CreateItem()
        {
            Console.WriteLine("\n1.1 - Creating item");

            // Create a SalesOrder object. This object has nested properties and various types including numbers, DateTimes and strings.
            // This can be saved as JSON as is without converting into rows/columns.
            Person person = new Person()
            {
                Id = Guid.NewGuid().ToString().Replace("-", string.Empty),
                City = "Toronto",
                Name = "Reza",
                Email = "Reza@test.com"
            };
            ItemResponse<Person> response = 
                _container.CreateItemAsync(person, new PartitionKey(person.City)).Result;

            return person;
        }
    }
}