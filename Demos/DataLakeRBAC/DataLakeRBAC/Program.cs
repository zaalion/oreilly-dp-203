using System;
using Azure;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Azure.Storage;
using System.IO;
using Azure.Core;
using Azure.Identity;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace DataLakeRBAC
{
    /// <summary>
    /// https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-directory-file-acl-dotnet
    /// https://www.nuget.org/packages/Azure.Storage.Files.DataLake/
    /// </summary>
    class Program
    {
        private static DataLakeServiceClient _dataLakeServiceClient;
        private static DataLakeFileSystemClient _dataLakeFileSystemClient;
                
        private static readonly string _clientTenantId = "0ec02b79-d89f-48c4-9870-da4a7498d887";
        private static readonly string _clientId = "1afcf1e7-7fd8-46d4-9d11-d1307572a68c";
        private static readonly string _clientSecret = "GYeO8~D-UVpvZ0n3Gc-vol3b4UkWoVb.b6";

        private static readonly string _accountName = "datalakedemo01";
        private static readonly string _filesystemName = "filesystem-logs01";
        private static readonly string _directoryName = "my-directory";

        static void Main(string[] args)
        {
            Console.WriteLine("Working with Data Lake Gen2 authentication...");

            // Data Lake authentication
            _dataLakeServiceClient = GetDataLakeServiceClient(_accountName, _clientId,
                _clientSecret, _clientTenantId);

            // Create a container (filesystem)
            //CreateFileSystem(_dataLakeServiceClient).Wait();

            // Create a directory
            _dataLakeFileSystemClient =
                CreateDirectory(_dataLakeServiceClient, _filesystemName);

            // upload a file
            UploadFile(_dataLakeFileSystemClient).Wait();

            // List all uploaded files
            Console.WriteLine("");
            ListFilesInDirectory(_dataLakeFileSystemClient).Wait();

            Console.WriteLine("");
            Console.WriteLine("Press enter to exit...");
            Console.ReadLine();
        }

        public static DataLakeServiceClient GetDataLakeServiceClient(string accountName, string clientID, 
            string clientSecret, string tenantID)
        {

            TokenCredential credential = new ClientSecretCredential(
                tenantID, clientID, clientSecret, new TokenCredentialOptions());

            string dfsUri = "https://" + accountName + ".dfs.core.windows.net";

            return new DataLakeServiceClient(new Uri(dfsUri), credential);
        }

        public static async Task<DataLakeFileSystemClient> CreateFileSystem 
            (DataLakeServiceClient serviceClient)
        {
            return await serviceClient.CreateFileSystemAsync(_filesystemName);
        }

        public static DataLakeFileSystemClient CreateDirectory
            (DataLakeServiceClient serviceClient, string fileSystemName)
        {
            DataLakeFileSystemClient fileSystemClient =
                serviceClient.GetFileSystemClient(fileSystemName);

            DataLakeDirectoryClient directoryClient =
                fileSystemClient.CreateDirectoryAsync(_directoryName).Result;

            // You can create a sub-directory if needed
            //return await directoryClient.CreateSubDirectoryAsync("my-subdirectory");

            return fileSystemClient;
        }

        /// <summary>
        /// Use this method to programatically upload a file to data lake Gen2
        /// </summary>
        /// <param name="fileSystemClient"></param>
        /// <returns></returns>
        public static async Task UploadFile(DataLakeFileSystemClient fileSystemClient)
        {
            DataLakeDirectoryClient directoryClient =
                fileSystemClient.GetDirectoryClient(_directoryName);

            // the file in Data Lake
            DataLakeFileClient fileClient = await directoryClient.CreateFileAsync("uploaded-logfile.txt");

            // Local file to be uploaded
            FileStream fileStream =
                File.OpenRead(@"D:\__temp\my-log.txt");

            long fileSize = fileStream.Length;

            await fileClient.AppendAsync(fileStream, offset: 0);

            await fileClient.FlushAsync(position: fileSize);

        }

        /// <summary>
        /// Use this method to programatically download a file from data lake Gen2
        /// </summary>
        /// <param name="fileSystemClient"></param>
        /// <returns></returns>
        public static async Task DownloadFile(DataLakeFileSystemClient fileSystemClient)
        {
            DataLakeDirectoryClient directoryClient =
                fileSystemClient.GetDirectoryClient("my-directory");

            DataLakeFileClient fileClient =
                directoryClient.GetFileClient("my-log-downloaded.txt");

            Response<FileDownloadInfo> downloadResponse = await fileClient.ReadAsync();

            BinaryReader reader = new BinaryReader(downloadResponse.Value.Content);

            FileStream fileStream =
                File.OpenWrite("D:\\_temp\\my-image-downloaded.png");

            int bufferSize = 4096;

            byte[] buffer = new byte[bufferSize];

            int count;

            while ((count = reader.Read(buffer, 0, buffer.Length)) != 0)
            {
                fileStream.Write(buffer, 0, count);
            }

            await fileStream.FlushAsync();

            fileStream.Close();
        }

        public static async Task ListFilesInDirectory(DataLakeFileSystemClient fileSystemClient)
        {
            IAsyncEnumerator<PathItem> enumerator =
                fileSystemClient.GetPathsAsync("my-directory").GetAsyncEnumerator();

            await enumerator.MoveNextAsync();

            PathItem item = enumerator.Current;

            while (item != null)
            {
                Console.WriteLine(item.Name);

                if (!await enumerator.MoveNextAsync())
                {
                    break;
                }

                item = enumerator.Current;
            }
        }
    }
}