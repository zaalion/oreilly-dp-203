using System;
using Microsoft.Azure.EventHubs;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ConsoleApp18
{
    class Program
    {
        private static EventHubClient eventHubClient;

        private const string EventHubConnectionString =
            "Endpoint=sb://eventhubnsor203demo.servicebus.windows.net/;" +
            "SharedAccessKeyName=RootManageSharedAccessKey;" +
            "SharedAccessKey=q6LPclhZJ3QMosB/Ctu+hPPNuYXPJExZ19lguAzNWzI=";
        private const string EventHubName = "hubsensor01";

        // list of sent messages
        private static List<TempratureInfo> messages = new List<TempratureInfo>();

        static void Main(string[] args)
        {
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(EventHubConnectionString)
            {
                EntityPath = EventHubName
            };

            eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            SendMessagesToEventHub(500).Wait();

            eventHubClient.Close();

            //File.WriteAllText("SEN01.json", JsonConvert.SerializeObject(messages));

            Console.WriteLine("Press any key to continue ...");
            Console.ReadKey();
        }

        // Sends {n} messages to event hub.
        private static async Task SendMessagesToEventHub(int numMessagesToSend)
        {
            for (var i = 0; i < numMessagesToSend; i++)
            {
                try
                {
                    var tEvent = new TempratureInfo()
                    {
                        Id = Guid.NewGuid(),
                        TempratureCelcius = GetRandomNumber(50, 500),
                        SensorId = "SEN-001E"
                    };

                    string message = JsonConvert.SerializeObject(tEvent);

                    messages.Add(tEvent);

                    Console.WriteLine($"Sending message: {message}");

                    await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
                }

                await Task.Delay(500);
            }

            Console.WriteLine($"{numMessagesToSend} messages sent.");
        }

        public static double GetRandomNumber(double minimum, double maximum)
        {
            Random random = new Random();
            return random.NextDouble() * (maximum - minimum) + minimum;
        }
    }
}
