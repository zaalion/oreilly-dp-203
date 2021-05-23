using Microsoft.Azure.Cosmos.Table;
using System;
using System.Collections.Generic;
using System.Text;

namespace CosmosTable
{
    public class Person : TableEntity
    {
        public Person()
        {
        }

        public Person(string firstName, string lastName)
        {
            PartitionKey = lastName;
            RowKey = firstName;
        }

        public string Email { get; set; }

        public string PhoneNumber { get; set; }
    }
}
