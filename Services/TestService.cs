using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using DBInsertSpeedTests.Models;
using FastMember;

namespace DBInsertSpeedTests.Services
{
    public class TestService
    {
        public static void RunTests()
        {
            var stopwatch = new Stopwatch();

            const int BATCHES = 200;
            const int BATCH_SIZE = 1000;

            // Generate data
            var person = new Person { Id = 1, FirstName = "Test", LastName = "Dude", Age = 30, Gender = Gender.Male };
            var people = new List<Person>();

            for (var i = 0; i < BATCH_SIZE; i++){
                people.Add(person);
            }

            var sqlConnection = new SqlConnection("connectionString");

            #region SqlSerializer Test

            stopwatch.Start();
            sqlConnection.Open();

            var sqlSerializer = new SqlSerializer<Person>();

            for (var i = 0; i < BATCHES; i++){
                var query = sqlSerializer.SerializeInsert(people);
                var command = new SqlCommand(query) { Connection = sqlConnection };
                command.ExecuteNonQuery();
            }

            sqlConnection.Close();
            stopwatch.Stop();

            var test1Elapsed = stopwatch.Elapsed;

            #endregion

            #region SqlBulkCopy and FastMember

            stopwatch.Restart();
            sqlConnection.Open();

            for (var i = 0; i < BATCHES; i++)
            {
                using (var bcp = new SqlBulkCopy(sqlConnection))
                using (var reader = ObjectReader.Create(people))
                {
                    bcp.DestinationTableName = typeof(Person).Name;
                    bcp.WriteToServer(reader);
                }
            }

            sqlConnection.Close();
            stopwatch.Stop();
            var test2Elapsed = stopwatch.Elapsed;

            #endregion

            Console.WriteLine($"Speed of SqlSerializer: {test1Elapsed}");
            Console.WriteLine($"Speed of SqlBulkCopy: {test2Elapsed}");
        }
    }
}
