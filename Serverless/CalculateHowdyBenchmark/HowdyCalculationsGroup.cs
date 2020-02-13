using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Azure.WebJobs;
using Newtonsoft.Json;
using StackExchange.Redis;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;

namespace CalculateHowdyBenchmark
{
    public static class HowdyCalculationsGroup
    {
        [FunctionName("CalculateHowdyBenchmark")]
        public static async Task Run([TimerTrigger("0 0 4 * * *")]TimerInfo myTimer, ILogger log, ExecutionContext context)
        {
            var config = new ConfigurationBuilder()
                                .SetBasePath(context.FunctionAppDirectory)
                                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                                .AddEnvironmentVariables()
                                .Build();
            var sqlConnectionString = config.GetConnectionString("az_sql_prod_db_connectionString");
            var redisConnectionString = config.GetConnectionString("RedisConnection");

            IDatabase cache = (await ConnectionMultiplexer.ConnectAsync(redisConnectionString)).GetDatabase();
            
            //var ress= await cache.StringGetAsync("employeeList");
            //var y =  JsonConvert.DeserializeObject<IList<CompanyActiveEmployeeModel>>(ress);


            var employeesTimeLineList = new List<CompanyActiveEmployeeModel>();
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                Console.WriteLine(sqlConnection.State);
                SqlCommand cmd = new SqlCommand(); 
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.CommandText = "Howdy_Employees_Unified_TimeLine";
                cmd.Connection = sqlConnection;
                var x  = await cmd.ExecuteReaderAsync();
                while (await x.ReadAsync())
                {
                    
                    employeesTimeLineList.Add(new CompanyActiveEmployeeModel
                    {
                        ID = (int)x["ID"],
                        CompanyID = (int) x["CompanyID"],
                        EffectiveFrom = (DateTime)x["EffectiveFrom"],
                        EffectiveTo = (DateTime)x["EffectiveTo"]
                    });
                }
                sqlConnection.Close();
            }
            log.LogInformation($"Number of Records from DB: {employeesTimeLineList.Count}");
            var redisSaveResult = await cache.StringSetAsync("employeeList", JsonConvert.SerializeObject(employeesTimeLineList));
            log.LogInformation($"Azure Redis SetString Result: {redisSaveResult}");
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
        }

       

    }

    public class CompanyActiveEmployeeModel
    {
        public int ID { get; set; }
        public int CompanyID { get; set; }
        public DateTime EffectiveFrom { get; set; }
        public DateTime EffectiveTo { get; set; }
    }


}

