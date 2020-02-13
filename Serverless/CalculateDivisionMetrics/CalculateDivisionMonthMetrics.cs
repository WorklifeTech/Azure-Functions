using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Azure.WebJobs;
using Newtonsoft.Json;
using StackExchange.Redis;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;

namespace CalculateDivisionMetrics
{
    public static class CalculateDivisionMonthMetrics
    {
        [FunctionName("CalculateDivisionMetrics")]
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
            
            var divisionMetricsList = new List<DivisionMetricsModel>();
            using (SqlConnection sqlConnection = new SqlConnection(sqlConnectionString))
            {
                sqlConnection.Open();
                Console.WriteLine(sqlConnection.State);
                SqlCommand cmd = new SqlCommand(); 
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.CommandText = "GetDivisionDimensionMetrics";
                cmd.Connection = sqlConnection;
                var x  = await cmd.ExecuteReaderAsync();
                while (await x.ReadAsync())
                {

                    divisionMetricsList.Add(new DivisionMetricsModel
                    {
                        CompanyId = (int)x["CompanyId"],
                        MonthStart = (DateTime)x["MonthStart"],
                        DivisionName = (string)x["DivisionName"],
                        DepartmentId = (int)x["DepartmentId"],
                        DepartmentName = (string)x["DepartmentName"],
                        NoOfEmployees = (int)x["NoOfEmployees"],
                        ParticipationPctCurrentMonth = (x["ParticipationPctCurrentMonth"] == DBNull.Value) ? null : (decimal?)x["ParticipationPctCurrentMonth"],
                        AvgScorePerAnswer = (x["AvgScorePerAnswer"] == DBNull.Value) ? null : (decimal?)x["AvgScorePerAnswer"],
                        Answer1AverageScore = (x["Answer1AverageScore"] == DBNull.Value) ? null : (decimal?)x["Answer1AverageScore"],
                        Answer2AverageScore = (x["Answer2AverageScore"] == DBNull.Value) ? null : (decimal?)x["Answer2AverageScore"],
                        Answer3AverageScore = (x["Answer3AverageScore"] == DBNull.Value) ? null : (decimal?)x["Answer3AverageScore"],
                        Answer4AverageScore = (x["Answer4AverageScore"] == DBNull.Value) ? null : (decimal?)x["Answer4AverageScore"],
                        Answer5AverageScore = (x["Answer5AverageScore"] == DBNull.Value) ? null : (decimal?)x["Answer5AverageScore"],
                    });
                }
                sqlConnection.Close();
            }
            log.LogInformation($"Number of Records from DB: {divisionMetricsList.Count}");
            var redisSaveResult = await cache.StringSetAsync("divisionMetricsList", JsonConvert.SerializeObject(divisionMetricsList));
            log.LogInformation($"Azure Redis SetString Result: {redisSaveResult}");
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
        }
    }

    public class DivisionMetricsModel
    {
        public int CompanyId { get; set; }

        public string DivisionName { get; set; }

        public int DepartmentId { get; set; }

        public string DepartmentName { get; set; }

        public DateTime MonthStart { get; set; }

        public int NoOfEmployees { get; set; }

        public decimal? ParticipationPctCurrentMonth { get; set; }

        public decimal? AvgScorePerAnswer { get; set; }

        public decimal? Answer1AverageScore { get; set; }

        public decimal? Answer2AverageScore { get; set; }

        public decimal? Answer3AverageScore { get; set; }

        public decimal? Answer4AverageScore { get; set; }

        public decimal? Answer5AverageScore { get; set; }


    }
}

