using Microsoft.IdentityModel.Clients.ActiveDirectory;
using System;
using System.Net;
using System.Net.Http;
using System.Data.SqlClient;
using Newtonsoft.Json;
using System.Data;

namespace AX_Data
{
    class Program
    {

        public static string aadTenant = "";  //https://login.windows.net/onCompanyName.onmicrosoft.com;
        public static string aadClientAppId = "";  //AppGUID;
        public static string aadClientAppSecret = ""; //AppSecret;
        public static string aadResource = ""; //https://CompanyName.operations.dynamics.com;
        public static string connString = "Server=CompanyServer;Database=CompanyDatabase;Trusted_Connection=True;";
        public static string[] entities = new string[] { "LoanedEquipments", "LoanItems", "Positions", "Workers", "People" };

        static void Main(string[] args)
        {
            AuthenticationResult token = Authenticate();
            foreach (string entity in entities)
            {
                GetEntity(token, entity);
            }
        }

        static AuthenticationResult Authenticate()
        {
            AuthenticationContext authenticationContext = new AuthenticationContext(aadTenant, false);
            AuthenticationResult authenticationResult;

            var credential = new ClientCredential(aadClientAppId, aadClientAppSecret);

            authenticationResult = authenticationContext.AcquireTokenAsync(aadResource, credential).Result;

            return authenticationResult;
        }

        static void GetEntity(AuthenticationResult token, string entity)
        {
            using (var client = new HttpClient())
            {
                System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
                client.DefaultRequestHeaders.Add("Authorization", "Bearer " + token.AccessToken);
                client.DefaultRequestHeaders.Add("Accept", "application/json;odata.metadata=none");
                string entityURI = aadResource + "/data/" + entity;
                var response = client.GetAsync(entityURI).Result;
                string result = response.Content.ReadAsStringAsync().Result;
                Newtonsoft.Json.Linq.JObject jobject = Newtonsoft.Json.Linq.JObject.Parse(result);
                DataTable dtValue = (DataTable)JsonConvert.DeserializeObject(jobject.Property("value").Value.ToString(), (typeof(DataTable)));

                string tableName = "dbo." + entity;
                BulkCopy(dtValue, tableName);
            }
        }

        static void ExecuteSQL(string sql)
        {
            using (SqlConnection connection = new SqlConnection(connString))
            {
                connection.Open();
                SqlCommand cmd = new SqlCommand(sql, connection);
                cmd.ExecuteScalar();
                connection.Close();
            }
        }

        static void TruncateTable(string tableName)
        {
            string sql = "TRUNCATE TABLE " + tableName;
            ExecuteSQL(sql);
        }

        static void BulkCopy(DataTable dt, String tableName)
        {
            TruncateTable(tableName);
        
            using (SqlConnection connection = new SqlConnection(connString))
            {
                SqlBulkCopy bulkCopy =
                        new SqlBulkCopy
                        (
                        connection,
                        SqlBulkCopyOptions.TableLock |
                        SqlBulkCopyOptions.FireTriggers |
                        SqlBulkCopyOptions.UseInternalTransaction,
                        null
                        );

                bulkCopy.DestinationTableName = tableName;
                connection.Open();

                bulkCopy.WriteToServer(dt);
                connection.Close();
            }
            
            dt.Clear();
        }
    }
}
