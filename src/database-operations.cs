using System;
using System.Text.Json;
using DatabaseDDL.Model;
using Microsoft.Extensions.Configuration;
using System.Data.SqlClient; 

namespace DatabaseDDL{ 
 
    public class DatabaseOperations{
        public static string CreateSQLStatementFromJsonString (IConfiguration configuration, string strJsonString)
        {
            var ddl = JsonSerializer.Deserialize<DDLJson>(strJsonString)!;

            string columnSection = "";
            foreach (var c in ddl.data_product_schema)
            {
                columnSection += $" [{c.column_name}] {c.column_type},";
            }
            // remove last comma
            columnSection = columnSection.Remove(columnSection.Length-1,1);
            string externalDataSrcName = configuration.GetSection("DestinationSettings:ExternalDataSrcName").Value + "DataSrc";
            string withStatement = $"DATA_SOURCE = [{externalDataSrcName}]";
            string sqlPrepend = "";
            if (ddl.is_shard_map_source)
            {
                withStatement += $", DISTRIBUTION = SHARDED([{ddl.shard_column}])";                    
            }
            if (ddl.data_source_kind =="SQLDB")
            {
                string dbDataSrc = configuration.GetSection("DatabaseSettings:DataSource").Value;
                string url = configuration.GetSection("PurviewSettings:PurviewSQLTableQualifiedName").Value;
                string[] url_parts = url.Split('/');
                string dbSourceCatalog = url_parts[3];

                string cred = configuration.GetSection("DestinationSettings:ExternalDataSrcName").Value + "Cred"; //assuming this cred already exists!
                sqlPrepend = $"IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = '{externalDataSrcName}') CREATE EXTERNAL DATA SOURCE [{externalDataSrcName}] WITH (TYPE = RDBMS, LOCATION = '{dbDataSrc}', DATABASE_NAME = '{dbSourceCatalog}', CREDENTIAL = {cred} );";
            }
            else if (ddl.data_source_kind !="SQLDB") //assuming storage source, note this will only work with synapse!
            {
                
                string url = configuration.GetSection("PurviewSettings:PurviewSchemaQualifiedName").Value;
                url = DatabaseDDL.UtilsOperations.ConvertHttpToAbfssLink(url);

                sqlPrepend = "if not exists(select * from sys.external_file_formats t where t.name='parquet') CREATE EXTERNAL FILE FORMAT [parquet] WITH ( FORMAT_TYPE = PARQUET);" +
                    "if not exists(select * from sys.database_scoped_credentials t where t.name='WorkspaceIdentity') CREATE DATABASE SCOPED CREDENTIAL WorkspaceIdentity WITH IDENTITY = 'Managed Identity';" +
                    $"if not exists(select * from sys.external_data_sources t where t.name='{externalDataSrcName}') CREATE EXTERNAL DATA SOURCE [{externalDataSrcName}] WITH (LOCATION = '{url}', CREDENTIAL = WorkspaceIdentity);";

                withStatement += $", location = '{ddl.location}', file_format = [{ddl.file_format}]";
            }
            string ExternalTableName = configuration.GetSection("DestinationSettings:ExternalTableName").Value;
            string ExternalSchemaName = configuration.GetSection("DestinationSettings:ExternalSchemaName").Value;
            String sql = sqlPrepend + $"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{ExternalSchemaName}' ) EXEC('CREATE SCHEMA [{ExternalSchemaName}]');" +
                $"if not exists(select * from sys.external_tables t inner join sys.schemas s on s.schema_id = t.schema_id where t.name='{ExternalTableName}' and s.name='{ExternalSchemaName}') " +
                $"CREATE EXTERNAL TABLE [{ExternalSchemaName}].[{ExternalTableName}] ("+
                $"{columnSection}) with ({withStatement});";
            
            return (sql);
        }
         
         public static void ExecuteSQLStatement (IConfiguration configuration, string strSqlStatement)
         {   
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();

            builder.DataSource = configuration.GetSection("DatabaseSettings:DataSource").Value; //<your_server.database.windows.net>
            builder.UserID = configuration.GetSection("DatabaseSettings:UserID").Value;            
            builder.Password = configuration.GetSection("DatabaseSettings:Password").Value;     
            builder.InitialCatalog = configuration.GetSection("DatabaseSettings:InitialCatalog").Value;
            builder.ConnectTimeout = 60;
        
            using (SqlConnection connection = new SqlConnection(builder.ConnectionString))
            {
                Console.WriteLine("\nExecuting DDL Statement");
                Console.WriteLine("=========================================\n");
                
                connection.Open();

                SqlCommand cmd = connection.CreateCommand();

                cmd.CommandText = strSqlStatement ; //@"exec sp_set_session_context @key=N'TenantId', @value=@shardingKey";
                //cmd.Parameters.AddWithValue("@shardingKey", shardingKey);
                cmd.ExecuteNonQuery();
                }
         }
    }
}
