using System;
using Microsoft.Extensions.Configuration;
using System.Text.Json;
using Azure.Identity;
using System.Threading.Tasks;
using System.Linq;

namespace DatabaseDDL{ 
    public class SchemaTransformationOperations{
        public async static Task<System.Text.Json.JsonDocument> ExtractScanInfoFromPurview (IConfiguration configuration, string purviewEntityType){
                string tenantId=configuration.GetSection("PurviewSettings:TenantId").Value;
                string applicationId=configuration.GetSection("PurviewSettings:ApplicationId").Value;
                string authenticationKey=configuration.GetSection("PurviewSettings:AuthenticationKey").Value;
                string purviewUri=configuration.GetSection("PurviewSettings:PurviewUri").Value;
                string purviewSchemaQualifiedName=configuration.GetSection("PurviewSettings:PurviewSchemaQualifiedName").Value;
                string purviewFilePathQualifiedName=configuration.GetSection("PurviewSettings:PurviewFilePathQualifiedName").Value;
                string purviewSQLTableQualifiedName=configuration.GetSection("PurviewSettings:PurviewSQLTableQualifiedName").Value;
                                
                ClientSecretCredential cred = new ClientSecretCredential (tenantId, applicationId, authenticationKey);
                string purviewQualifiedName = purviewSchemaQualifiedName;
                switch (purviewEntityType)
                {
                    case "tabular_schema":
                        purviewQualifiedName = purviewSchemaQualifiedName;
                        break;
                    case "azure_datalake_gen2_path":
                        purviewQualifiedName = purviewFilePathQualifiedName;
                        break;
                    case "azure_sql_table":
                        purviewQualifiedName = purviewSQLTableQualifiedName;
                        break;
                }
                JsonDocument purviewJsonResponse = await DatabaseDDL.PurviewOperations.GetPurviewJsonResponse(cred,purviewUri, purviewEntityType, purviewQualifiedName);
                return (purviewJsonResponse);
        }

        public async static Task<string> ExtractTabularSchemaFromPurview (IConfiguration configuration){
                JsonDocument purviewJsonResponseSchema = await ExtractScanInfoFromPurview(configuration, "tabular_schema");
                string intermediateJsonResult = DatabaseDDL.JsonOperations.ExtractTabularSchemaFromJson(purviewJsonResponseSchema);

                JsonDocument purviewJsonResponseEntity = await ExtractScanInfoFromPurview(configuration, "azure_datalake_gen2_path");
                string pathFromPurview = purviewJsonResponseEntity.RootElement.GetProperty("entity").GetProperty("attributes").GetProperty("path").GetString();
                string path = DatabaseDDL.UtilsOperations.ConvertHttpToAbfssLink(pathFromPurview);

                // we need to remove the container from the path else it won't work on synapse
                path = path.Split("/",3).Last();
                string name = purviewJsonResponseEntity.RootElement.GetProperty("entity").GetProperty("attributes").GetProperty("name").GetString();
                
                int initialPosition = name.IndexOf(".");
                string format = name.Substring(initialPosition + 1);
                string source_name = name.Substring(0,initialPosition);

                // Note Data Product Name and Domain Name are currently not used - this is reserved to future improvements
                string finalJsonResult = "{" + $"\"data_product_name\" : \"your_sample_data_product\",\"domain_name\" : \"your_sample_data_domain\", \"data_source\" : \"{source_name}\", \"location\": \"{path}\",\"file_format\": \"{format}\", {intermediateJsonResult}" + "}";
                Console.WriteLine(finalJsonResult);

                return (finalJsonResult);
        }

         public async static Task<string> ExtractSQLSchemaFromPurview (IConfiguration configuration){
                JsonDocument purviewJsonResponseSchema = await ExtractScanInfoFromPurview(configuration, "azure_sql_table");

                string intermediateJsonResult = DatabaseDDL.JsonOperations.ExtractSQLTableSchemaFromJson(purviewJsonResponseSchema);
                string finalJsonResult = "{" + $"\"data_product_name\" : \"your_sample_data_product\",\"domain_name\" : \"your_sample_data_domain\", \"data_source\" : \"ElasticQueryUserDataSrc\", \"data_source_kind\" : \"SQLDB\", {intermediateJsonResult}" + "}";
                Console.WriteLine(finalJsonResult);
                return (finalJsonResult);
         }

    }

}