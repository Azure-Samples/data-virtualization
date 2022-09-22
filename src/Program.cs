using System;
using System.Configuration;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace DatabaseDDL
{
    class Program
    {
        public static void Main()
        {
            MainAsync().GetAwaiter().GetResult();
        }

         private static async Task MainAsync()
        {
            try 
            { 
                //Configuration extraction from settings file
                var bld = new ConfigurationBuilder()
                            .SetBasePath(System.Environment.CurrentDirectory)
                            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
                var configuration = bld.Build();

                string purviewJsonResult = "";
                //Case 1: Storage to Synapse
                if (!string.IsNullOrEmpty(configuration.GetSection("PurviewSettings:PurviewFilePathQualifiedName").Value))
                {
                    purviewJsonResult = await DatabaseDDL.SchemaTransformationOperations.ExtractTabularSchemaFromPurview(configuration);
                }
                //Case 2: SQL DB to SQL DB
                else if (!string.IsNullOrEmpty(configuration.GetSection("PurviewSettings:PurviewSQLTableQualifiedName").Value))
                {
                    purviewJsonResult  = await DatabaseDDL.SchemaTransformationOperations.ExtractSQLSchemaFromPurview (configuration);
                }
                
                string sqlStatement = DatabaseDDL.DatabaseOperations.CreateSQLStatementFromJsonString(configuration, purviewJsonResult);
                DatabaseDDL.DatabaseOperations.ExecuteSQLStatement(configuration,sqlStatement);        

                

            }
            catch (ConfigurationErrorsException e)
            {
                Console.WriteLine(e.ToString());
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
            Console.WriteLine("\nDone. Press enter.");
            Console.ReadLine();
        }
    }
}
