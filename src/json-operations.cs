using System;
using System.Text.Json;
namespace DatabaseDDL{ 

    public class JsonOperations{
        public static string ExtractTabularSchemaFromJson (JsonDocument responseDocument)
        {
            string resultJson = "";

            foreach (JsonProperty property in responseDocument.RootElement.GetProperty("referredEntities").EnumerateObject())
            {
                string guid = property.Name;
                JsonElement valueElement = property.Value;
                string columnName = valueElement.GetProperty("attributes").GetProperty("name").GetString();
                string columnType = valueElement.GetProperty("attributes").GetProperty("type").GetString();

                string convertedType = DatabaseDDL.UtilsOperations.ConvertTabularType(columnType);
                if(resultJson == ""){
                    resultJson += "{" + $"\"column_name\":\"{columnName}\",\"column_type\": \"{convertedType}\"" + "}";
                }
                else{
                    resultJson += ",{" + $"\"column_name\":\"{columnName}\",\"column_type\": \"{convertedType}\"" + "}";
                }
            }
            string intermediateResultJson = $"\"data_product_schema\" : [{resultJson}]";
            return (intermediateResultJson);
        }

        public static string ExtractSQLTableSchemaFromJson (JsonDocument responseDocument)
        {
            string resultJson = "";

            foreach (JsonProperty property in responseDocument.RootElement.GetProperty("referredEntities").EnumerateObject())
            {
                string guid = property.Name;
                JsonElement valueElement = property.Value;
                string columnName = valueElement.GetProperty("attributes").GetProperty("name").GetString();
                string columnType = valueElement.GetProperty("attributes").GetProperty("data_type").GetString();
                var columnLength = valueElement.GetProperty("attributes").GetProperty("length");
                var columnPrecision = valueElement.GetProperty("attributes").GetProperty("precision");
                var columnScale = valueElement.GetProperty("attributes").GetProperty("scale");

                string convertedType = DatabaseDDL.UtilsOperations.ConvertSQLType(columnType,columnLength.ToString(), columnPrecision.ToString(), columnScale.ToString());
                //In case of xml, drop the column as external tables cant handle xml
                if(convertedType != "xml"){
                    if(resultJson == ""){
                        resultJson += "{" + $"\"column_name\":\"{columnName}\",\"column_type\": \"{convertedType}\"" + "}";
                    }
                    else{
                            resultJson += ",{" + $"\"column_name\":\"{columnName}\",\"column_type\": \"{convertedType}\"" + "}";
                    }
                } 
            }
            string intermediateResultJson = $"\"data_product_schema\" : [{resultJson}]";
            return (intermediateResultJson);
        }
    }   
}