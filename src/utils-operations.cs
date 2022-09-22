using System;
namespace DatabaseDDL{ 

    public class UtilsOperations{
        public static string ConvertTabularType(string strTypeToConvert)
        {
                string strConvertedType="";
                switch (strTypeToConvert)
                {
                    case "INT_16":
                        strConvertedType = "smallint";
                        break;

                    case "DATE":
                        strConvertedType = "date";
                        break;

                    case "INT96":
                        strConvertedType = "datetime2(7)";
                        break;
                    
                    case "INT32":
                        strConvertedType = "int";
                        break;
                    
                    case "UTF8":
                        strConvertedType = "varchar(32)";
                        break;
                    default:
                        Console.WriteLine($"Non existing type {strTypeToConvert}.");
                        break;
                }
                return (strConvertedType);
        }

        public static string ConvertSQLType(string strTypeToConvert, string strLength, string strPrecision, string strScale)
        {
            string strConvertedType="";
            switch (strTypeToConvert)
            {
                case "int":
                    strConvertedType = "int";
                    break;
                case "decimal":
                    //https://docs.microsoft.com/en-us/sql/t-sql/data-types/decimal-and-numeric-transact-sql?view=sql-server-ver15
                    strConvertedType = $"decimal({strPrecision},{strScale})";
                    break;
                case "datetime":
                    strConvertedType = "datetime";
                    break;

                case "nvarchar":
                    switch(strLength)
                    {
                        case "-1":
                            strConvertedType = $"nvarchar(max)";
                            break;
                        default:
                            strConvertedType = $"nvarchar({Convert.ToInt32(strLength)/2})"; // 2 =#bytes per character for SQL
                            break;
                    }
                    break;
                case "varchar":
                    switch(strLength)
                    {
                        case "-1":
                            strConvertedType = $"varchar(max)";
                            break;
                        default:
                            strConvertedType = $"varchar({strLength})";
                            break;
                    }
                    break;
                default:
                    // This should apply to:
                    // int, tinyint, smallint, bigint
                    // float real
                    // uniqueidentifier, bit
                    // datetime,
                    // money, smallmoney
                    strConvertedType = strTypeToConvert;
                    break;
            }
            return (strConvertedType);
        }   

        public static string ConvertHttpToAbfssLink(string strSourceLink)
        {
            string abfssLink;
            if (strSourceLink.Contains("http://")  || strSourceLink.Contains("https://"))
            {
                string[] parts = strSourceLink.Split('/');
                abfssLink = "abfss://" + parts[3] + "@" + parts[2];
            }
            else{
                abfssLink = strSourceLink;
            }
            
            return (abfssLink);
        }
    }
} 