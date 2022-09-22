using System.Collections.Generic;


namespace DatabaseDDL.Model
{
    public class Column {
            public string column_name {get;set;}

            public string column_type {get;set;}
    }
    public class DDLJson
    {
        public string data_product_name {get;set;}
        public string domain_name {get;set;}
        public string data_source {get;set;}
        public string location {get;set;}
        public string file_format {get;set;}
        public string data_source_kind {get;set;}
        public bool is_shard_map_source {get;set;}
        public string shard_column {get;set;}
        public List<Column> data_product_schema {get;set;}
    }
}