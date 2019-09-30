using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace ETL_STAGE
{
    static class splSqlService
    {
        public static List<string> GetColumnsDefinitions(DataRowCollection rows)
        {
            List<string> Columns = new List<string>();
            foreach (DataRow dr in rows)
            {

                Columns.Add(
                       string.Format("{0} {1}{2} {3} {4}",
                            dr["ColumnName"].ToString(),
                            (dr["DataTypeName"].ToString() == "varchar" ? "nvarchar" : dr["DataTypeName"].ToString()),
                            (HasSize(dr["DataTypeName"].ToString())) ? "(" + ((int)dr["ColumnSize"] > 4000 ? "max" : dr["ColumnSize"].ToString()) + ")" : (HasPrecisionAndScale(dr["DataTypeName"].ToString())) ? "(" + dr["NumericPrecision"].ToString() + "," + dr["NumericScale"].ToString() + ")" : "",
                            (dr["IsIdentity"].ToString() == "true") ? "IDENTITY" : "",
                            (dr["AllowDBNull"].ToString() == "true" || true) ? "NULL" : "NOT NULL")
                    );
            }
            return Columns;
        }
        public static bool ObjectIsExist(string connectionString, string ObjectName)
        {
            string ObjectExitsQueryText = $"select cast( (case when object_id('{ObjectName}') is not null then 1 else 0 end) as bit)";
            bool ObjectNameExits = (bool)splSqlService.GetScalarSql(connectionString, ObjectExitsQueryText);
            return ObjectNameExits;
        }
        public static List<string> GetColumnsNames(DataRowCollection rows)
        {
            List<string> Columns = new List<string>();
            foreach (DataRow dr in rows)
            {
                Columns.Add(dr["ColumnName"].ToString());
            }
            return Columns;
        }
        public static SqlDataReader GetDataReader(string dataConnectionString, string dataQuery)
        {
            SqlConnection con = new SqlConnection(dataConnectionString);
            con.Open();
            SqlCommand sourceData = new SqlCommand(dataQuery, con);
            return sourceData.ExecuteReader(CommandBehavior.CloseConnection);
        }
        private static bool HasSize(string dataType)
        {
            Dictionary<string, bool> dataTypes = new Dictionary<string, bool>();
            dataTypes.Add("bigint", false);
            dataTypes.Add("binary", true);
            dataTypes.Add("bit", false);
            dataTypes.Add("char", true);
            dataTypes.Add("date", false);
            dataTypes.Add("datetime", false);
            dataTypes.Add("datetime2", false);
            dataTypes.Add("datetimeoffset", false);
            dataTypes.Add("decimal", false);
            dataTypes.Add("float", false);
            dataTypes.Add("geography", false);
            dataTypes.Add("geometry", false);
            dataTypes.Add("hierarchyid", false);
            dataTypes.Add("image", true);
            dataTypes.Add("int", false);
            dataTypes.Add("money", false);
            dataTypes.Add("nchar", true);
            dataTypes.Add("ntext", true);
            dataTypes.Add("numeric", false);
            dataTypes.Add("nvarchar", true);
            dataTypes.Add("real", false);
            dataTypes.Add("smalldatetime", false);
            dataTypes.Add("smallint", false);
            dataTypes.Add("smallmoney", false);
            dataTypes.Add("sql_variant", false);
            dataTypes.Add("sysname", false);
            dataTypes.Add("text", true);
            dataTypes.Add("time", false);
            dataTypes.Add("timestamp", false);
            dataTypes.Add("tinyint", false);
            dataTypes.Add("uniqueidentifier", false);
            dataTypes.Add("varbinary", true);
            dataTypes.Add("varchar", true);
            dataTypes.Add("xml", false);
            if (dataTypes.ContainsKey(dataType))
                return dataTypes[dataType];
            return false;
        }

        private static bool HasPrecisionAndScale(string dataType)
        {
            Dictionary<string, bool> dataTypes = new Dictionary<string, bool>();
            dataTypes.Add("bigint", false);
            dataTypes.Add("binary", false);
            dataTypes.Add("bit", false);
            dataTypes.Add("char", false);
            dataTypes.Add("date", false);
            dataTypes.Add("datetime", false);
            dataTypes.Add("datetime2", false);
            dataTypes.Add("datetimeoffset", false);
            dataTypes.Add("decimal", true);
            dataTypes.Add("float", false);
            dataTypes.Add("geography", false);
            dataTypes.Add("geometry", false);
            dataTypes.Add("hierarchyid", false);
            dataTypes.Add("image", false);
            dataTypes.Add("int", false);
            dataTypes.Add("money", false);
            dataTypes.Add("nchar", false);
            dataTypes.Add("ntext", false);
            dataTypes.Add("numeric", false);
            dataTypes.Add("nvarchar", false);
            dataTypes.Add("real", true);
            dataTypes.Add("smalldatetime", false);
            dataTypes.Add("smallint", false);
            dataTypes.Add("smallmoney", false);
            dataTypes.Add("sql_variant", false);
            dataTypes.Add("sysname", false);
            dataTypes.Add("text", false);
            dataTypes.Add("time", false);
            dataTypes.Add("timestamp", false);
            dataTypes.Add("tinyint", false);
            dataTypes.Add("uniqueidentifier", false);
            dataTypes.Add("varbinary", false);
            dataTypes.Add("varchar", false);
            dataTypes.Add("xml", false);
            if (dataTypes.ContainsKey(dataType))
                return dataTypes[dataType];
            return false;
        }
        public static object GetScalarSql(string ConnectionString, string Query)
        {
            object scalar = null;
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                using (SqlCommand Target = new SqlCommand(Query, con))
                {
                    try
                    {
                        con.Open();
                        scalar = Target.ExecuteScalar();
                        con.Close();
                    }
                    catch (SqlException sqlex)
                    {
                        Logger.WriteErrorLog(sqlex.Message);
                    }
                    finally
                    {
                        if (con.State == ConnectionState.Open)
                        {
                            con.Close();
                        }
                    }
                }
            }
            return scalar;
        }
        public static void ExecuteSQL(string ConnectionString, string Query)
        {
            using (SqlConnection con = new SqlConnection(ConnectionString))
            {
                using (SqlCommand Target = new SqlCommand(Query, con))
                {
                    try
                    {
                        con.Open();
                        Target.ExecuteNonQuery();
                        con.Close();
                    }
                    catch (SqlException sqlex)
                    {
                        Logger.WriteErrorLog(sqlex.Message);
                    }
                    finally
                    {
                        if (con.State == ConnectionState.Open)
                        {
                            con.Close();
                        }
                    }
                }
            }
        }
    }
}
