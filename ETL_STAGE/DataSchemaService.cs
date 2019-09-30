using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace ETL_STAGE
{
    class DataSchemaService
    {
        public static DataTable getDataTableSchemaOnly(string dataConnectionSting, string dataQuery, CommandBehavior option)
        {
            DataTable dt = new DataTable();
            using (SqlConnection con = new SqlConnection(dataConnectionSting))
            {
                using (SqlCommand data = new SqlCommand(dataQuery, con))
                {
                    try
                    {
                        con.Open();
                        dt.Load(data.ExecuteReader(option)); //CommandBehavior.SchemaOnly
                    }
                    catch (SqlException sqlEx)
                    {
                        Logger.WriteErrorLog("getDataTable:" + sqlEx.Message);
                        Logger.WriteErrorLog($"dataConnectionSting:[{dataConnectionSting}]  dataQuery:[{dataQuery}]");
                    }
                    finally
                    {
                        if (con.State == ConnectionState.Open) con.Close();
                    }
                }
            }
            return dt;
        }
        static public DataRowCollection getSchemaRowsCollection(string connectionString, string query)
        {
            DataRowCollection SchemaRows = null;
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                using (SqlCommand Source = new SqlCommand(query, con))
                {
                    try
                    {
                        con.Open();
                        using (var dr = Source.ExecuteReader(CommandBehavior.SchemaOnly | CommandBehavior.CloseConnection))
                        {
                            SchemaRows = dr.GetSchemaTable().Rows;
                            dr.Close();
                        }
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
            return SchemaRows;
        }
        static public bool DataRowsIsEqual(DataRowCollection Source, DataRowCollection Target)
        {
            if (Source == null || Target == null)
            {
                Logger.WriteErrorLog("Source or Target is null");
                return false;
            }
            else
            {
                List<string> TargetColumns = splSqlService.GetColumnsDefinitions(Target);
                List<string> SourceColumns = splSqlService.GetColumnsDefinitions(Source);
                foreach (string SourceColumnDefinition in SourceColumns)
                {
                    if (!TargetColumns.Contains(SourceColumnDefinition)) return false;
                }
            }
            return true;
        }
    }
}
