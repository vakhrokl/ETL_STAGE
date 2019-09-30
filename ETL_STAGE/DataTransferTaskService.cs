using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Linq;

namespace ETL_STAGE
{
    static class DataTransferTaskService
    {
        static bool EtlConnectionIsValid;
        static string ETLConnectionString;
        
        public static bool TestConnection(string ConnectionString)
        {
            using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(ConnectionString))
            {
                try
                {
                    con.Open(); con.Close();
                    Logger.WriteLog("Connection test...OK");
                    EtlConnectionIsValid = true;
                    ETLConnectionString = ConnectionString;
                    return true;
                }
                catch (System.Data.SqlClient.SqlException sqlex)
                {
                    Logger.WriteErrorLog(sqlex.Message);
                    EtlConnectionIsValid = false;
                    return false;
                }
            }
        }
        public static List<DataTransferTask> GetTasks(string ConnectionString)
        {
            List<DataTransferTask> TransferTasks = new List<DataTransferTask>();
            if (!EtlConnectionIsValid) return TransferTasks; 

            using (System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(ConnectionString))
            {
                using (System.Data.SqlClient.SqlCommand getTaskQuery = new System.Data.SqlClient.SqlCommand("exec dbo.ETL_getTasks 0", con))
                {
                    con.Open();
                    try
                    {
                        using (System.Data.SqlClient.SqlDataReader dr = getTaskQuery.ExecuteReader())
                        {
                            while (dr.Read())
                            {
                                TransferTasks.Add(new DataTransferTask()
                                {
                                    TaskId = (int)dr["TASK_ID"],
                                    SourceTableName = dr["TASK_NAME"].ToString(),
                                    SourceId = (int)dr["SOURCE_ID"],
                                    SourceConnectionString = dr["SOURCE_CONNECTION"].ToString(),
                                    SourceName = dr["SOURCE_NAME"].ToString(),
                                    SourceGetDataQuery = dr["DATA_QUERY"].ToString(),
                                    TargetConnectionString = dr["TARGET_CONNECTION"].ToString(),
                                    TargetName = dr["TARGET_NAME"].ToString(),
                                    TargetTableName = dr["TARGET_TABLE"].ToString()
                                });
                            }
                            dr.Close();
                        }
                    }
                    catch (System.Data.SqlClient.SqlException SQLEX)
                    {
                        Console.WriteLine(SQLEX.Message);
                    }
                    con.Close();
                }
            }
            return TransferTasks;
        }

        internal static bool checkCollation(DataTransferTask t)
        {
            string colate1 = "1";
            string colate2 = "2";
            
            using (SqlConnection con1 = new SqlConnection(t.SourceConnectionString))
            {
                con1.Open();
                using (SqlCommand com = new SqlCommand($"SELECT DATABASEPROPERTYEX('{con1.Database}', 'Collation') AS Collation", con1))
                {
                    colate1 = (string)com.ExecuteScalar();
                }
                con1.Close();
            }
            using (SqlConnection con2 = new SqlConnection(t.TargetConnectionString))
            {
                con2.Open();
                using (SqlCommand com = new SqlCommand($"SELECT DATABASEPROPERTYEX('{con2.Database}', 'Collation') AS Collation", con2))
                {
                    colate2 = (string)com.ExecuteScalar();
                }
                con2.Close();
            }
            
            return colate1 == colate2;
        }

        internal static void ReleaseTask(int taskId)
        {
            if (EtlConnectionIsValid)
            {
                string query = $"exec dbo.etl_setTaskStatus {taskId}, 0;";
                splSqlService.ExecuteSQL(ETLConnectionString, query);
            }
        }
        
        internal static void TakeTask(int taskId)
        {
            if (EtlConnectionIsValid)
            {
                string query = $"exec dbo.etl_setTaskStatus {taskId}, 1;";
                splSqlService.ExecuteSQL(ETLConnectionString, query);
            }
            
        }

        public static bool CheckTarget(DataTransferTask dataTransferTask)
        {
            bool TargetIsExists = splSqlService.ObjectIsExist(dataTransferTask.TargetConnectionString, dataTransferTask.TargetTableName);
            if (!TargetIsExists)
            {
                System.Data.DataRowCollection SchemaRows = DataSchemaService.getSchemaRowsCollection(dataTransferTask.SourceConnectionString, dataTransferTask.SourceGetDataQuery);
                if (SchemaRows == null) return false;
                CreateTargetTable(dataTransferTask.TargetTableName, SchemaRows, dataTransferTask.TargetConnectionString, true);
                return true;
            }
            else //check Source = Target
            {
                DataRowCollection SourceRows = DataSchemaService.getSchemaRowsCollection(dataTransferTask.SourceConnectionString,dataTransferTask.SourceGetDataQuery);
                string TargetQuery = $"select * from {dataTransferTask.TargetTableName}";
                DataRowCollection TargetRows = DataSchemaService.getSchemaRowsCollection(dataTransferTask.TargetConnectionString, TargetQuery);
                if(DataSchemaService.DataRowsIsEqual(SourceRows, TargetRows))
                {
                    return true;
                }
                else
                {
                    System.Data.DataRowCollection SchemaRows = DataSchemaService.getSchemaRowsCollection(dataTransferTask.SourceConnectionString, dataTransferTask.SourceGetDataQuery);
                    if (SchemaRows == null) return false;
                    CreateTargetTable(dataTransferTask.TargetTableName, SchemaRows, dataTransferTask.TargetConnectionString, true);
                    return true;
                }

            }
        }
        private static void CreateTargetTable(string TargetTableName, System.Data.DataRowCollection ColumnsDefinition,string TargetConnectionString, bool DropTable = false)
        {
            
            List<string> Columns = splSqlService.GetColumnsDefinitions(ColumnsDefinition);
            //generate Create table script
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            sb.AppendLine($"IF(OBJECT_ID('{TargetTableName}') IS NOT NULL) DROP TABLE {TargetTableName};");
            sb.AppendLine($"CREATE TABLE {TargetTableName}(");
            int column_id = 0;
            foreach(string ColumnDefinition in Columns)
            {
                if (column_id > 0) sb.AppendLine(", ");
                sb.Append(ColumnDefinition);
                column_id++;
            }
            sb.Append(")");

            //upply to server
            splSqlService.ExecuteSQL(TargetConnectionString, sb.ToString());
        }
        
        

        public static int CopyDataOverDataTable(DataTransferTask dataTransferTask, int batchSize = 5000)
        {
            int totalCount = 0;
            int rowsCount = 0;
            
            List<Thread> bulkTasks = new List<Thread>();
            DataTable dt = DataSchemaService.getDataTableSchemaOnly(
                dataConnectionSting: dataTransferTask.SourceConnectionString, 
                dataQuery: dataTransferTask.SourceGetDataQuery, 
                option: CommandBehavior.SchemaOnly
                );
            dt.TableName = dataTransferTask.TargetTableName;
            
            //clear target table
            splSqlService.ExecuteSQL(dataTransferTask.TargetConnectionString, $"TRUNCATE TABLE {dataTransferTask.TargetTableName}");

            object[][] dataRows = new object[batchSize][];
            using (SqlDataReader dr = splSqlService.GetDataReader(dataTransferTask.SourceConnectionString, dataTransferTask.SourceGetDataQuery))
            {
                try
                {
                    while (dr.Read())
                    {
                        dataRows[rowsCount] = GetDataRow(dr);
                        rowsCount++;
                        if (rowsCount == batchSize)
                        {
                            bulkTasks.Add(Megafunction(dataTransferTask.TargetConnectionString, dataRows, dt, rowsCount));
                            totalCount += rowsCount;
                            dataRows = new object[batchSize][];
                            rowsCount = 0;
                        }
                    }
                    if (rowsCount > 0)
                    {
                        totalCount += rowsCount;
                        bulkTasks.Add(Megafunction(dataTransferTask.TargetConnectionString, dataRows, dt, rowsCount));
                    }
                }
                catch (SqlException sqlex)
                {
                    Logger.WriteErrorLog($"Errore while SqlBulkCopy {dataTransferTask.TargetTableName} to {dataTransferTask.TargetConnectionString}");
                    Logger.WriteErrorLog(sqlex.Message);
                }
                finally
                {
                    
                }
                while (bulkTasks.Any(a => a.ThreadState != ThreadState.Stopped))
                {
                    Thread.Sleep(500);
                } ;//wait for last bulk insert
                dr.Close();
            }
            //System.GC.Collect();
            return totalCount;
        }
        private static DataTable GetTable(object[][] dataRows, DataTable dt, int batchSize)
        {
            DataTable dtq = new DataTable();
            dtq = dt.Clone();  //copy structure to new datatable
            dtq.BeginLoadData();

            for(int i = 0; i < batchSize; i++)
            {
                dtq.LoadDataRow(dataRows[i], LoadOption.OverwriteChanges);
            }
            return dtq;
        }
        private static Thread Megafunction(string targetConnectionString, object[][] dataRows, DataTable dt, int batchSize)
        {
            Thread T = new Thread(() => {
                DataTable dtq = GetTable(dataRows, dt, batchSize);
                WriteDatatableToServer(dtq, targetConnectionString);
            });
            T.Start();
            return T;
        }
        private static void WriteDatatableToServer(DataTable dt, string targetConnectionString)
        {

            using (SqlConnection targetConnection = new SqlConnection(targetConnectionString))
            {
                targetConnection.Open();
                using (SqlBulkCopy bulk = new SqlBulkCopy(targetConnection) { DestinationTableName = dt.TableName })
                {
                    try
                    {
                        foreach (DataColumn col in dt.Columns) { bulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping(col.ColumnName, col.ColumnName)); }
                        bulk.WriteToServer(dt);
                        bulk.Close();
                        //Console.WriteLine($"END Task {taskNumber} write dt:{dt.Rows.Count} rows");
                    }
                    catch (SqlException sqlEx)
                    {
                        Logger.WriteErrorLog(sqlEx.Message);
                    }
                    finally
                    {
                        if (targetConnection.State == ConnectionState.Open) targetConnection.Close();
                    }
                }
            }
            dt.Dispose();
            //System.GC.Collect();
        }
        //static System.Threading.Mutex mutexObj = new System.Threading.Mutex();
        private static Thread WriteDatatableToServerThead(DataTable dt, string targetConnectionString, int taskNumber)
        {
            System.Threading.Thread T = new System.Threading.Thread(() => {
               //mutexObj.WaitOne();
                //Console.WriteLine($"BEGIN Task {taskNumber} write dt:{dt.Rows.Count} rows");
                using (SqlConnection targetConnection = new SqlConnection(targetConnectionString))
                {
                    targetConnection.Open();
                    using (SqlBulkCopy bulk = new SqlBulkCopy(targetConnection) { DestinationTableName = dt.TableName })
                    {
                        try
                        {
                            foreach(DataColumn col in dt.Columns) { bulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping(col.ColumnName, col.ColumnName)); }
                            bulk.WriteToServer(dt);
                            bulk.Close();
                            //Console.WriteLine($"END Task {taskNumber} write dt:{dt.Rows.Count} rows");
                        }
                        catch (SqlException sqlEx)
                        {
                            Logger.WriteErrorLog(sqlEx.Message);
                        }
                        finally
                        {
                            if (targetConnection.State == ConnectionState.Open) targetConnection.Close();
                        }
                    }
                }
                dt.Dispose();
                System.GC.Collect();
               //mutexObj.ReleaseMutex();
            });
            T.Start();
            return T;
        }
        
   
        private static object[] GetDataRow(SqlDataReader dr)
        {
            object[] buffer = new object[dr.FieldCount];
            dr.GetValues(buffer);
            return buffer;
        }

        
        public static void CopyDataOverDataReader(DataTransferTask dataTransferTask)
        {
            //clear target table
            splSqlService.ExecuteSQL(dataTransferTask.TargetConnectionString, $"TRUNCATE TABLE {dataTransferTask.TargetTableName}");
            SqlDataReader dr = splSqlService.GetDataReader(dataTransferTask.SourceConnectionString, dataTransferTask.SourceGetDataQuery);
            using (SqlConnection targetCon = new SqlConnection(dataTransferTask.TargetConnectionString))
            {
                try
                {
                    using (SqlBulkCopy bulk = new SqlBulkCopy(targetCon)
                    {
                        DestinationTableName = dataTransferTask.TargetTableName,
                        BatchSize = 10000
                    })
                    {
                        foreach (DataRow col in dr.GetSchemaTable().Rows)
                        {
                            bulk.ColumnMappings.Add(new SqlBulkCopyColumnMapping((string)col["ColumnName"], (string)col["ColumnName"]));
                        }
                        targetCon.Open();
                        bulk.WriteToServer(dr);
                        bulk.Close();
                    }
                    targetCon.Close();
                }
                catch (SqlException sqlex)
                {
                    Logger.WriteErrorLog($"Errore while SqlBulkCopy {dataTransferTask.TargetTableName} to {dataTransferTask.TargetConnectionString}");
                    Logger.WriteErrorLog(sqlex.Message);
                }
                finally
                {
                    if (targetCon.State == ConnectionState.Open) targetCon.Close();
                    if (!dr.IsClosed) dr.Close();
                }
            }
            dr.Close();
            //System.GC.Collect();
        }
        public static bool CheckTaskIsBusy(int taskId)
        {
            return (bool)splSqlService.GetScalarSql(ConnectionString: ETLConnectionString, Query: $"exec dbo.[ETL_getTaskStatus] {taskId}");
        }
        
    }
}
