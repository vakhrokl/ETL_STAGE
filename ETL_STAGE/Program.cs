using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETL_STAGE
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 1)
            {
                string ConnectionString = args[0].Replace('"', ' ');
                if (DataTransferTaskService.TestConnection(ConnectionString))
                {
                    foreach (var T in DataTransferTaskService.GetTasks(ConnectionString))
                    {
                        Logger.WriteLog($"Execute copy{T.SourceTableName} from {T.SourceName} to {T.TargetName}.{T.TargetTableName}");
                        ExecuteTask(T);
                    }
                }
                
            }
            else
            {
                Logger.WriteErrorLog("Only one argument allow / requeried. please type connection string as as single argument");
                Logger.WriteErrorLog("Example \"Server = ZALMAN; Database = STAGE; User Id = VKV; Password = VKV\"");
            }

            Console.WriteLine(Environment.NewLine + "Press any key...");
            Console.ReadKey();
        }

        private static void ExecuteTask(DataTransferTask t)
        {
            if(!DataTransferTaskService.CheckTaskIsBusy(t.TaskId) || true)
            {
                if (DataTransferTaskService.CheckTarget(t))
                {
                    DataTransferTaskService.TakeTask(t.TaskId);
                    var ds = DateTime.Now;
                    int rowcount = DataTransferTaskService.CopyDataOverDataTable(t, 15000);
                    Logger.WriteLog($"{rowcount} rows affected, duration: {(DateTime.Now - ds).TotalMilliseconds} ms");
                    DataTransferTaskService.ReleaseTask(t.TaskId);
                }
            }
            System.GC.Collect();
        }
    }
}
