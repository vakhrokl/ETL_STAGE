using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETL_STAGE
{
    class DataTransferTask
    {
        public int TaskId { get; set; }
        //SOURCE PARAMETERS
        public int SourceId { get; set; }
        public string SourceConnectionString { get; set; }
        public string SourceName { get; set; }
        public string SourceTableName { get; set; }
        public string SourceGetDataQuery { get; set; }

        //TARGET PARAMETERS
        public string TargetConnectionString { get; set; }
        public string TargetName { get; set; }
        public string TargetTableName { get; set; }        
        
    }
}
