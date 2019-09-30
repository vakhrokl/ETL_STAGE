using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ETL_STAGE
{
    enum LogTypeEnum { console, file};
    static class Logger
    {
        const LogTypeEnum logType = LogTypeEnum.console;
        public static string fileName = $"{DateTime.Now.ToString("yyyyMMdd_HHmm")}_SpellabsTranslomPortalLoader.log";
        public static string ErrorfileName = $"{DateTime.Now.ToString("yyyyMMdd_HHmm")}_ErrorSpellabsTranslomPortalLoader.log";
        public static void WriteLog(string logMessage)
        {
            switch (logType)
            {
                case LogTypeEnum.file:

                    System.IO.File.AppendAllText(fileName, $"{DateTime.Now.ToString("dd.MM.yyyy HH.mm.ss")}: {logMessage}"+Environment.NewLine);
                    break;
                case LogTypeEnum.console:
                    Console.WriteLine(DateTime.Now.ToString("yyyyMMdd_HHmm: ")+logMessage);
                    break;
            }
        }
        public static void WriteErrorLog(string logMessage)
        {
            switch (logType)
            {
                case LogTypeEnum.file:
                    System.IO.File.AppendAllText(ErrorfileName, $"{DateTime.Now.ToString("dd.MM.yyyy HH.mm.ss")}: {logMessage}" + Environment.NewLine);
                    break;
                case LogTypeEnum.console:
                    Console.WriteLine($"{DateTime.Now.ToString("yyyyMMdd_HHmm")} ERRORE: {logMessage}");
                    break;
            }
        }
    }
}
