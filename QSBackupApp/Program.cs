using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.IO;
using System.ServiceProcess;
using System.Diagnostics;
using System.Globalization;
using System.Collections;

namespace QSBackupApp
{
    class Program
    {
        private  static string path;
        private static string staticPath;
        private static string databaseHostname;
        private static string databasePassword;
        private static string rootPath;
        private static string pgDumpPath;
        private static string qvdFolder;

        static void Main(string[] args)
        {

            string todayString = DateTime.Now.ToString("dd-MM-yyyy HH-mm-ss");
            string operation =  args[0].Length>0 ? args[0]: "backup";
            


            path = ConfigurationManager.AppSettings["path"];
            staticPath = path;
            databaseHostname = ConfigurationManager.AppSettings["databaseHostname"];
            databasePassword = ConfigurationManager.AppSettings["databasePassword"];
            rootPath = ConfigurationManager.AppSettings["rootPath"];
            pgDumpPath = ConfigurationManager.AppSettings["pgDumpPath"];
            qvdFolder = ConfigurationManager.AppSettings["qvdFolder"];

            if (operation == "backup")
            {
                path = path + "\\" + todayString;
                Directory.CreateDirectory(path);
            }




            FileStream fs = new FileStream(path + "\\" + "Status.txt", FileMode.Create);
            StreamWriter sw = new StreamWriter(fs);
            Console.SetOut(sw);





            Console.WriteLine("1. Operation: " + operation);
            Console.WriteLine("1.1. path:" + path);
            Console.WriteLine("1.2. databaseHostname:" + databaseHostname);
            Console.WriteLine("1.3. databasePassword length:" + databasePassword.Length);
            Console.WriteLine("1.4. rootPath:" + rootPath);
            Console.WriteLine("1.5. rootPath:" + qvdFolder);

            Logging("Operation:" + operation + " Started:");



            try
            {
                if (operation == "backup")
                {
                            
                    //stop services
                    Console.WriteLine("2. Stopping services");
                    string serviceStatus = StartStopService("QlikLoggingService");
                    Console.WriteLine("2.1. QlikLoggingService:" + serviceStatus);
                    serviceStatus = StartStopService("QlikSenseEngineService");
                    Console.WriteLine("2.2. QlikSenseEngineService:" + serviceStatus);
                    serviceStatus = StartStopService("QlikSensePrintingService");
                    Console.WriteLine("2.3. QlikSensePrintingService:" + serviceStatus);
                    serviceStatus = StartStopService("QlikSenseProxyService");
                    Console.WriteLine("2.4. QlikSenseProxyService:" + serviceStatus);
                    serviceStatus = StartStopService("QlikSenseSchedulerService");
                    Console.WriteLine("2.5. QlikSenseSchedulerService:" + serviceStatus);
                    serviceStatus = StartStopService("QlikSenseServiceDispatcher");
                    Console.WriteLine("2.6. QlikSenseServiceDispatcher:" + serviceStatus);
                    serviceStatus = StartStopService("QlikSenseRepositoryService");
                    Console.WriteLine("2.7. QlikSenseRepositoryService:" + serviceStatus);

                    //Backup database
                    Console.WriteLine("3. Backup Database: " + PostgreSqlDump(path));

                    //Copy shared folder
                    Console.WriteLine("4. Copy Shared content: " + Copy(rootPath,path + "\\share"));

                    Console.WriteLine("5. Copy DVDs folder: " + Copy(qvdFolder, path + "\\QVDs"));

                    //Start Services
                    Console.WriteLine("6. Starting services");
                    serviceStatus = StartStopService("QlikLoggingService",false);
                    Console.WriteLine("6.1. QlikLoggingService:" + serviceStatus);
                    serviceStatus = StartStopService("QlikSenseEngineService", false);
                    Console.WriteLine("6.2. QlikSenseEngineService:" + serviceStatus);
                    serviceStatus = StartStopService("QlikSensePrintingService", false);
                    Console.WriteLine("6.3. QlikSensePrintingService:" + serviceStatus);
                    serviceStatus = StartStopService("QlikSenseProxyService", false);
                    Console.WriteLine("6.4. QlikSenseProxyService:" + serviceStatus);
                    serviceStatus = StartStopService("QlikSenseSchedulerService", false);
                    Console.WriteLine("6.5. QlikSenseSchedulerService:" + serviceStatus);
                    serviceStatus = StartStopService("QlikSenseServiceDispatcher", false);
                    Console.WriteLine("6.6. QlikSenseServiceDispatcher:" + serviceStatus);
                    serviceStatus = StartStopService("QlikSenseRepositoryService", false);
                    Console.WriteLine("6.7. QlikSenseRepositoryService:" + serviceStatus);

                    //delete old backups
                    DeleteOldBackups();

                }
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message);
            }

            



            sw.Close();
            Logging("Operation:" + operation + " Finished:");



        }
        public static string StartStopService(string serviceName, Boolean stop = true, int timeoutMilliseconds = 180000)
        {
            ServiceController service = new ServiceController(serviceName);
            try
            {
                TimeSpan timeout = TimeSpan.FromMilliseconds(timeoutMilliseconds);

                if (stop)
                {
                    service.Stop();
                    service.WaitForStatus(ServiceControllerStatus.Stopped, timeout);
                }
                else
                {
                    service.Start();
                    service.WaitForStatus(ServiceControllerStatus.Running, timeout);
                }
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message);
            }
            return service.Status.ToString();
        }

		public static string PostgreSqlDump(string outFile)
		{
            String status = "Done";
			String dumpCommand = "\"" + pgDumpPath + "\\pg_dump.exe\" -h "+ databaseHostname + " -p 4432 -U postgres -b -F t -f \""+ outFile + "\\QSR_backup.tar" + "\" QSR";
			String passFileContent = "" + databaseHostname + ":4432:QSR:postgres:" + databasePassword + "";

			String batFilePath = Path.Combine(
				Path.GetTempPath(),
				Guid.NewGuid().ToString() + ".bat");

			String passFilePath = Path.Combine(
				Path.GetTempPath(),
				Guid.NewGuid().ToString() + ".conf");

			try
			{
				String batchContent = "";
				batchContent += "@" + "set PGPASSFILE=" + passFilePath + "\n";
                batchContent += "@" + dumpCommand;// "  > " + "\"" + outFile + "\"" + "\n";

				File.WriteAllText(
					batFilePath,
					batchContent,
					Encoding.ASCII);

				File.WriteAllText(
					passFilePath,
					passFileContent,
					Encoding.ASCII);

				if (File.Exists(outFile))
					File.Delete(outFile);

				ProcessStartInfo oInfo = new ProcessStartInfo(batFilePath);
				oInfo.UseShellExecute = false;
				oInfo.CreateNoWindow = true;

				using (Process proc = System.Diagnostics.Process.Start(oInfo))
				{
					proc.WaitForExit();
					proc.Close();
				}
                return status;

            }
            catch(Exception e)
            {
                status = e.Message;
                return status;
            }
			finally
			{
				if (File.Exists(batFilePath))
					File.Delete(batFilePath);

				if (File.Exists(passFilePath))
					File.Delete(passFilePath);
			}
		}


        public static string Copy(string sourceDirectory, string targetDirectory)
        {
            string status = "Done";
            try
            {
                var diSource = new DirectoryInfo(sourceDirectory);
                var diTarget = new DirectoryInfo(targetDirectory);

                CopyAll(diSource, diTarget);
                return status;
            }
            catch(Exception e)
            {
                status = e.Message;
                return status;
            }
            
        }

        public static void CopyAll(DirectoryInfo source, DirectoryInfo target)
        {
            Directory.CreateDirectory(target.FullName);

            // Copy each file into the new directory.
            foreach (FileInfo fi in source.GetFiles())
            {
                fi.CopyTo(Path.Combine(target.FullName, fi.Name), true);
            }

            // Copy each subdirectory using recursion.
            foreach (DirectoryInfo diSourceSubDir in source.GetDirectories())
            {
                DirectoryInfo nextTargetSubDir =
                    target.CreateSubdirectory(diSourceSubDir.Name);
                CopyAll(diSourceSubDir, nextTargetSubDir);
            }
        }



        public static void Logging(string txt)
        {
            string Fpath = staticPath + "\\log.txt";
            using (StreamWriter sw = File.AppendText(Fpath))
            {
                sw.WriteLine(txt + DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss"));
            }
        }

        public static void DeleteOldBackups()
        {
            string[] filesArray = Directory.GetDirectories(staticPath);

            List<DateTime> backupDates = new List<DateTime>();
            foreach (var d in Directory.GetDirectories(staticPath))
            {

                var dirName = new DirectoryInfo(d).Name;
                try
                {
                    DateTime result = DateTime.ParseExact(dirName, "dd-MM-yyyy HH-mm-ss", CultureInfo.InvariantCulture);
                    backupDates.Add(result);
                }
                catch (FormatException)
                {
                }

            }

            var tbackupDates = backupDates.OrderByDescending(x => x);
            int count = 0;
            foreach (var tb in tbackupDates)
            {
                if (count > 2)
                {
                    if (Directory.Exists(staticPath + "\\" + tb.ToString("dd-MM-yyyy HH-mm-ss")))
                    {
                        Directory.Delete(staticPath + "\\" + tb.ToString("dd-MM-yyyy HH-mm-ss"), true);
                        Logging("Delete old backup: " + staticPath + "\\" + tb.ToString("dd-MM-yyyy HH-mm-ss"));
                    }
                }
                count++;
            }

        }

    }
}
