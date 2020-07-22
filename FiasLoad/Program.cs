using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Data.OleDb;
using System.Data.Common;
using System.IO.Compression;
using LogsLib;
using System.Text.RegularExpressions;
using System.Runtime.Serialization.Formatters.Binary;

namespace FiasLoad
{
    class Program
    {
        static readonly WebClient client = new WebClient();
        static private readonly string ConnectionStringTemplate = "Data Source=192.168.0.100,1433\\MSSQLSERVER;Initial Catalog=FIAS;Integrated Security=true;";
        private const string pattern = @"\|[ ]+?\|";
        private const string pattern2 = @"(\d)[ ]+?\|";
        private const int max_num_lines = 100000;
        private static int count_packets = 0;
        static void Main(string[] args)
        {
            Logs.SetLogParams(true, 10);
            var execSQL = new SqlCommand();
            execSQL.CommandType = CommandType.Text;
            execSQL.Connection = new SqlConnection(ConnectionStringTemplate);
            execSQL.Connection.Open();

            execSQL.CommandText = "select top 1 VerDate from dbo.Versions";
            DateTime? last_update_verDate = (DateTime?)execSQL.ExecuteScalar();

            string last_loaded = null;

            if (last_update_verDate != null)
            {
                DateTime workDate1 = (DateTime)last_update_verDate;
                execSQL.CommandText = "select top 1 Loaded from dbo.Versions where VerDate ='" + workDate1.Date.ToString("yyyyMMdd") + "'";
                last_loaded = execSQL.ExecuteScalar().ToString();
            }



            execSQL.Connection.Close();
            execSQL.Dispose();

            Console.WriteLine(last_update_verDate == null ? "null" : last_update_verDate.ToString());

            DateTime last_verDate;
            try
            {

                string data = client.DownloadString("http://fias.nalog.ru/Public/Downloads/Actual/VerDate.txt");
                //Console.WriteLine(data);
                DateTime.TryParse(data, out last_verDate);
                Console.WriteLine(last_verDate.Date);
                if ((last_verDate.Date == last_update_verDate?.Date) && (last_loaded !=""))
                    return;

                if ((last_verDate.Date != last_update_verDate?.Date))
                {

                    Logs.SaveLogString("Найдена новая версия");
                    execSQL = new SqlCommand();
                    execSQL.CommandType = CommandType.Text;
                    execSQL.Connection = new SqlConnection(ConnectionStringTemplate);
                    execSQL.Connection.Open();
                    execSQL.CommandText = "insert into dbo.Versions(VerDate) values('" + last_verDate.Date.ToString("yyyyMMdd") + "')";

                    execSQL.ExecuteNonQuery();
                    execSQL.Connection.Close();
                    execSQL.Dispose();
                }

                //создаем каталог
                string path = @"C:\FIAS_LAST\" + last_verDate.Date.ToString("yyyyMMdd");
                DirectoryInfo dir_fias = new DirectoryInfo(path);
                if (dir_fias.Exists == false)
                    dir_fias.Create();
                string zip_path = path + "\\fias_dbf.zip";
                Console.WriteLine("Началась загрузка в " + DateTime.Now.ToString());
                Logs.SaveLogString("Началась загрузка zip");
                if (File.Exists(zip_path) == false)
                    client.DownloadFile("http://fias.nalog.ru/Public/Downloads/Actual/fias_dbf.zip", zip_path);
                if (File.Exists(zip_path) == false)
                {
                    Logs.SaveLogString("Ошибка загрузки zip");
                    return;
                }

                execSQL = new SqlCommand();
                execSQL.CommandType = CommandType.Text;
                execSQL.Connection = new SqlConnection(ConnectionStringTemplate);
                execSQL.Connection.Open();
                execSQL.CommandText = "update v set Downloaded = '" + DateTime.Now.Date.ToString("yyyyMMdd") + "'  from dbo.Versions v where VerDate ='" + last_verDate.Date.ToString("yyyyMMdd") + "'";

                execSQL.ExecuteNonQuery();
                execSQL.Connection.Close();
                execSQL.Dispose();

                Console.WriteLine("Загрузка завершена в " + DateTime.Now.ToString());
                Logs.SaveLogString("Загрузка zip завершена");
                //разархивация
                Console.WriteLine("Началась разархивация zip в " + DateTime.Now.ToString());
                Logs.SaveLogString("Началась разархивация zip");
                using (ZipArchive archive = ZipFile.OpenRead(zip_path))
                {
                    foreach (ZipArchiveEntry entry in archive.Entries)
                    {
                        if (File.Exists(Path.Combine(path, entry.FullName)) == false)
                            if((Left(entry.Name, 6) == "ADDROB") || (Left(entry.Name, 5) == "HOUSE"))
                                entry.ExtractToFile(Path.Combine(path, entry.FullName), false);
                    }
                }
                Console.WriteLine("Разархивация zip завершена в " + DateTime.Now.ToString());
                Logs.SaveLogString("Разархивация zip завершена");

                execSQL = new SqlCommand();
                execSQL.CommandType = CommandType.Text;
                execSQL.Connection = new SqlConnection(ConnectionStringTemplate);
                execSQL.Connection.Open();
                execSQL.CommandText = "update v set Unziped = '" + DateTime.Now.Date.ToString("yyyyMMdd") + "'  from dbo.Versions v where VerDate ='" + last_verDate.Date.ToString("yyyyMMdd") + "'";

                execSQL.ExecuteNonQuery();
                execSQL.Connection.Close();
                execSQL.Dispose();

                //string last_ver_csv = null;

                //execSQL = new SqlCommand();
                //execSQL.CommandType = CommandType.Text;
                //execSQL.Connection = new SqlConnection(ConnectionStringTemplate);
                //execSQL.Connection.Open();
                //execSQL.CommandText = "select top 1 CopyToCSV from dbo.Versions where VerDate ='" + last_verDate.Date.ToString("yyyyMMdd") + "'";
                //last_ver_csv = execSQL.ExecuteScalar().ToString();
                //execSQL.Connection.Close();
                //execSQL.Dispose();

                //if (last_ver_csv != "")
                //{
                //    Console.WriteLine("Началась запись в csv в " + DateTime.Now.ToString());
                //    Logs.SaveLogString("Началась запись в csv");
                //    ret = DBF2CSV(path);
                //    Console.WriteLine("запись в csv завершена в " + DateTime.Now.ToString());
                //    Logs.SaveLogString("запись в csv завершена");
                //}

                Console.WriteLine("Началась загрузка в базу в " + DateTime.Now.ToString());
                Logs.SaveLogString("Началась загрузка в базу");

                execSQL = new SqlCommand();
                execSQL.CommandType = CommandType.Text;
                execSQL.Connection = new SqlConnection(ConnectionStringTemplate);
                execSQL.Connection.Open();
                execSQL.CommandText = "truncate table [dbo].[Fias_AddrObj]; truncate table [dbo].[Fias_House]";

                execSQL.ExecuteNonQuery();
                execSQL.Connection.Close();
                execSQL.Dispose();

                if (!DBF2SQL(path))
                    return;

                execSQL.CommandType = CommandType.Text;
                execSQL.Connection = new SqlConnection(ConnectionStringTemplate);
                execSQL.Connection.Open();
                execSQL.CommandText = "update v set CopyToCSV = '" + DateTime.Now.Date.ToString("yyyyMMdd") + "'  from dbo.Versions v where VerDate ='" + last_verDate.Date.ToString("yyyyMMdd") + "'";
                execSQL.ExecuteNonQuery();
                execSQL.Connection.Close();


                execSQL.CommandType = CommandType.StoredProcedure;
                execSQL.CommandTimeout = 0;
                execSQL.Connection = new SqlConnection(ConnectionStringTemplate);
                execSQL.Connection.Open();
                execSQL.CommandText = "FormatFiasHouse";
                execSQL.Parameters.Add("@VerDate", SqlDbType.NVarChar,10).Value = last_verDate.Date.ToString("yyyyMMdd");


                execSQL.ExecuteNonQuery();
                execSQL.Connection.Close();

                execSQL.CommandType = CommandType.Text;
                execSQL.Connection = new SqlConnection(ConnectionStringTemplate);
                execSQL.Connection.Open();
                execSQL.CommandText = "update v set Loaded = '" + DateTime.Now.Date.ToString("yyyyMMdd") + "'  from dbo.Versions v where VerDate ='" + last_verDate.Date.ToString("yyyyMMdd") + "'";
                execSQL.ExecuteNonQuery();
                execSQL.Connection.Close();

                Console.WriteLine("загрузка в базу завершена в " + DateTime.Now.ToString());
                Logs.SaveLogString("загрузка в базу завершена");

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Logs.SaveLogString(e.Message);
                return;
            }
            finally
            {
                Console.ReadKey();
            }

            return;
        }


        static private bool DBF2SQL(string dbfs_path)
        {
            try
            {
                //var file_csv = File.Create(@"c:\FIAS_LAST\CSV\ADDROB.csv");
                //file_csv.Close();
                //file_csv = File.Create(@"c:\FIAS_LAST\CSV\HOUSE.csv");
                //file_csv.Close();

                string filePath = dbfs_path;//Path.GetDirectoryName(rfileName);
                string sourceConnectionString = "Provider = VFPOLEDB.1; Data Source = " + filePath + ";";

                var sourceConnection = new OleDbConnection(sourceConnectionString);
                sourceConnection.Open();

                DataTable tables = sourceConnection.GetSchema(OleDbMetaDataCollectionNames.Tables);

                string sql;
                string tablename;

                OleDbCommand sourceCommand;
                string fields_list;
                string buf;
                StringBuilder sb = new StringBuilder();
                int count = 0;//счетчик строк
                int count2 = 0;//счетчик файлов
                foreach (DataRow r in tables.Rows)
                {
                    count2++;
                    fields_list = "";
                    tablename = r.ItemArray[2].ToString();
                    if (Left(tablename.ToUpper(), 6) == "ADDROB")
                    {
                        Console.WriteLine(tablename);
//                        filename = @"c:\FIAS_LAST\CSV\ADDROB.csv";
                        fields_list = GenearateSQLForAddrObjTable();
                    }
                    else
                    {
//                        filename = @"c:\FIAS_LAST\CSV\HOUSE.csv";
                        Console.WriteLine(tablename);
                        fields_list = GenearateSQLForHouseTable();
                    }

                    sql = "SELECT " + fields_list.ToLower() + " FROM " + tablename;

                    List<string> values = new List<string>();
                    sourceCommand = new OleDbCommand(sql, sourceConnection);
                    using (var reader = sourceCommand.ExecuteReader())
                    {

                        while (reader.Read())
                        {

                            if (count == 0)
                            {
                                sb = new StringBuilder();

                                ////Добавить названия столбцоы
                                //values.Clear();

                                //for (int ii = 0; ii < reader.FieldCount; ii++)
                                //{
                                //    values.Add(reader.GetName(ii));
                                //}
                                //sb.AppendLine(string.Join("|", values));
                            }

                            count++;

                            string[] line_delimeter = { "\r\n" };
                            values.Clear();
                            for (int ii = 0; ii < reader.FieldCount; ii++)
                            {
                                values.Add(reader[ii].ToString());
                            }

                            sb.AppendLine(string.Join("|", values));
                            if (count == max_num_lines)
                            {
                                buf = sb.ToString();
                                //var ar1 = buf.Split(line_delimeter);//для теста
                                while (Regex.Matches(buf, pattern, RegexOptions.IgnoreCase).Count > 0)
                                {
                                    buf = Regex.Replace(buf, pattern, "||");
                                }
                                buf = Regex.Replace(buf, pattern2, "$1|");

                                sb.Clear();

                                //var ar1 = buf.Split(line_delimeter, StringSplitOptions.RemoveEmptyEntries);
                                //buf = null;

                                //if (!TestSqlBulkCopy(ref ar1, tablename))
                                if (!TestSqlBulkCopy2(ref buf, tablename))
                                    return false;
                                count = 0;
                            }

                        }
                    }
                    
                    if (Right(tablename, 2) == "99") //(count != 0)
                    {
                        buf = sb.ToString();
                        while (Regex.Matches(buf, pattern, RegexOptions.IgnoreCase).Count > 0)
                        {
                            buf = Regex.Replace(buf, pattern, "||");
                        }
                        buf = Regex.Replace(buf, pattern2, "$1|");

                        sb.Clear();

                        //var ar1 = buf.Split(line_delimeter, StringSplitOptions.RemoveEmptyEntries);
                        //buf = null;

                        //if (!TestSqlBulkCopy(ref ar1, tablename))
                        if (!TestSqlBulkCopy2(ref buf, tablename))
                            return false;

                        count = 0;
                        count2 = 0;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Logs.SaveLogString(e.Message);
                return false;
            }

            return true;

        }

        private static bool TestSqlBulkCopy2(ref string buf, string tablename)
        {
            count_packets++;
            //Сохранить в temp.csv
            string path_csv = @"c:\FIAS_LAST\CSV\temp.csv";
            var file_csv = File.Create(path_csv);
            file_csv.Close();

            File.AppendAllText(path_csv, buf);
            buf = null;

            string destinationTableName;

            if (Left(tablename.ToUpper(), 6) == "ADDROB")
                destinationTableName = "dbo.Fias_AddrObj";
            else
                destinationTableName = "dbo.Fias_House";

            SqlCommand execSQL = new SqlCommand();
            execSQL.CommandType = CommandType.Text;
            execSQL.Connection = new SqlConnection(ConnectionStringTemplate);
            execSQL.Connection.Open();
            execSQL.CommandTimeout = 0;
            execSQL.CommandText = "BULK INSERT " + destinationTableName + " ";
            execSQL.CommandText += "FROM '" + path_csv + "' ";
            execSQL.CommandText += "WITH ";
            execSQL.CommandText += "(";
//            execSQL.CommandText += "BATCHSIZE = 50000, ";
            execSQL.CommandText += "FIELDTERMINATOR = '|', ";
            execSQL.CommandText += "ROWTERMINATOR = '\r\n', ";
            execSQL.CommandText += "CODEPAGE = 'UTF8',";
            execSQL.CommandText += "KEEPNULLS ";
            execSQL.CommandText += ")";

            execSQL.ExecuteNonQuery();
            execSQL.Connection.Close();
            execSQL.Dispose();

            Console.WriteLine("загрузка пакета в базу завершена");

            if (count_packets % 10 == 0)
            {
                execSQL = new SqlCommand();
                execSQL.CommandType = CommandType.Text;
                execSQL.Connection = new SqlConnection(ConnectionStringTemplate);
                execSQL.Connection.Open();
                execSQL.CommandTimeout = 0;
                execSQL.CommandText = "DBCC SHRINKDATABASE (FIAS, TRUNCATEONLY)";
//                execSQL.CommandText = "BACKUP LOG WITH TRUNCATE_ONLY; ";

                execSQL.ExecuteNonQuery();
                execSQL.Connection.Close();
                execSQL.Dispose();
            }

            return true;
        }
        private static bool TestSqlBulkCopy(ref string[] lines, string tablename)//Данное значение типа String из источника данных не может быть преобразовано в тип uniqueidentifier указанного столбца назначения.
        {
            //bulk insert
            var columns = lines[0].Split('|');
            var dt = new DataTable();
            foreach (var c in columns)
                dt.Columns.Add(c);

            for (int i = 1; i < lines.Length; i++)
            {
                //Console.WriteLine(i);
                //if(i==99999)
                //    Console.WriteLine(lines[i]);
                dt.Rows.Add(lines[i].Split('|'));
            }
            using(SqlBulkCopy bulkCopy = new SqlBulkCopy(ConnectionStringTemplate))
            {
                DataTableReader reader = dt.CreateDataReader();
                if (Left(tablename.ToUpper(), 6) == "ADDROB")
                    bulkCopy.DestinationTableName = "dbo.Fias_AddrObj";
                else
                    bulkCopy.DestinationTableName = "dbo.Fias_House";

                // Set the bulkCopy.Properties
                //bulkCopy.BatchSize = 50;
                bulkCopy.EnableStreaming = true;
                bulkCopy.BulkCopyTimeout = 0;

                try
                {
                    // Write from the source to the destination.
                    bulkCopy.WriteToServer(reader);
                    Console.WriteLine("Запись в базу прошла");
                    Logs.SaveLogString("Запись в базу прошла");

                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    return false;
                }
                finally
                {
                    lines = null;
                    dt.Dispose();
                }

                return true;
            }

        }

        public static string GenearateSQLForAddrObjTable()
        {
            string ret = "";
            ret += "AOID, ";
            ret += "AOGUID, ";
            ret += "PARENTGUID, ";
            ret += "REGIONCODE, ";
            ret += "OFFNAME, ";
            ret += "POSTALCODE, ";
            ret += "SHORTNAME, ";
            ret += "AOLEVEL, ";
            ret += "CODE, ";
            ret += "PLAINCODE, ";
            ret += "ACTSTATUS, ";
            ret += "LIVESTATUS, ";
            ret += "CURRSTATUS, ";
            ret += "IFNSFL, ";
            ret += "TERRIFNSFL, ";
            ret += "IFNSUL, ";
            ret += "TERRIFNSUL, ";
            ret += "OKATO, ";
            ret += "OKTMO, ";
            ret += "STARTDATE, ";
            ret += "ENDDATE, ";
            ret += "UPDATEDATE, ";
            ret += "\"\" as STRSTATUS";

            return ret;
        }

        public static string GenearateSQLForHouseTable()
        {
            string ret = "";
            ret += "HOUSEID, ";
            ret += "HOUSEGUID, ";
            ret += "AOGUID, ";
            ret += "POSTALCODE, ";
            ret += "HOUSENUM, ";
            ret += "\"\" as C1, ";
            ret += "ESTSTATUS, ";
            ret += "BUILDNUM, ";
            ret += "STRUCNUM, ";
            ret += "IFNSFL, ";
            ret += "TERRIFNSFL, ";
            ret += "IFNSUL, ";
            ret += "TERRIFNSUL, ";
            ret += "OKATO, ";
            ret += "OKTMO, ";
            ret += "STARTDATE, ";
            ret += "ENDDATE, ";
            ret += "UPDATEDATE, ";
            ret += "STRSTATUS";

            return ret;
        }

        static private string Left(string str1, int num)
        {
            return str1.Substring(0, num);
        }
        static private string Right(string str1, int num)
        {
            int pos1 = str1.Length - num;
            return str1.Substring(pos1, num);
        }
    }
}
