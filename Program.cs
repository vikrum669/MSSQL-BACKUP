using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using Newtonsoft.Json;

namespace MSSQL_BACKUP
{
    class BackupInfo
    {
        public string[] database { get; set; }
        public bool backupALL { get; set; }
        public string path { get; set; }
    }
    class Program
    {
        static string connectionString = "Server=localhost;initial catalog=master;Trusted_Connection=True";
        static void Main(string[] args)
        {
            string listDatabaseQuery = "SELECT name FROM sys.databases d WHERE d.database_id > 4";

            try
            {
                string configuration = File.ReadAllText("mssql_backup.conf");
                BackupInfo backupInfo = JsonConvert.DeserializeObject<BackupInfo>(configuration);


                SqlConnection connection = new SqlConnection(connectionString);
                using (connection)
                {
                    connection.Open();

                    SqlCommand listDatabase_Cmd = new SqlCommand(listDatabaseQuery, connection);
                    SqlCommand backupDatabase_Cmd = new SqlCommand(null, connection);

                    //check if all database backup flag is on or not
                    // if true, all database will be backed up
                    // else only the specified databases in mssql_backup.conf file will be backed up
                    if (backupInfo.backupALL)
                    {
                        using (SqlDataAdapter adapter = new SqlDataAdapter(listDatabase_Cmd))
                        {
                            DataTable dblist = new DataTable();
                            adapter.Fill(dblist);
                            int count = dblist.Rows.Count;
                            for (int i = 0; i < count; i++)
                            {
                                string path = backupInfo.path + dblist.Rows[i]["name"].ToString() + ".bkp";
                                System.Console.WriteLine(path);
                                backupDatabase_Cmd.CommandText = "BACKUP DATABASE " + dblist.Rows[i]["name"].ToString() + " TO DISK='" + path + "' WITH INIT";
                                System.Console.WriteLine(backupDatabase_Cmd.CommandText);
                                System.Console.WriteLine(backupDatabase_Cmd.ExecuteNonQuery());
                                backupDatabase_Cmd.Parameters.Clear();
                            }
                        }
                    }
                    else
                    {
                        foreach (string dbname in backupInfo.database)
                        {
                            string path = backupInfo.path + dbname + ".bkp";
                            backupDatabase_Cmd.CommandText = "BACKUP DATABASE " + dbname + " TO DISK='" + path + "' WITH INIT";
                            backupDatabase_Cmd.ExecuteNonQuery();
                            backupDatabase_Cmd.Parameters.Clear();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                File.WriteAllText("Error.log", DateTime.Now + " " + ex.Message);
            }
        }
    }
}
