using System;
using System.Data;
using System.Data.Common;
using System.Net.WebSockets;
using System.Transactions;
using System.Xml.Linq;
using MySqlConnector;


namespace testsqlfill

{
    internal class Program
    {

        static string server = "localhost";
        static string database = "test";
        static string username = "root";
        static string password = "";

        static void Main(string[] args)
        {
            string connection = "SERVER= " + server + ";" + "DATABASE=" + database + ";" + "UID= " + username + ";" + "PASSWORD=" + password;
            MySqlConnection con = new MySqlConnection(connection);
            con.Open();

            using (var conn = new MySqlConnection(connection))
            {
                Console.WriteLine("connection created successfuly");
            }

            string queryCustomer = "SELECT * FROM miller WHERE id >= 101;";
            MySqlDataAdapter adapter = new MySqlDataAdapter(queryCustomer, connection);


            DataSet dataSet = new DataSet();
            DataTable dt = new();
            adapter.Fill(dataSet, "miller");
            dt = dataSet.Tables[0];

            //101-602 
            DataTable Tires = new DataTable();

            Tires.Clear();
            Tires.Columns.Add("Id");
            Tires.Columns.Add("Number");
            Tires.Columns.Add("EventID");
            Tires.Columns.Add("StartDate");
            Tires.Columns.Add("EndDate");
            Tires.Columns.Add("Type");

            dataSet.Tables.Add(Tires);


            Random rnd = new Random();
            //DateTime d1 = new DateTime(2019, 09, 10, rnd.Next(8,17), rnd.Next(8, 61), 25);
            DateTime d1 = new DateTime(2019, 09, 10, 08, 00, 00);
            DateTime d2;

            int num = 0;
            int helper = 0;
            int type = rnd.Next(4);

            for (int i = 0; i < 10000; i++)
            {
                if (i % 100 == 0)
                { 
                d1 = d1.AddMinutes(15);
                }
                //d1 = d1 + d2;
                d2 = d1.AddMinutes(20);
                Tires.Rows.Add(new object[] { i, num, dt.Rows[helper][0], d1.ToString("yyyy-MM-dd HH:mm"), d2.ToString("yyyy-MM-dd HH:mm"), type });
                d1 = d1.AddMinutes(20);
                helper++;
                if (helper == 72)
                {
                    num++;
                    type = rnd.Next(1, 4);
                    helper = 0;
                }
            }

            #region 
            //string insertString = @"insert into tires values(@Id,@Number,@EventID,@StartDate,@EndDate,@Type)";
            //MySqlCommand insertCommand = new MySqlCommand(insertString);
            //insertCommand.Parameters.AddWithValue("@Id", Tires.Rows[0]["Id"]);
            //insertCommand.Parameters.AddWithValue("@Number", Tires.Rows[0]["Number"]);
            //insertCommand.Parameters.AddWithValue("@EventID", Tires.Rows[0]["EventID"]);
            //insertCommand.Parameters.AddWithValue("StartDate", Tires.Rows[0]["StartDate"].ToString("yyyy-MM-dd"));
            //insertCommand.Parameters.AddWithValue("@EndDate", Tires.Rows[0]["EndDate"]);
            //insertCommand.Parameters.AddWithValue("@Type", Tires.Rows[0]["Type"]);
            //using (MySqlConnection conn = new MySqlConnection(connection))
            //{
            //    conn.Open();
            //    insertCommand.Connection = conn;
            //    using (MySqlDataAdapter adapters = new MySqlDataAdapter())
            //    {
            //        adapters.InsertCommand = insertCommand;
            //        //var dataTable = (DataTable)stuDetBindingSource.DataSource;

            //        adapters.Update(Tires);
            //        Console.WriteLine("Successfully to update the table.");
            //    }
            //}

            
            //foreach (DataTable thisTable in dataSet.Tables)
            //{
            //    // For each row, print the values of each column.
            //    foreach (DataRow row in thisTable.Rows)
            //    {
            //        foreach (DataColumn column in thisTable.Columns)
            //        {
            //            Console.WriteLine(row[column]);
            //        }
            //    }
            //}

            // create table if not exists 
            //string createTableQuery = @"Create Table [Tires] 
            //            ( SaleDate datetime, ItemName nvarchar(1000),ItemsCount int)";
            //MySqlCommand command = new MySqlCommand(createTableQuery, connection);
            //command.ExecuteNonQuery();

            // Copy the DataTable to SQL Server Table using SqlBulkCopy
            //using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(connection))
            //{
            //    sqlBulkCopy.DestinationTableName = salesData.TableName;

            //    foreach (var column in salesData.Columns)
            //        sqlBulkCopy.ColumnMappings.Add(column.ToString(), column.ToString());

            //    sqlBulkCopy.WriteToServer(salesData);
            //}
            #endregion


            BulkInsertMySQL(Tires, "Tires");

            Console.ReadKey();
        }

        public static void BulkInsertMySQL(DataTable table, string tableName)
        {
            string connect = "SERVER= " + server + ";" + "DATABASE=" + database + ";" + "UID= " + username + ";" + "PASSWORD=" + password;
            using (MySqlConnection connection = new MySqlConnection(connect))
            {
                connection.Open();

                using (MySqlTransaction tran = connection.BeginTransaction(System.Data.IsolationLevel.Serializable))
                {
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        cmd.Connection = connection;
                        cmd.Transaction = tran;
                        cmd.CommandText = $"SELECT * FROM " + tableName + " limit 0";
                       

                        using (MySqlDataAdapter adapter = new MySqlDataAdapter(cmd))
                        {
                            adapter.UpdateBatchSize = 10000;
                            using (MySqlCommandBuilder cb = new MySqlCommandBuilder(adapter))
                            {
                                cb.SetAllValues = true;
                                adapter.Update(table);
                                Console.WriteLine("Successfully to update the table.");
                                tran.Commit();
                            }
                        };
                    }
                }
            }
        }
    }
}