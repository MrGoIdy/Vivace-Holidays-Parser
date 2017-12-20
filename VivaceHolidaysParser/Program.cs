using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;


/* Created BY Zolotarev Alexander 12/2017 */
namespace VivaceHolidaysParser
{
    class Program
    {
        private static string GetConnectionString()
        {
            return (new MySqlConnectionStringBuilder
            {
                Server = "5.132.159.203",
                UserID = "alex",
                Password = "Minsk2017!",
                Database = "VIVACE"
            })
            .ConnectionString;
        }

        struct TableToSave
        {
            public string date;
            public string country;
            public string holiday;
            public string exchange;
            public string details;

            public TableToSave(string date, string country, string holiday, string exchange, string details)
            {
                var day = date.Split('.')[0];
                var month = date.Split('.')[1];
                var year = date.Split('.')[2];
                this.date = year+"."+month+"."+day;
                this.country = country;
                this.holiday = holiday;
                this.exchange = exchange;
                this.details = details;
            }
        }

        public static bool DeleteAllFromTableHolidays()
        {
            try
            {
                using (MySqlConnection connection = new MySqlConnection(GetConnectionString()))
                {
                    using (MySqlCommand query = connection.CreateCommand())
                    {
                        connection.Open();

                        using (MySqlCommand command = new MySqlCommand(
                "DELETE FROM T_HOLIDAYS WHERE DATE IS NOT NULL", connection))
                        {
                            command.ExecuteNonQuery();
                        }
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }

        }

        public static bool InsertIntoTableHolidays(string date, string country, string holiday, string exchange, string details)
        {
            try
            {
                using (MySqlConnection connection = new MySqlConnection(GetConnectionString()))
                {
                    using (MySqlCommand query = connection.CreateCommand())
                    {
                        connection.Open();

                        using (MySqlCommand command = new MySqlCommand(
                "INSERT INTO T_HOLIDAYS VALUES(@Date, @Country, @Holiday, @Exchange, @Details)", connection))
                        {
                            command.Parameters.Add(new MySqlParameter("Date", date));
                            command.Parameters.Add(new MySqlParameter("Country", country));
                            command.Parameters.Add(new MySqlParameter("Holiday", holiday));
                            command.Parameters.Add(new MySqlParameter("Exchange", exchange));
                            command.Parameters.Add(new MySqlParameter("Details", details));
                            command.ExecuteNonQuery();
                        }
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                return false;
            }

        }


        static string ConvertToGermany(string str)
        {
            return str.Replace(@"\u00f6", "ö").Replace(@"\u00d6", "Ö").Replace(@"\u00df", "ß").Trim();
        }

        static void Main(string[] args)
        {
            Console.WriteLine("--Vivace Holidays Parser--");
            using (var web = new WebClient())
            {
                web.Headers.Add("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36");
                try
                {
                    int year = 2015;
                    var doc = new HtmlAgilityPack.HtmlDocument();
                    //2015
                    List<TableToSave> data = new List<TableToSave>();
                    int errorCounter = 0;
                    int i = 0;

                    for (year = 2015; year <= 2018; year++)
                    {

                        doc.LoadHtml(web.DownloadString("https://www.wallstreet-online.de/_rpc/json/news/calendar/getCalendarTable?formtype=holiday&range=" + year + "&country%5B%5D=22&country%5B%5D=25&country%5B%5D=2&country%5B%5D=6&country%5B%5D=93&country%5B%5D=32&country%5B%5D=33&country%5B%5D=1&country%5B%5D=3&country%5B%5D=20&offset="));
                        var dataContainer = doc.DocumentNode.SelectNodes("//td");
                        int collumnNumber = 0;
                        int trCount = dataContainer.Count() / 5 - 1;
                        for (i = 0; i <= trCount; i++)
                        {
                            try
                            {
                                string date = ConvertToGermany(dataContainer[collumnNumber].InnerText.Split('<')[0]);
                                string country = ConvertToGermany(dataContainer[collumnNumber + 1].InnerText.Split('<')[0]);
                                string holiday = ConvertToGermany(dataContainer[collumnNumber + 2].InnerText.Split('>')[1].Split('<')[0]);
                                string exchange = ConvertToGermany(dataContainer[collumnNumber + 3].InnerText.Split('<')[0]);
                                string details = ConvertToGermany(dataContainer[collumnNumber + 4].InnerText.Split('<')[0]);
                                data.Add(new TableToSave(date, country, holiday, exchange, details));
                                Console.WriteLine("Parsing: " + i + "/" + trCount + "(To " + year + " year)");
                                collumnNumber += 5;
                            }
                            catch
                            {
                                Console.WriteLine("Error with data with row-index" +i+ "/Year:" + year + "!");
                                errorCounter++;
                            }
                        }
                       
                    }
                    
                    i = 0;
                    Console.Write("Removing old data from DataBase...");
                    DeleteAllFromTableHolidays();
                    Console.WriteLine(" complete");
                    Console.WriteLine("Connecting to DataBase");
                    foreach (var item in data)
                    {
                        i++;
                        if (InsertIntoTableHolidays(item.date, item.country, item.holiday, item.exchange, item.details))
                        {
                            Console.WriteLine("Saving " + (i - errorCounter) + "/" + data.Count);
                        }
                        else
                        {
                            Console.WriteLine("Error with data " + item.date + " " + item.country + " " + year + "!");
                        }
                    }
                }
                catch(Exception ex)
                {
                    Console.WriteLine(ex.StackTrace);
                    Console.ReadLine();
                }
            }
        }
    }
}