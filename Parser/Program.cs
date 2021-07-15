using System;
using System.IO;
using System.Linq;
using Newtonsoft.Json.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using System.Collections.Generic;
using RestSharp;
using MySql.Data.MySqlClient;

namespace Parser
{
    class StartData
    {
        public DateTime Date { get; set; }
        public string LocationName { get; set; }
        public string IP { get; set; }
    }
    class FinishData : StartData
    {
        public float Longitude { get; set; }
        public float Latitude { get; set; }
        public int Day { get; set; }
        public int Month { get; set; }
        public int Year { get; set; }
        public int Hour { get; set; }
        public int Minute { get; set; }
        public string DayOfWeek { get; set; }
    }

    class LatLon
    {
        public float latitude  { get; set; }
        public float longitude { get; set; }
    }

    class CheckIP
    {
        public string status { get; set; }
        public string country { get; set; } = "";
        public string city { get; set; } = "";
        public float lat { get; set; } = 0;
        public float lon { get; set; } = 0;
    }


    class Program
    {
        public static List<StartData> data = new List<StartData>();
        public static List<FinishData> fData = new List<FinishData>();
        static void Main(string[] args)
        {
            Console.WriteLine("Program was started");

            // Create file .json, get all data to variable
            DataFileHandler df = new DataFileHandler();
            data = df.SendData();

            // Make finish data list
            FinishDataHandler fd = new FinishDataHandler();
            fData = fd.CreateDataList(data);

            // Add all data to DB
            SQLWorker sw = new SQLWorker();
            sw.AddDataToDB(fData);

            Console.WriteLine("Done");
        }
    }

    class SQLWorker
    {
        public void AddDataToDB(List<FinishData> fData)
        {
            string connStr = "server=mysql.essense.myjino.ru;user=045332084_test;database=essense_parser;port=3306;password=testUser";
            MySqlConnection conn = new MySqlConnection(connStr);
            try
            {
                Console.WriteLine("Connecting to MySQL...");
                conn.Open();

                foreach (var fItem in fData)
                {
                    MySqlCommand cmd = conn.CreateCommand();
                    var tempData = fItem.Date.ToString("yyyy-MM-ddTHH:mm:ss.000Z");
                    cmd.CommandText = $"INSERT INTO ParsedData(LocationName, Longitude, Latitude, Date, Year, Month, Day, Hour, Minute, DayOfWeek, IP) " +
                                      $"VALUES(?LocationName, ?Longitude, ?Latitude, ?tempData, ?Year, ?Month, ?Day, ?Hour, ?Minute, ?DayOfWeek, ?IP);";

                    cmd.Parameters.AddWithValue("?LocationName", fItem.LocationName);
                    cmd.Parameters.AddWithValue("?Longitude", fItem.Longitude);
                    cmd.Parameters.AddWithValue("?Latitude", fItem.Latitude);
                    cmd.Parameters.AddWithValue("?tempData", tempData);
                    cmd.Parameters.AddWithValue("?Year", fItem.Year);
                    cmd.Parameters.AddWithValue("?Month", fItem.Month);
                    cmd.Parameters.AddWithValue("?Day", fItem.Day);
                    cmd.Parameters.AddWithValue("?Hour", fItem.Hour);
                    cmd.Parameters.AddWithValue("?Minute", fItem.Minute);
                    cmd.Parameters.AddWithValue("?DayOfWeek", fItem.DayOfWeek);
                    cmd.Parameters.AddWithValue("?IP", fItem.IP);

                    cmd.ExecuteNonQuery();
                }
                conn.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            conn.Close();
        }
    }

    class DataFileHandler
    {
        public static List<StartData> dataList = new List<StartData>();
        public List<StartData> SendData()
        {
            WriteData().Wait();
            GetData().Wait();
            return dataList;
        }
        static async Task WriteData()
        {
            string filePath = "data.json";
            using (FileStream fs = new FileStream(filePath, FileMode.OpenOrCreate))
            {
                List<StartData> dataList = new List<StartData>();
                dataList.Add(new StartData() { Date = new DateTime(2017, 11, 01, 17, 35, 00), LocationName = "London", IP = "178.62.7.94" });
                dataList.Add(new StartData() { Date = new DateTime(2018, 07, 11, 15, 25, 00), LocationName = "Moscow", IP = "213.167.37.187" });
                dataList.Add(new StartData() { Date = new DateTime(2019, 06, 12, 13, 10, 00), LocationName = "Paris", IP = "146.112.128.150" });
                dataList.Add(new StartData() { Date = new DateTime(2020, 01, 03, 14, 30, 00), LocationName = "Berlin", IP = "193.29.106.84" });
                dataList.Add(new StartData() { Date = new DateTime(2021, 10, 09, 16, 55, 00), LocationName = "Tokyo", IP = "153.246.135.169" });
                dataList.Add(new StartData() { Date = new DateTime(2007, 08, 07, 17, 23, 00), LocationName = "Sydney", IP = "00005445454555" });

                await JsonSerializer.SerializeAsync(fs, dataList);
            }
        }
        static async Task GetData()
        {
            string filePath = "data.json";
            using (FileStream fs = new FileStream(filePath, mode: FileMode.OpenOrCreate))
            {
                dataList = await JsonSerializer.DeserializeAsync<List<StartData>>(fs);
            }
        }
    }

    class FinishDataHandler
    {
        public static List<FinishData> finishData = new List<FinishData>();
        public List<FinishData> CreateDataList(List<StartData> data)
        {
            foreach (var d in data)
            {
                CheckIP ipResult = CheckingIP(d.IP);

                var item = new FinishData()
                {
                    Date = d.Date,
                    LocationName = d.LocationName,
                    Day = d.Date.Day,
                    Month = d.Date.Month,
                    Year = d.Date.Year,
                    Hour = d.Date.Hour,
                    Minute = d.Date.Minute,
                    DayOfWeek = d.Date.DayOfWeek + ""
                };

                if (ipResult.status == "success")
                {
                    item.IP = d.IP;
                    item.Latitude = ipResult.lat;
                    item.Longitude = ipResult.lon;
                }
                else
                {
                    // If we have not valid IP
                    LatLon geoResult = GetCoordinatesByCity(d.LocationName);
                    item.Latitude = geoResult.latitude;
                    item.Longitude = geoResult.longitude;
                }

                finishData.Add(item);
            }
            return finishData;
        }

        public static CheckIP CheckingIP(string IP)
        {
            string ipDataUrl = "http://ip-api.com/json/";
            string ipDataParams = "?fields=status,country,city,lat,lon";

            var client = new RestClient(ipDataUrl + IP + ipDataParams);
            var request = new RestRequest();
            request.AddHeader("Content-Type", "application/json");
            request.Method = Method.GET;

            IRestResponse responce = client.Execute(request);

            var ipResult = JObject.Parse(responce.Content).ToObject<CheckIP>();

            return ipResult;
        }

        public static LatLon GetCoordinatesByCity(string city)
        {
            string apiKey = "ce0527717fd3617e85876e6da808a357";
            string geoDataUrl = "http://api.positionstack.com/v1/forward";

            var geoClient = new RestClient(geoDataUrl + $"?access_key={apiKey}&query={city}");
            var geoRequest = new RestRequest();
            geoRequest.AddHeader("Content-Type", "application/json");
            geoRequest.Method = Method.GET;

            IRestResponse geoResponce = geoClient.Execute(geoRequest);

            var geoResult = JObject.Parse(geoResponce.Content)["data"].FirstOrDefault().ToObject<LatLon>();

            return geoResult;
        }
    }


}
