using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Microsoft.AspNetCore.WebUtilities;

namespace ScrapeTsting
{
    public class Program
    {
        public class OhlcData
        {
            public string? Pair { get; set; }
            public string? Timestamp { get; set; }
            public string? Open { get; set; }
            public string? High { get; set; }
            public string? Low { get; set; }
            public string? Close { get; set; }
            public string? Volume { get; set; }
        }

        public class OhlcResponse
        {
            public string? Name { get; set; }
            public string? Period { get; set; }
            public string? Description { get; set; }

            public string? Pair { get; set; }

            public List<OhlcData>? Ohlc { get; set; }
        }

        public class DataResponse
        {
            public OhlcResponse? Data { get; set; }
        }

        public static async Task Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appSettings.json", optional: true, reloadOnChange: true);

            var configuration = builder.Build();
            var historicalDataFolderPath = configuration["AppSettings:HistoricalDataFolderPath"];

            string currencyPair = "btceur";
            string url = $"https://www.bitstamp.net/api/v2/ohlc/{currencyPair}/";

            DateTime start = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            DateTime end = new DateTime(2020, 1, 1, 23, 59, 59, DateTimeKind.Utc);

            List<int> dates = new List<int>();

            for (var dt = start; dt <= end; dt = dt.AddHours(1))
            {
                var unixTime = (int)Math.Round((dt - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds);
                dates.Add(unixTime);
            }

            List<OhlcData> masterData = new List<OhlcData>();

            using (var client = new HttpClient())
            {
                for (int i = 0; i < dates.Count - 1; i++)
                {
                    var first = dates[i];
                    var last = dates[i + 1];

                    var parameters = new Dictionary<string, string>
                {
                    {"step", "3600"},
                    {"limit", "10"},
                    {"start", first.ToString()},
                    {"end", last.ToString()}
                };

                    var requestUrl = QueryHelpers.AddQueryString(url, parameters);
                    var response = await client.GetAsync(requestUrl);

                    if (!response.IsSuccessStatusCode)
                    {
                        Console.WriteLine($"Failed to get data for {first}-{last}: {response.StatusCode}");
                        continue;
                    }

                    var data = await response.Content.ReadAsStringAsync();

                    Console.WriteLine($"{data}");

                    var dataResponse = JsonConvert.DeserializeObject<DataResponse>(data);
                    var ohlcResponse = dataResponse?.Data;


                    if (ohlcResponse?.Ohlc == null)
                    {
                        Console.WriteLine($"No data for {first}-{last}");
                        continue;
                    }

                    foreach (var ohlcData in ohlcResponse.Ohlc)
                    {
                        ohlcData.Pair = ohlcResponse.Pair;
                    }
                    masterData.AddRange(ohlcResponse.Ohlc);
                }
            }
            Console.WriteLine("Name For the File");
            var historicalDatacsv = Console.ReadLine();
            var path = Path.Combine(historicalDataFolderPath, historicalDatacsv + ".csv");

            using (var writer = new StreamWriter(path))
            {
                writer.WriteLine("Pair,Timestamp,Open,High,Low,Close,Volume");

                foreach (var data in masterData)
                {
                    writer.WriteLine($"{data.Pair},{data.Timestamp},{data.Open},{data.High},{data.Low},{data.Close},{data.Volume}");
                }
            }

        }
    }
}
      

       
