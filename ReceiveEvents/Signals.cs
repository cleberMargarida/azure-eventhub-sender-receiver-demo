using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;

namespace Ingestion.Api.Dto
{
    public class Signals
    {
        public Signals()
        {
            this.Items = new Collection<SignalDataDto>();
        }

        [JsonProperty("signals")]
        public Collection<SignalDataDto> Items { get; set; }

        [JsonIgnore]
        public string DeviceId { get; set; }
    }

    public class SignalDataDto
    {
        private Guid? _hashCode;

        [JsonProperty("deviceId")]
        public string DeviceId { get; set; }
        [JsonProperty("id")]
        public string SignalName { get; set; }
        [JsonProperty("timestamp")]
        public long TimeStamp { get; set; }
        [JsonProperty("value")]
        public string Value { get; set; }
        [JsonProperty("keyValue")]
        public List<KeyValue> KeyValue { get; set; }

        [JsonIgnore]
        public DateTime DateTimeUTC => DateTime.UnixEpoch.AddMilliseconds(TimeStamp);

        [JsonIgnore]
        public Guid HashCode => _hashCode ??= Guid.Parse(GetMd5HashCode());

        public SignalDataDto()
        {
            this.KeyValue = new List<KeyValue>();
        }

        public SignalDataDto(string deviceId, string signalName, long timeStamp, string value)
        {
            this.DeviceId = deviceId;
            this.SignalName = signalName;
            this.TimeStamp = timeStamp;
            this.Value = value;
        }

        private string GetMd5HashCode()
        {
            using var md5 = System.Security.Cryptography.MD5.Create();
            var input = this.SignalName + this.TimeStamp + this.DeviceId + (this.Value ?? JsonConvert.SerializeObject(KeyValue) ?? "");
            byte[] inputBytes = Encoding.UTF8.GetBytes(input);
            byte[] hashBytes = md5.ComputeHash(inputBytes);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < hashBytes.Length; i++)
            {
                sb.Append(hashBytes[i].ToString("x2"));
            }
            return sb.ToString();
        }
    }

    public class KeyValue
    {
        [JsonProperty("key")]
        public long Key { get; set; }

        [JsonProperty("value")]
        public float Value { get; set; }
    }
}

