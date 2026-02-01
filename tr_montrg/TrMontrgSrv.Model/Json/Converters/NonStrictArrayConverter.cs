// https://techiesweb.net/2021/04/24/system_text_json-deserialize-json-array-in-type-and-value-format.html

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.Model.Json.Converters
{
    public sealed class NonStrictArrayConverter<T> : JsonConverter<T> where T : IEnumerable
    {
        public override T Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.StartArray)
            {
                // Proper array, we can deserialize from this token onwards.
                return JsonSerializer.Deserialize<T>(ref reader, options);
            }

            var value = default(T);
            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.StartArray)
                {
                    value = JsonSerializer.Deserialize<T>(ref reader, options);
                }
                if (reader.TokenType == JsonTokenType.EndObject)
                {
                    // finished processing the array and reached the outer closing bracket token of wrapper object.
                    break;
                }
            }

            return value;
        }

        public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
        {
            JsonSerializer.Serialize(writer, value, value.GetType(), options);
        }
    }
}
