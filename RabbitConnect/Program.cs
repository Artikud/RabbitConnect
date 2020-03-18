using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using EasyNetQ;
using Newtonsoft.Json;
using JsonSerializer = Newtonsoft.Json.JsonSerializer;

namespace RabbitConnect
{
    class Program
    {
        static async Task Main()
        {
            var bus = RabbitHutch.CreateBus("host=hostadress;username=user;password=pwd;prefetchcount=25");

            var exchange = await bus.Advanced.ExchangeDeclareAsync("Test_echange_1", "topic");
            var queue = bus.Advanced.QueueDeclare("test_queue_1", autoDelete: true, durable: true, exclusive: true);
            bus.Advanced.Bind(exchange, queue, "*");

            bus.Advanced.Consume(queue, (body, props, info) =>
            {
                var receivedMessage = Encoding.UTF8.GetString(body);
                Console.WriteLine($"Message received {receivedMessage}");
                Console.ReadLine();
            });

            var serializer = new JsonSerializer();

            const string message = "qwerty";
            byte[] byteMessage;

            await using (var stream = new MemoryStream())
            {
                await using var streamWriter = new StreamWriter(stream);
                using var jsonWriter = new JsonTextWriter(streamWriter);
                serializer.Serialize(jsonWriter, message);
                streamWriter.Flush();
                byteMessage = stream.ToArray();
            }

            await bus.Advanced.PublishAsync(exchange, "routingKey", false, new MessageProperties(), byteMessage);
        }
    }
}
