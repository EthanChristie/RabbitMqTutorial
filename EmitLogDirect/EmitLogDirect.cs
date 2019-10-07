using System;
using System.Linq;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Framing;

namespace EmitLogDirect
{
    public class EmitLogDirect
    {
        private const string ExchangeName = "direct_logs";

        public static void Main(string[] args)
        {
            var factory = new ConnectionFactory { HostName = "localhost" };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(ExchangeName, ExchangeType.Direct);

                var message = GetMessage(args);
                var body = Encoding.UTF8.GetBytes(message);

                var basicProperties = new BasicProperties();
                basicProperties.Timestamp = new AmqpTimestamp(DateTime.Now.Ticks);

                var severity = args.Length > 0 ? args[0] : "info";

                channel.BasicPublish(exchange: ExchangeName,
                    routingKey: severity,
                    basicProperties: basicProperties,
                    body: body
                );

                Console.WriteLine(" [x] Sent {0}", message);
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
        private static string GetMessage(string[] args)
        {
            return args.Length < 1 ? "Hello World!" : string.Join(" ", args.Skip(1).ToArray());
        }
    }
}