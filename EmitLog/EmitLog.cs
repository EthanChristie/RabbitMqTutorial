using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Framing;

namespace EmitLog
{
    public class EmitLog
    {
        public static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare("logs", ExchangeType.Fanout);

                var message = GetMessage(args);
                var body = Encoding.UTF8.GetBytes(message);

                var basicProperties = new BasicProperties();
                basicProperties.Timestamp = new AmqpTimestamp(DateTime.Now.Ticks);

                channel.BasicPublish(exchange: "logs",
                    routingKey: "",
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
            return args.Length == 0 ? "Hello World!" : string.Join(" ", args);
        }
    }
}
