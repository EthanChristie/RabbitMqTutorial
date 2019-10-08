using System;
using System.Linq;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Framing;

namespace EmitLogTopic
{
    public class EmitLogTopic
    {
        private const string ExchangeName = "topic_logs";

        public static void Main(string[] args)
        {
            var factory = new ConnectionFactory { HostName = "localhost" };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(ExchangeName, ExchangeType.Topic);

                var message = GetMessage(args);
                var routingKey = args.Length > 0 ? args[0] : "anonymous.info";

                var body = Encoding.UTF8.GetBytes(message);

                var basicProperties = new BasicProperties
                {
                    Timestamp = new AmqpTimestamp(DateTime.Now.Ticks)
                };


                channel.BasicPublish(exchange: ExchangeName,
                    routingKey: routingKey,
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