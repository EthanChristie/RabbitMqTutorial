using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ReceiveLogTopic
{
    public class ReceiveLogTopic
    {
        private const string ExchangeName = "topic_logs";

        public static void Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.Error.WriteLine("Usage: {0} [binding_key...]", Environment.GetCommandLineArgs()[0]);
                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
                Environment.ExitCode = 1;
                return;
            }

            var factory = new ConnectionFactory { HostName = "localhost" };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(ExchangeName, ExchangeType.Topic);
                var queueName = channel.QueueDeclare().QueueName;


                foreach (var bindingKey in args)
                {
                    channel.QueueBind(
                        queue: queueName,
                        exchange: ExchangeName,
                        routingKey: bindingKey
                    );
                }

                Console.WriteLine(" [*] Waiting for messages");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);

                    var timeStamp = new DateTime(ea.BasicProperties.Timestamp.UnixTime);

                    Console.WriteLine($"[{timeStamp}] [{ea.RoutingKey}] Received");
                    Console.WriteLine($"    {message}");
                };

                channel.BasicConsume(
                    queue: queueName,
                    autoAck: true,
                    consumer: consumer
                );

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }
    }
}