using System;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ReceiveLog
{
    public class ReceiveLog
    {
        public static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare("logs", ExchangeType.Fanout);
                var queueName = channel.QueueDeclare().QueueName;
                channel.QueueBind(
                    queue: queueName,
                    exchange: "logs",
                    routingKey: ""
                );

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);

                    var timeStamp = new DateTime(ea.BasicProperties.Timestamp.UnixTime);

                    Console.WriteLine($" [{timeStamp}]: Received {message}");

                    Console.WriteLine(" [x] Done");
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
