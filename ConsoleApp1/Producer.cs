using System.Net;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace ConsoleApp1;

public class Producer
{
    private readonly ILogger<Producer> _logger;
    private readonly IProducer<Null, string> _producer;

    public Producer()
    {
        ProducerConfig config = new()
        {
            BootstrapServers = "localhost:29092",
            ClientId = Dns.GetHostName()
        };

        _producer = new ProducerBuilder<Null, string>(config).Build();
        _logger = LoggerFactory.Create(x => x.AddConsole()).CreateLogger<Producer>();
    }

    public Task ProduceAsync(
        string topic,
        string value,
        CancellationToken cancellationToken = default)
    {
        try
        {
            // for high throughput processing, we use Produce method, which is also asynchronous, in that it never blocks.
            // https://docs.confluent.io/kafka-clients/dotnet/current/overview.html#producer
            _producer.Produce(topic, new Message<Null, string>
            {
                Value = value
            }, DeliveryHandler);

            void DeliveryHandler(DeliveryReport<Null, string> report)
            {
                if (report.Error.IsError)
                {
                    _logger.LogError(
                        "Error occurred when delivery message. Topic: {Topic}, Value: {Value}, Error: {Error}",
                        topic, value, report.Error.ToString()
                    );
                }
            }
        }
        catch (ProduceException<Null, string> ex)
        {
            var deliveryResult = ex.DeliveryResult;

            _logger.LogError(
                "Error occurred when delivery message. Topic: {Topic}, Value: {Value}, Error: {Error}",
                topic, value, ex.Error.ToString()
            );
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Exception occurred when publish message.");
        }

        return Task.CompletedTask;
    }
}