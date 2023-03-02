using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace ConsoleApp1;

public class Consumer
{
    private readonly IConsumer<Null, string> _consumer;
    private readonly ILogger<Consumer> _logger;

    public Consumer()
    {
        ConsumerConfig config = new()
        {
            GroupId = "bug-day",
            BootstrapServers = "localhost:29092",

            // read messages from start if no commit exists
            AutoOffsetReset = AutoOffsetReset.Earliest,

            // at least once delivery semantics
            // https://docs.confluent.io/kafka-clients/dotnet/current/overview.html#store-offsets
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 5000,
            // disable auto-store of offsets
            EnableAutoOffsetStore = false
        };

        _consumer = new ConsumerBuilder<Null, string>(config).Build();
        _logger = LoggerFactory.Create(x => x.AddConsole()).CreateLogger<Consumer>();
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        _consumer.Subscribe(new[] { "A", "B" });

        var consumeResult = new ConsumeResult<Null, string>();
        var message = string.Empty;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                consumeResult = _consumer.Consume(cancellationToken);

                if (consumeResult.IsPartitionEOF)
                {
                    // reached end of topic
                    continue;
                }

                message = consumeResult.Message == null ? string.Empty : consumeResult.Message.Value;

                await HandleMessageAsync(message);
            }
            catch (ConsumeException ex)
            {
                var error = ex.Error.ToString();
                if (error.StartsWith("Subscribed topic not available"))
                {
                    // ignore topic not exists exception
                    // because we currently set `auto.create.topics.enable=true` on broker
                    // ref: https://kafka.apache.org/documentation/#brokerconfigs_auto.create.topics.enable
                    _logger.LogWarning(error);
                    continue;
                }

                _logger.LogError("Failed consume message: {Message}. Error: {Error}", message, error);

                if (ex.Error.IsFatal)
                {
                    // https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#fatal-consumer-errors
                    break;
                }
            }
            catch (OperationCanceledException)
            {
                // ignore
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Exception occurred when consume message: {Message}.", message);
            }
            finally
            {
                try
                {
                    // store offset manually
                    _consumer.StoreOffset(consumeResult);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Exception occurred when store offset.");
                }
            }
        }
    }

    private static async Task HandleMessageAsync(string message)
    {
        Console.WriteLine($"Receive message: {message} At {DateTime.Now}");

        await Task.CompletedTask;
    }
}