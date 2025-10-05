using Confluent.Kafka;
using KafkaManagerConsumer;
using KafkaManagerConsumer.Worker;

var builder = Host.CreateApplicationBuilder(args);


builder.Services.AddKafkaConsumer<string, string>(kafka =>
{
    kafka.ConfigureBuilder(options =>
    {
        options.Topic = "kafka-manager";
        options.Config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "kafka-manager-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        options.MaxConsumers = 9;
        options.BatchSize = 50;
        options.BatchTimeout = TimeSpan.FromSeconds(2);
        options.IdleTimeout = TimeSpan.FromSeconds(30);
    })
    .WithHandler<MessageHandler>();
});

builder.Services.AddHostedService<KafkaConsumerBackgroundService<string, string>>();


var host = builder.Build();
host.Run();

public class MessageHandler : IKafkaMessageHandler<string, string>
{
    public Task HandleAsync(IReadOnlyList<Confluent.Kafka.ConsumeResult<string, string>> messages, CancellationToken ct)
    {
        foreach (var msg in messages)
        {
            Console.WriteLine($"Mensagem recebida: {msg.Message.Key} - {msg.Message.Value}");
        }
        return Task.CompletedTask;
    }
}