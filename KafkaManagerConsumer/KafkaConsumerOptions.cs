using Confluent.Kafka;

namespace KafkaManagerConsumer
{
    public class KafkaConsumerOptions<TKey, TValue>
    {
        public string Topic { get; set; } = default!;
        public ConsumerConfig? Config { get; set; }
        public int MaxConsumers { get; set; } = 4;
        public int BatchSize { get; set; } = 100;
        public TimeSpan BatchTimeout { get; set; } = TimeSpan.FromSeconds(2);
        public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromSeconds(30);
    }
}
