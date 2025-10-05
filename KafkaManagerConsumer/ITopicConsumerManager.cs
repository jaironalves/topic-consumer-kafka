
namespace KafkaManagerConsumer
{
    public interface ITopicConsumerManager<TKey, TValue>
    {
        Task ExecuteAsync(CancellationToken cancellationToken);
        Task StopAsync(CancellationToken cancellationToken);
    }
}