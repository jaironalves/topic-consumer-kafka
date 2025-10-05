
namespace KafkaManagerConsumer
{
    public interface IKafkaConsumerManager<TKey, TValue>
    {
        void Start();
        Task StopAsync();
    }
}