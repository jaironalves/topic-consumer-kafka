namespace KafkaManagerConsumer.Worker
{
    public class KafkaConsumerBackgroundService<TKey, TValue> : BackgroundService
    {
        private readonly ITopicConsumerManager<TKey, TValue> _manager;

        public KafkaConsumerBackgroundService(ITopicConsumerManager<TKey, TValue> manager)
        {
            _manager = manager;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await _manager.ExecuteAsync(stoppingToken);            
        }
    }
}
