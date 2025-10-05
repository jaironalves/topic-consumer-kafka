namespace KafkaManagerConsumer.Worker
{
    public class KafkaConsumerBackgroundService<TKey, TValue> : BackgroundService
    {
        private readonly IKafkaConsumerManager<TKey, TValue> _manager;

        public KafkaConsumerBackgroundService(IKafkaConsumerManager<TKey, TValue> manager)
        {
            _manager = manager;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _manager.Start();
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            await _manager.StopAsync();
        }

    }
}
