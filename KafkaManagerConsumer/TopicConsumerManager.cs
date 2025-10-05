using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaManagerConsumer
{
    public class TopicConsumerManager<TKey, TValue> : ITopicConsumerManager<TKey, TValue>
    {
        private Task? _monitorTask;
        private CancellationTokenSource? _stoppingCts;

        private readonly KafkaConsumerOptions<TKey, TValue> _options;
        private readonly IKafkaMessageHandler<TKey, TValue> _handler;
        private readonly ILogger<TopicConsumerManager<TKey, TValue>> _logger;

        // lista de workers e suas Tasks
        private readonly List<(TopicConsumer<TKey, TValue> Consumer, Task RunTask)> _consumers =
            [];

        private CancellationTokenSource? _cts;
        
        private int _idSeq = 0;

        public TopicConsumerManager(
            IOptions<KafkaConsumerOptions<TKey, TValue>> options,
            IKafkaMessageHandler<TKey, TValue> handler,
            ILogger<TopicConsumerManager<TKey, TValue>> logger)
        {
            _options = options.Value;
            _handler = handler;
            _logger = logger;
        }

        public Task ExecuteAsync(CancellationToken cancellationToken)
        {            
            AddConsumer(cancellationToken);
            return MonitorLoopAsync(cancellationToken);

            _monitorTask = Task.Run(() => MonitorLoopAsync(_cts.Token));
            _logger.LogInformation("KafkaConsumerManager iniciado para tópico {Topic}", _options.Topic);
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            //if (_cts == null) return;
            //_cts.Cancel();

            //if (_monitorTask != null) await _monitorTask.ConfigureAwait(false);

            // stop all workers
            var copy = _consumers.ToList();
            foreach (var (consumer, runTask) in copy)
            {
                try
                {
                    await consumer.StopAsync(cancellationToken).ConfigureAwait(false);
                    if (runTask != null) await runTask.ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Erro ao parar worker");
                }
            }

            _consumers.Clear();
            _cts = null;
            _logger.LogInformation("KafkaConsumerManager parado");
        }

        private async Task MonitorLoopAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    // total de partições atribuídas atualmente ao grupo (somatório dos workers)
                    var totalAssignedPartitions = _consumers.Sum(w => w.Consumer.PartitionCount);
                    var totalWorkers = _consumers.Count;

                    _logger.LogInformation("Manager status: workers={Workers}, assignedPartitions={Parts}",
                        totalWorkers, totalAssignedPartitions);

                    // Upscale: se o número de partições atribuídas for maior que o número de workers
                    if (totalAssignedPartitions > totalWorkers && totalWorkers < _options.MaxConsumers)
                    {
                        _logger.LogInformation("Upscale: partições ({Parts}) > workers ({Workers}) — adicionando consumer",
                            totalAssignedPartitions, totalWorkers);
                        AddConsumer(cancellationToken);
                    }
                    // Se ainda não houve atribuição (ex: totalAssignedPartitions == 0), podemos tentar esperar.
                    // Não criamos múltiplos consumers imediatamente — deixamos o loop decidir.

                    // Downscale: procurar worker sem partição e que esteja sem partição há mais que idleTimeout
                    if (_consumers.Count > 1)
                    {
                        var now = DateTime.UtcNow;
                        var idle = _consumers.FirstOrDefault(w =>
                            w.Consumer.PartitionCount == 0 &&
                            (now - w.Consumer.LastPartitionAssigned) > _options.IdleTimeout);

                        if (idle.Consumer != null)
                        {
                            _logger.LogInformation("Downscale: worker inativo sem partição > {Idle}s — removendo",
                                _options.IdleTimeout.TotalSeconds);

                            // para e aguarda sua finalização
                            await idle.Consumer.StopAsync(cancellationToken).ConfigureAwait(false);
                            if (idle.RunTask != null) await idle.RunTask.ConfigureAwait(false);

                            _consumers.Remove(idle);
                            _logger.LogInformation("Worker removido. Total agora: {Count}", _consumers.Count);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Erro no monitor loop do KafkaConsumerManager");
                }

                await Task.Delay(TimeSpan.FromSeconds(20), cancellationToken).ConfigureAwait(false);
            }
        }

        private void AddConsumer(CancellationToken cancellationToken)
        {
            var id = Interlocked.Increment(ref _idSeq);

            // constroi config novo para esse consumer (chama o ConfigureBuilder se houver)
            var config = new ConsumerConfig();
            config = _options.Config;
            //_options.ConfigureBuilder?.Invoke(config);

            // certifica que há GroupId definido
            if (string.IsNullOrWhiteSpace(config.GroupId))
                throw new InvalidOperationException("ConfigureBuilder deve definir GroupId no ConsumerConfig.");

            var consumer = new TopicConsumer<TKey, TValue>(
                id,
                _options.Topic,
                config,
                _handler,
                _logger,
                _options.BatchSize,
                _options.BatchTimeout);

            var runTask = consumer.StartAsync(cancellationToken); // _cts sempre não-nulo aqui porque Start() cria
            _consumers.Add((consumer, runTask));
            _logger.LogInformation("Adicionado novo worker id={Id}. Total workers: {Count}", id, _consumers.Count);
        }
    }
}
