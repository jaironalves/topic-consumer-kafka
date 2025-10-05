using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace KafkaManagerConsumer
{
    public class TopicConsumer<TKey, TValue>
    {
        private readonly int _id;
        private readonly string _topic;
        private readonly ConsumerConfig _config;
        private readonly IKafkaMessageHandler<TKey, TValue> _handler;
        private readonly ILogger _logger;
        private readonly int _batchSize;
        private readonly TimeSpan _batchTimeout;

        private Task? _consumerTask;
        private CancellationTokenSource? _stoppingCts;

        public virtual Task? ConsumerTask => _consumerTask;

        public bool Actived { get; private set; } = false;
        public bool FirstConsumeFailed { get; private set; } = false;
        public int PartitionCount { get; private set; }
        public DateTime LastPartitionAssigned { get; private set; } = DateTime.UtcNow;

        public TopicConsumer(
            int id,
            string topic,
            ConsumerConfig config,
            IKafkaMessageHandler<TKey, TValue> handler,
            ILogger logger,
            int batchSize = 100,
            TimeSpan? batchTimeout = null)
        {
            _id = id;
            _topic = topic;
            _config = config;
            _handler = handler;
            _logger = logger;
            _batchSize = batchSize;
            _batchTimeout = batchTimeout ?? TimeSpan.FromSeconds(2);
        }

        // Inicia o loop e devolve a Task para o manager poder aguardar se quiser
        public Task StartAsync(CancellationToken cancellationToken)
        {
            if (_consumerTask != null)
                return _consumerTask;

            _stoppingCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _consumerTask = RunLoopAsync(_stoppingCts.Token);

            if (_consumerTask.IsCompleted)
            {
                return _consumerTask;
            }

            return Task.CompletedTask;
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            // Stop called without start
            if (_consumerTask == null)
            {
                return;
            }

            try
            {
                // Signal cancellation to the executing method
                _stoppingCts!.Cancel();
            }
            finally
            {
                // Wait until the task completes or the stop token triggers
                var tcs = new TaskCompletionSource<object>();
                using CancellationTokenRegistration registration = cancellationToken.Register(s => ((TaskCompletionSource<object>)s!).SetCanceled(), tcs);
                // Do not await the _consumerTask because cancelling it will throw an OperationCanceledException which we are explicitly ignoring
                await Task.WhenAny(_consumerTask, tcs.Task).ConfigureAwait(false);
            }
        }

        private Task<ConsumeResult<TKey, TValue>> ConsumeAsync(IConsumer<TKey, TValue> consumer)
        {
            var tcs = new TaskCompletionSource<ConsumeResult<TKey, TValue>>();
            Task.Run(() =>
            {
                try
                {
                    var result = consumer.Consume(_batchTimeout);
                    tcs.SetResult(result);
                }
                catch (Exception ex)
                {
                    tcs.SetException(ex);
                }
            });
            return tcs.Task;
        }

        private async Task<List<ConsumeResult<TKey, TValue>>> BatchConsumeAsync(IConsumer<TKey, TValue> consumer)
        {
            var consumerResults = new List<ConsumeResult<TKey, TValue>>();

            for (int i = 0; i < _batchSize; i++)
            {
                try
                {
                    var result = await ConsumeAsync(consumer);

                    if (!Actived)
                        Actived = (result is not null) || !FirstConsumeFailed;

                    if (result is null)
                    {
                        if (!Actived)
                            _logger.LogWarning(_id, "[C{Id}] Consume not actived", _id);

                        break;
                    }

                    consumerResults.Add(result);
                }
                catch (ConsumeException ex)
                {
                    if (!Actived && !FirstConsumeFailed)
                    {
                        FirstConsumeFailed = true;
                        _logger.LogError(ex, "[C{Id}] First ConsumeException (will not retry)", _id);
                    }
                }
            }

            return consumerResults;
        }

        private async Task RunLoopAsync(CancellationToken ct)
        {
            var builder = new ConsumerBuilder<TKey, TValue>(_config)
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    PartitionCount = partitions?.Count ?? 0;
                    LastPartitionAssigned = DateTime.UtcNow;
                    _logger.LogInformation("[C{Id}] Partitions assigned: {Count}", _id, PartitionCount);                    
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    PartitionCount = 0;
                    _logger.LogInformation("[C{Id}] Partitions revoked", _id);
                });

            using var consumer = builder.Build();
            consumer.Subscribe(_topic);
                        
            try
            {
                while (!ct.IsCancellationRequested)
                {
                   var consumerResults = await BatchConsumeAsync(consumer)
                        .ConfigureAwait(false);

                    if (!ct.IsCancellationRequested && consumerResults.Count > 0)
                    {
                        await DispatchBatch(consumerResults, ct);
                    }                    
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[C{Id}] Erro inesperado no loop do consumer", _id);
            }
            finally
            {
                try
                {
                    consumer.Close();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "[C{Id}] Erro ao fechar consumer", _id);
                }
            }
        }

        private async Task DispatchBatch(List<ConsumeResult<TKey, TValue>> batch, CancellationToken ct)
        {
            try
            {
                await _handler.HandleAsync(batch, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[C{Id}] Erro no handler ao processar batch (size={Size})", _id, batch.Count);
                // aqui você pode aplicar retry/backoff/registro para reprocessar, se quiser
            }
        }
    }
}
