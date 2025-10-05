using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        private CancellationTokenSource? _cts;
        private Task? _runningTask;

        // Estado visível ao manager
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
        public Task StartAsync(CancellationToken managerToken)
        {
            if (_runningTask != null) return _runningTask;

            _cts = CancellationTokenSource.CreateLinkedTokenSource(managerToken);
            _runningTask = Task.Run(() => RunLoopAsync(_cts.Token), CancellationToken.None);
            return _runningTask;
        }

        // O manager chama StopAsync() para parar esse consumer
        public async Task StopAsync()
        {
            try
            {
                if (_cts == null) return;
                _cts.Cancel();
                if (_runningTask != null) await _runningTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Erro ao parar consumer {Id}", _id);
            }
        }

        private async Task RunLoopAsync(CancellationToken ct)
        {
            var builder = new ConsumerBuilder<TKey, TValue>(_config)
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    PartitionCount = partitions?.Count ?? 0;
                    LastPartitionAssigned = DateTime.UtcNow;
                    _logger.LogDebug("[C{Id}] Partitions assigned: {Count}", _id, PartitionCount);
                    //return partitions;
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    PartitionCount = 0;
                    _logger.LogDebug("[C{Id}] Partitions revoked", _id);
                });

            using var consumer = builder.Build();
            consumer.Subscribe(_topic);

            var buffer = new List<ConsumeResult<TKey, TValue>>(_batchSize);
            DateTime? bufferFirstEnqueue = null;

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    ConsumeResult<TKey, TValue>? result = null;
                    try
                    {
                        // bloqueia até batchTimeout
                        result = consumer.Consume(_batchTimeout);
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogWarning(ex, "[C{Id}] ConsumeException", _id);
                    }

                    if (result != null)
                    {
                        buffer.Add(result);
                        bufferFirstEnqueue ??= DateTime.UtcNow;
                    }

                    // flush by size
                    if (buffer.Count >= _batchSize)
                    {
                        await DispatchBatch(buffer, ct).ConfigureAwait(false);
                        bufferFirstEnqueue = null;
                        buffer.Clear();
                    }
                    else if (buffer.Count > 0 && bufferFirstEnqueue.HasValue
                             && DateTime.UtcNow - bufferFirstEnqueue.Value >= _batchTimeout)
                    {
                        // flush by timeout
                        await DispatchBatch(buffer, ct).ConfigureAwait(false);
                        bufferFirstEnqueue = null;
                        buffer.Clear();
                    }

                    // continue loop
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
