using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Confluent.Kafka.ConfigPropertyNames;

namespace KafkaManagerConsumer
{
    public class KafkaConsumerManager<TKey, TValue> : IKafkaConsumerManager<TKey, TValue>
    {
        private readonly KafkaConsumerOptions<TKey, TValue> _options;
        private readonly IKafkaMessageHandler<TKey, TValue> _handler;
        private readonly ILogger<KafkaConsumerManager<TKey, TValue>> _logger;

        // lista de workers e suas Tasks
        private readonly List<(KafkaBatchConsumer<TKey, TValue> Consumer, Task RunTask)> _workers =
            [];

        private CancellationTokenSource? _cts;
        private Task? _monitorTask;
        private int _idSeq = 0;

        public KafkaConsumerManager(
            IOptions<KafkaConsumerOptions<TKey, TValue>> options,
            IKafkaMessageHandler<TKey, TValue> handler,
            ILogger<KafkaConsumerManager<TKey, TValue>> logger)
        {
            _options = options.Value;
            _handler = handler;
            _logger = logger;
        }

        public void Start()
        {
            if (_cts != null) throw new InvalidOperationException("Manager já iniciado");

            _cts = new CancellationTokenSource();
            // garante pelo menos 1 consumer inicial
            AddConsumer();
            _monitorTask = Task.Run(() => MonitorLoopAsync(_cts.Token));
            _logger.LogInformation("KafkaConsumerManager iniciado para tópico {Topic}", _options.Topic);
        }

        public async Task StopAsync()
        {
            if (_cts == null) return;
            _cts.Cancel();

            if (_monitorTask != null) await _monitorTask.ConfigureAwait(false);

            // stop all workers
            var copy = _workers.ToList();
            foreach (var (consumer, runTask) in copy)
            {
                try
                {
                    await consumer.StopAsync().ConfigureAwait(false);
                    if (runTask != null) await runTask.ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Erro ao parar worker");
                }
            }

            _workers.Clear();
            _cts = null;
            _logger.LogInformation("KafkaConsumerManager parado");
        }

        private async Task MonitorLoopAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    // total de partições atribuídas atualmente ao grupo (somatório dos workers)
                    var totalAssignedPartitions = _workers.Sum(w => w.Consumer.PartitionCount);
                    var totalWorkers = _workers.Count;

                    _logger.LogDebug("Manager status: workers={Workers}, assignedPartitions={Parts}",
                        totalWorkers, totalAssignedPartitions);

                    // Upscale: se o número de partições atribuídas for maior que o número de workers
                    if (totalAssignedPartitions > totalWorkers && totalWorkers < _options.MaxConsumers)
                    {
                        _logger.LogInformation("Upscale: partições ({Parts}) > workers ({Workers}) — adicionando consumer",
                            totalAssignedPartitions, totalWorkers);
                        AddConsumer();
                    }
                    // Se ainda não houve atribuição (ex: totalAssignedPartitions == 0), podemos tentar esperar.
                    // Não criamos múltiplos consumers imediatamente — deixamos o loop decidir.

                    // Downscale: procurar worker sem partição e que esteja sem partição há mais que idleTimeout
                    if (_workers.Count > 1)
                    {
                        var now = DateTime.UtcNow;
                        var idle = _workers.FirstOrDefault(w =>
                            w.Consumer.PartitionCount == 0 &&
                            (now - w.Consumer.LastPartitionAssigned) > _options.IdleTimeout);

                        if (idle.Consumer != null)
                        {
                            _logger.LogInformation("Downscale: worker inativo sem partição > {Idle}s — removendo",
                                _options.IdleTimeout.TotalSeconds);

                            // para e aguarda sua finalização
                            await idle.Consumer.StopAsync().ConfigureAwait(false);
                            if (idle.RunTask != null) await idle.RunTask.ConfigureAwait(false);

                            _workers.Remove(idle);
                            _logger.LogInformation("Worker removido. Total agora: {Count}", _workers.Count);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Erro no monitor loop do KafkaConsumerManager");
                }

                await Task.Delay(TimeSpan.FromSeconds(5), ct).ConfigureAwait(false);
            }
        }

        private void AddConsumer()
        {
            var id = Interlocked.Increment(ref _idSeq);

            // constroi config novo para esse consumer (chama o ConfigureBuilder se houver)
            var config = new ConsumerConfig();
            config = _options.Config;
            //_options.ConfigureBuilder?.Invoke(config);

            // certifica que há GroupId definido
            if (string.IsNullOrWhiteSpace(config.GroupId))
                throw new InvalidOperationException("ConfigureBuilder deve definir GroupId no ConsumerConfig.");

            var consumer = new KafkaBatchConsumer<TKey, TValue>(
                id,
                _options.Topic,
                config,
                _handler,
                _logger,
                _options.BatchSize,
                _options.BatchTimeout);

            var runTask = consumer.StartAsync(_cts!.Token); // _cts sempre não-nulo aqui porque Start() cria
            _workers.Add((consumer, runTask));
            _logger.LogInformation("Adicionado novo worker id={Id}. Total workers: {Count}", id, _workers.Count);
        }
    }
}
