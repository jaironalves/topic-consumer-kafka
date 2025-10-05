using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaManagerConsumer
{
    public interface IKafkaMessageHandler<TKey, TValue>
    {
        Task HandleAsync(IReadOnlyList<ConsumeResult<TKey, TValue>> messages, CancellationToken ct);
    }
}
