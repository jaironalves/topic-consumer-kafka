using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaManagerConsumer
{
    public class KafkaManagerBuilder<TKey, TValue>
    {
        private readonly IServiceCollection _services;
        private Action<KafkaConsumerOptions<TKey, TValue>>? _configure;

        public KafkaManagerBuilder(IServiceCollection services)
        {
            _services = services;
        }

        public KafkaManagerBuilder<TKey, TValue> ConfigureBuilder(Action<KafkaConsumerOptions<TKey, TValue>> configure)
        {
            _configure = configure;
            return this;
        }

        public KafkaManagerBuilder<TKey, TValue> WithHandler<TH>() where TH : class, IKafkaMessageHandler<TKey, TValue>
        {
            _services.AddSingleton<IKafkaMessageHandler<TKey, TValue>, TH>();
            return this;
        }

        public void Build()
        {
            _services.Configure(_configure!);

            _services.AddSingleton<ITopicConsumerManager<TKey, TValue>>(sp =>
            {
                var options = sp.GetRequiredService<IOptions<KafkaConsumerOptions<TKey, TValue>>>();
                var handler = sp.GetRequiredService<IKafkaMessageHandler<TKey, TValue>>();
                var logger = sp.GetRequiredService<ILogger<TopicConsumerManager<TKey, TValue>>>();

                return new TopicConsumerManager<TKey, TValue>(
                    options,
                    handler,
                    logger
                );
            });

            //_services.AddHostedService<KafkaConsumerBackgroundService<TKey, TValue>>();
        }
    }
}
