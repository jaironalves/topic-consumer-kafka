using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaManagerConsumer
{
    public static class KafkaServiceCollectionExtensions
    {
        public static IServiceCollection AddKafkaConsumer<TKey, TValue>(
            this IServiceCollection services,
            Action<KafkaManagerBuilder<TKey, TValue>> configure)
        {
            var builder = new KafkaManagerBuilder<TKey, TValue>(services);
            configure(builder);
            builder.Build();
            return services;
        }
    }
}
