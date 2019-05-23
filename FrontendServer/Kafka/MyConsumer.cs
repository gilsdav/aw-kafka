
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;
using test.Hubs;

namespace test.Kafka
{
    public class MyConsumer: BackgroundService
    {
        private Consumer<Ignore, string> consumer;
        private Consumer<Ignore, string> consumerHistory;
        private bool runing;
        private readonly IHubContext<ClickHub, IClickClient> clickClients;

        public MyConsumer(IHubContext<ClickHub, IClickClient> _clickClients)
        {
            this.clickClients = _clickClients;
            var config = new ConsumerConfig
            { 
                GroupId = "test-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetResetType.Earliest
            };
            this.consumer = new Consumer<Ignore, string>(config);
            this.consumerHistory = new Consumer<Ignore, string>(config);
        }

        private void listen()
        {
            Console.WriteLine("Subscribe click-topic");
            this.consumer.Subscribe("click-topic");
            this.runing = true;
            this.consumer.OnError += (_, e) => this.runing = !e.IsFatal;
             while (this.runing)
            {
                try
                {
                    Console.WriteLine("Start Consume click-topic");
                    
                    // this.consumer.Seek(new TopicPartitionOffset(new TopicPartition("click-topic", new Partition()), Offset.Beginning));

                    var cr = this.consumer.Consume();
                    this.clickClients.Clients.All.SendClick(cr.Value);
                    Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error occured: {e.Error.Reason}");
                }
            }
            // Ensure the consumer leaves the group cleanly and final offsets are committed.
            this.stop();
        }

        private void listenhistory()
        {
            Console.WriteLine("Subscribe send-history-topic");
            this.consumerHistory.Subscribe("send-history-topic");
            this.runing = true;
            this.consumerHistory.OnError += (_, e) => this.runing = !e.IsFatal;
             while (this.runing)
            {
                try
                {
                    Console.WriteLine("Start Consume send-history-topic");
                    var cr = this.consumerHistory.Consume();
                    Console.WriteLine($"Consumed send-history message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                    var response = JsonConvert.DeserializeObject<string[]>(cr.Value);
                    if (response.Length > 1) {
                        for (int i = 1; i < response.Length; i++)
                        {
                            this.clickClients.Clients.Client(response[0]).SendClick(response[i]);
                        }
                    }
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Error occured: {e.Error.Reason}");
                }
            }
            // Ensure the consumer leaves the group cleanly and final offsets are committed.
            this.stop();
        }

        private void stop()
        {
            this.runing = false;
            this.consumer.Close();
            this.consumerHistory.Close();
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Console.WriteLine("execute listening");
            // return Task.Run(() => this.listen());
            return Task.WhenAll(
                Task.Run(() => this.listen()),
                Task.Run(() => this.listenhistory())
            );
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("start listening");
            return base.StartAsync(cancellationToken);
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("stop listening");
            this.stop();
            return base.StopAsync(cancellationToken);
        }
        
    }
}
