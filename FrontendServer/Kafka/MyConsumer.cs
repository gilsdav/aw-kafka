
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
            // TODO: create an instance of Consumer as "this.consumer"
            // TODO: create an instance of Consumer as "this.consumerHistory"
        }

        private void listen()
        {
            Console.WriteLine("Subscribe click-topic");
            // TODO: Subscribe "click-topic" using "this.consumer"
            this.runing = true;
            this.consumer.OnError += (_, e) => this.runing = !e.IsFatal;
             while (this.runing)
            {
                try
                {
                    Console.WriteLine("Start Consume click-topic");
                    // TODO: consume the consumer and store result in "cr" and replace "bob" by the result
                    this.clickClients.Clients.All.SendClick("bob");
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
            // TODO: Subscribe "send-history-topic" using "this.consumerHistory"
            this.runing = true;
            this.consumerHistory.OnError += (_, e) => this.runing = !e.IsFatal;
             while (this.runing)
            {
                try
                {
                    Console.WriteLine("Start Consume send-history-topic");
                    // TODO: consume the consumerHistory and store result in "cr" and replace "['bob', 'bob']" by the result
                    var response = JsonConvert.DeserializeObject<string[]>("['bob', 'bob']");
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
            // TODO: close all consumers
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Console.WriteLine("execute listening");
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
