using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.DependencyInjection;
using test.Hubs;
using test.Kafka;

namespace test
{
    public class Startup
    {

        private const string CLICK_ROUTING = "click";


        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_2);
            services.AddScoped<MyProducer>();
            services.AddSingleton<Microsoft.Extensions.Hosting.IHostedService, MyConsumer>();
            services.AddSignalR();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            } else
            {
                app.UseHsts();
            }

            // global cors policy
            app.UseCors(x => x
                .WithOrigins("http://localhost:8080")
                .AllowAnyMethod()
                .AllowAnyHeader()
                .AllowCredentials());

            app.UseMvc();

            app.UseSignalR(route => {
                route.MapHub<ClickHub>($"/{CLICK_ROUTING}");
            });

        }
    }
}
