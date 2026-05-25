using FakeExchangeHost.Configuration;
using FakeExchangeHost.Endpoints;

var builder = WebApplication.CreateBuilder(args);
var tickSources = builder.Configuration.GetSection("EndpointsData").Get<List<TickSourceOptions>>() ?? [];
if (tickSources.Count == 0)
{
    tickSources.Add(TickSourceOptions.Default);
}

var urls = tickSources
    .Select(x => $"http://localhost:{x.Port}")
    .Distinct(StringComparer.OrdinalIgnoreCase);

builder.WebHost.UseUrls(urls.ToArray());
var app = builder.Build();

app.UseWebSockets();
foreach (var source in tickSources)
{
    app.MapTickStreamEndpoint(source);
}

app.Run();
