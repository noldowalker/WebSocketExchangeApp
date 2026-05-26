using FakeExchangeHost.Configuration;
using FakeExchangeHost.Endpoints;

var builder = WebApplication.CreateBuilder(args);
var endpointsData = builder.Configuration.GetSection("EndpointsData").Get<List<EndpointDataOptions>>() ?? [];
if (endpointsData.Count == 0)
{
    endpointsData.Add(new EndpointDataOptions
    {
        Name = "default-socket-value",
        Port = 5000,
        Resource = "/ws/default-socket-value",
        IntervalMs = 1000,
        PayloadJsonResourceName = "binance-ticker.json"
    });
}

var payloadsPath = Path.Combine(builder.Environment.ContentRootPath, "Payloads");
var tickSources = endpointsData.Select(x => BuildTickSource(x, payloadsPath)).ToList();

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

static TickSourceOptions BuildTickSource(EndpointDataOptions endpoint, string payloadsPath)
{
    if (string.IsNullOrWhiteSpace(endpoint.PayloadJsonResourceName))
    {
        throw new InvalidOperationException(
            $"PayloadJsonResourceName is required for endpoint '{endpoint.Name}'.");
    }

    var payloadFilePath = Path.Combine(payloadsPath, endpoint.PayloadJsonResourceName);
    if (!File.Exists(payloadFilePath))
    {
        throw new FileNotFoundException(
            $"Payload resource file not found for endpoint '{endpoint.Name}'.",
            payloadFilePath);
    }

    var payloadJson = File.ReadAllText(payloadFilePath);
    if (string.IsNullOrWhiteSpace(payloadJson))
    {
        throw new InvalidOperationException(
            $"Payload resource file is empty for endpoint '{endpoint.Name}': {payloadFilePath}");
    }

    return new TickSourceOptions
    {
        Name = endpoint.Name,
        Port = endpoint.Port,
        Resource = endpoint.Resource,
        IntervalMs = endpoint.IntervalMs,
        PayloadJson = payloadJson
    };
}
