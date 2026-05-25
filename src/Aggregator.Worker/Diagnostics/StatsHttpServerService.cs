using System.Net;
using System.Text;
using System.Text.Json;

namespace Aggregator.Worker.Diagnostics;

public sealed class StatsHttpServerService : BackgroundService
{
    private readonly ProcessingStats _stats;
    private readonly ILogger<StatsHttpServerService> _logger;
    private readonly HttpListener _listener = new();

    public StatsHttpServerService(ProcessingStats stats, ILogger<StatsHttpServerService> logger)
    {
        _stats = stats;
        _logger = logger;
        _listener.Prefixes.Add("http://localhost:5180/");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _listener.Start();
        _logger.LogInformation("Stats endpoint listening on http://localhost:5180/debug/stats");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var contextTask = _listener.GetContextAsync();
                var completed = await Task.WhenAny(contextTask, Task.Delay(Timeout.Infinite, stoppingToken));
                if (completed != contextTask)
                {
                    break;
                }

                var context = contextTask.Result;
                await HandleRequestAsync(context, stoppingToken);
            }
        }
        catch (HttpListenerException) when (stoppingToken.IsCancellationRequested)
        {
            // Listener stopped during shutdown.
        }
        finally
        {
            if (_listener.IsListening)
            {
                _listener.Stop();
            }
            _listener.Close();
        }
    }

    private async Task HandleRequestAsync(HttpListenerContext context, CancellationToken cancellationToken)
    {
        if (!string.Equals(context.Request.HttpMethod, "GET", StringComparison.OrdinalIgnoreCase))
        {
            context.Response.StatusCode = (int)HttpStatusCode.MethodNotAllowed;
            context.Response.Close();
            return;
        }

        if (!string.Equals(context.Request.Url?.AbsolutePath, "/debug/stats", StringComparison.Ordinal))
        {
            context.Response.StatusCode = (int)HttpStatusCode.NotFound;
            context.Response.Close();
            return;
        }

        var snapshot = _stats.Snapshot();
        var payload = JsonSerializer.Serialize(snapshot);
        var bytes = Encoding.UTF8.GetBytes(payload);

        context.Response.StatusCode = (int)HttpStatusCode.OK;
        context.Response.ContentType = "application/json";
        context.Response.ContentEncoding = Encoding.UTF8;
        context.Response.ContentLength64 = bytes.LongLength;
        await context.Response.OutputStream.WriteAsync(bytes, cancellationToken);
        context.Response.Close();
    }
}
