import os
import asyncio
import signal
import traceback
from .app import LogCapturerApp
from .metrics import metrics

try:
    import uvloop
    uvloop.install()
except Exception:
    pass

async def main():
    app = LogCapturerApp()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: app.shutdown_event.set())
        except NotImplementedError:
            pass
    metrics_task = None
    try:
        metrics_task = asyncio.create_task(metrics.start_system_metrics())
        await app.setup()
        await app.start()
        await app.shutdown_event.wait()
    except Exception:
        raise
    finally:
        if metrics_task:
            metrics_task.cancel()
            try:
                await metrics_task
            except asyncio.CancelledError:
                pass
        await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
