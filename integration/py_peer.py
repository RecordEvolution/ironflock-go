"""Python SDK peer for the Go SDK's live interop tests.

Modes (results are printed on a line starting with "RESULT:" because the
Python SDK itself prints to stdout):
  call <device_key> <topic>   call a device function, print the result as JSON
  serve <topic>               register an echo device function, print READY,
                              serve until stdin closes
  publish_table <table>       publish one row and one bulk batch (with tsp) to a table
"""

import asyncio
import json
import os
import sys

from ironflock import IronFlock

URL = os.environ.get("IRONFLOCK_TEST_PLATFORM_URL", "ws://localhost:18082/ws-ua-usr")


async def main():
    mode = sys.argv[1]
    ifl = IronFlock(ironFlockUrl=URL, reconnect_window=10)
    await ifl.start()
    try:
        if mode == "call":
            result = await ifl.call_device_function(int(sys.argv[2]), sys.argv[3],
                                                    args=[1, "two"], kwargs={"k": True})
            print("RESULT:" + json.dumps(result), flush=True)
        elif mode == "serve":
            def echo(*args, **kwargs):
                return {"args": list(args), "kwargs": kwargs, "from": "python"}

            await ifl.register_device_function(sys.argv[2], echo)
            print("READY", flush=True)
            await asyncio.get_running_loop().run_in_executor(None, sys.stdin.read)
        elif mode == "publish_table":
            table = sys.argv[2]
            # fleetdb drops a row without tsp.
            await ifl.publish_to_table(table, {"tsp": "2026-01-01T00:00:01Z", "temperature": 1.25, "source": "python"})
            await ifl.publish_rows_to_table(table, [
                {"tsp": "2026-01-01T00:00:02Z", "temperature": 2.5, "source": "python-bulk"},
                {"tsp": "2026-01-01T00:00:03Z", "temperature": 3.75, "source": "python-bulk"}])
            print("RESULT:true", flush=True)
        else:
            raise SystemExit("unknown mode " + mode)
    finally:
        await ifl.stop()


asyncio.run(main())
