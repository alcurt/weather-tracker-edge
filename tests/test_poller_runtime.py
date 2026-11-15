import asyncio

from poller import AsyncPoller


def test_client_ignores_broken_proxy_environment(monkeypatch):
    # 真实踩过：NO_PROXY 里出现 [::1] 时，httpx 0.28 构造 client 会抛 InvalidURL
    monkeypatch.setenv("NO_PROXY", "localhost,[::1]")
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:7897")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:7897")

    poller = AsyncPoller()
    assert poller.client is not None
    asyncio.run(poller.client.aclose())


def test_shutdown_cancels_source_tasks():
    async def scenario():
        poller = AsyncPoller()

        async def sleeper():
            await asyncio.sleep(3600)

        task = poller._add_task(sleeper())
        await asyncio.sleep(0)  # 让任务真正进入 sleep
        await poller.shutdown()
        return task

    task = asyncio.run(scenario())
    assert task.cancelled()
