import asyncio
import logging
import random
import time
from typing import List, Dict, Any

import aiohttp

USERS = [f"user_{i}" for i in range(10)]

MESSAGES = [f"Message {i}" for i in range(100)]

DEFAULT_SERVERS = ["http://localhost:8000", "http://localhost:8001"]


async def send_message(
    session: aiohttp.ClientSession, server_urls: List[str]
) -> Dict[str, Any]:
    """
    Функция для отправки сообщения на одну из реплик.
    
    :param session: HTTP-сессия aiohttp
    :type session: aiohttp.ClientSession
    :param server_urls: Список URL-адресов серверов
    :type server_urls: List[str]
    :return: Словарь с информацией о результате запроса
    :rtype: Dict[str, Any]
    """
    server_url = random.choice(server_urls)

    user = random.choice(USERS)
    text = random.choice(MESSAGES)

    url = f"{server_url}/message"
    payload = {"sender": user, "text": text}
    start_time = time.time()
    response = await session.post(url, json=payload)
    elapsed_time = time.time() - start_time
    response_json = await response.json()
    if response.status != 200:
        logging.log(
            level=logging.ERROR,
            msg=f"Запрос к {url} вернул {response.status}: {response_json}",
        )
    status = response.status

    return {
        "status": status,
        "elapsed_time": elapsed_time,
        "response": response_json,
        "server": server_url,
    }


async def worker(
    server_urls: List[str], num_requests: int = 100
) -> List[Dict[str, Any]]:
    """
    Функция воркер для отправки нескольких запросов.
    
    :param server_urls: Список URL-адресов серверов
    :type server_urls: List[str]
    :param num_requests: Количество запросов для отправки
    :type num_requests: int
    :return: Список результатов запросов
    :rtype: List[Dict[str, Any]]
    """
    results = []
    async with aiohttp.ClientSession() as session:
        for i in range(num_requests):
            try:
                result = await send_message(session, server_urls)
                results.append(result)
                await asyncio.sleep(random.uniform(0.0005, 0.001))
            except Exception as e:
                results.append({"error": str(e), "elapsed_time": 0})
    return results


async def run_test(
    server_urls: List[str], num_workers: int = 50, requests_per_worker: int = 100
) -> Dict[str, Any]:
    """
    Функция для запуска тестирования.
    
    :param server_urls: Список URL-адресов серверов
    :type server_urls: List[str]
    :param num_workers: Количество воркеров
    :type num_workers: int
    :param requests_per_worker: Количество запросов на воркер
    :type requests_per_worker: int
    :return: Словарь со статистикой тестирования
    :rtype: Dict[str, Any]
    """
    start_time = time.time()

    tasks = [worker(server_urls, requests_per_worker) for _ in range(num_workers)]

    results = await asyncio.gather(*tasks)

    total_time = time.time() - start_time

    all_results = [item for sublist in results for item in sublist]
    successful_results = [i for i in all_results if "error" not in i]
    request_times = [i["elapsed_time"] for i in successful_results]
    stats = {
        "total_time": total_time,
        "total_requests": len(all_results),
        "error_rate": (
            1 - (len(successful_results) / len(all_results)) if all_results else 0
        ),
        "throughput": len(successful_results) / total_time,
        "avg_request_time": sum(request_times) / len(request_times),
    }
    server_counts = {}
    for result in successful_results:
        server = result.get("server", "unknown")
        server_counts[server] = server_counts.get(server, 0) + 1

    return {**stats, "server_distribution": server_counts}


def print_results(results: Dict[str, Any]) -> None:
    """
    Функция для вывода результатов тестирования.
    
    :param results: Словарь со статистикой тестирования
    :type results: Dict[str, Any]
    :return: None
    """
    print("\n========== TEST RESULTS ==========")
    print(f"Total time: {round(results['total_time'], 2)} seconds")
    print(f"Total requests: {results['total_requests']}")
    print(f"Throughput: {round(results['throughput'], 2)} requests/second")
    print(f"Average request time: {round(results['avg_request_time'], 2)} seconds")


async def main(
    server_urls: List[str] = None, num_workers: int = 50, requests_per_worker: int = 100
):
    """
    Входная точка для запуска тестирования.
    
    :param server_urls: Список URL-адресов серверов, по умолчанию DEFAULT_SERVERS
    :type server_urls: List[str]
    :param num_workers: Количество воркеров
    :type num_workers: int
    :param requests_per_worker: Количество запросов на ворке
    :type requests_per_worker: int
    :return: None
    """
    if not server_urls:
        server_urls = DEFAULT_SERVERS

    results = await run_test(server_urls, num_workers, requests_per_worker)
    print_results(results)


if __name__ == "__main__":
    asyncio.run(main(DEFAULT_SERVERS, num_workers=50, requests_per_worker=100))
