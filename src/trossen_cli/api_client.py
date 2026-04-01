"""HTTP client wrapper with authentication injection."""

import asyncio
import os
from typing import Any

import httpx

from .auth import get_token

API_BASE_URL = os.environ.get("TROSSEN_API_URL", "https://cloud.trossen.com/api/v1")


class ApiError(Exception):
    """
    API error with status code and message.
    """

    def __init__(self, status_code: int, message: str, details: dict | None = None):
        self.status_code = status_code
        self.message = message
        self.details = details or {}
        super().__init__(f"API Error {status_code}: {message}")


class ApiClient:
    """
    HTTP client with authentication and retry logic.
    """

    def __init__(self, base_url: str | None = None):
        """
        Initialize API client.
        """
        self.base_url = base_url or API_BASE_URL
        self._access_token: str | None = None
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "ApiClient":
        """
        Enter async context.
        """
        self._access_token = get_token()
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(30.0, connect=10.0),
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Exit async context.
        """
        if self._client:
            await self._client.aclose()

    def _get_headers(self) -> dict[str, str]:
        """
        Get request headers with auth token.
        """
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if self._access_token:
            headers["X-API-Token"] = self._access_token
        return headers

    async def _handle_response(self, response: httpx.Response) -> Any:
        """
        Handle API response and raise errors if needed.
        """
        if response.status_code == 401:
            raise ApiError(401, "Authentication failed. Check your token or run 'trc auth login'")

        if response.status_code == 403:
            detail = "Access denied"
            try:
                error_data = response.json()
                if msg := error_data.get("message", error_data.get("detail")):
                    detail = msg
            except ValueError:
                pass
            raise ApiError(403, detail)

        if response.status_code == 404:
            raise ApiError(404, "Resource not found")

        if response.status_code >= 400:
            try:
                error_data = response.json()
                message = error_data.get("message", error_data.get("detail", "Unknown error"))
                raise ApiError(response.status_code, message, error_data)
            except ValueError:
                raise ApiError(response.status_code, response.text)

        if response.status_code == 204:
            return {}

        try:
            return response.json()
        except ValueError:
            return {"data": response.text}

    async def _request_with_retry(
        self,
        method: str,
        path: str,
        max_retries: int = 5,
        **kwargs,
    ) -> Any:
        """
        Make request with exponential backoff retry.
        """
        if not self._client:
            raise RuntimeError("Client not initialized. Use async context manager.")

        last_error: Exception | None = None

        for attempt in range(max_retries):
            try:
                response = await self._client.request(
                    method,
                    path,
                    headers=self._get_headers(),
                    **kwargs,
                )
                return await self._handle_response(response)

            except httpx.TimeoutException:
                last_error = ApiError(408, "Request timeout")
            except httpx.ConnectError:
                last_error = ApiError(503, "Could not connect to server")
            except ApiError as e:
                if e.status_code >= 500 or e.status_code == 429:
                    last_error = e
                else:
                    raise

            # Exponential backoff
            if attempt < max_retries - 1:
                wait_time = (2**attempt) * 0.5
                await asyncio.sleep(wait_time)

        if last_error:
            raise last_error
        raise ApiError(500, "Unknown error occurred")

    async def get(self, path: str, params: dict | None = None) -> Any:
        """
        Make GET request.
        """
        return await self._request_with_retry("GET", path, params=params)

    async def post(self, path: str, json: dict | None = None) -> Any:
        """
        Make POST request.
        """
        return await self._request_with_retry("POST", path, json=json)

    async def put(self, path: str, json: dict | None = None) -> Any:
        """
        Make PUT request.
        """
        return await self._request_with_retry("PUT", path, json=json)

    async def delete(self, path: str) -> Any:
        """
        Make DELETE request.
        """
        return await self._request_with_retry("DELETE", path)

    async def patch(self, path: str, json: dict | None = None) -> Any:
        """
        Make PATCH request.
        """
        return await self._request_with_retry("PATCH", path, json=json)


class SyncApiClient:
    """
    Synchronous wrapper around the async API client.
    """

    def __init__(self, base_url: str | None = None):
        """
        Initialize sync API client.
        """
        self.base_url = base_url
        self._async_client: ApiClient | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    def __enter__(self) -> "SyncApiClient":
        """
        Enter sync context.
        """
        self._loop = asyncio.new_event_loop()
        self._async_client = ApiClient(self.base_url)
        try:
            self._loop.run_until_complete(self._async_client.__aenter__())
        except BaseException:
            self._loop.close()
            self._loop = None
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit sync context.
        """
        if self._async_client and self._loop:
            self._loop.run_until_complete(self._async_client.__aexit__(exc_type, exc_val, exc_tb))
        if self._loop:
            self._loop.close()

    def _run(self, coro):
        """
        Run coroutine synchronously.
        """
        if not self._loop:
            raise RuntimeError("Client not initialized. Use context manager.")
        return self._loop.run_until_complete(coro)

    def get(self, path: str, params: dict | None = None) -> Any:
        """
        Make GET request.
        """
        if not self._async_client:
            raise RuntimeError("Client not initialized. Use context manager.")
        return self._run(self._async_client.get(path, params))

    def post(self, path: str, json: dict | None = None) -> Any:
        """
        Make POST request.
        """
        if not self._async_client:
            raise RuntimeError("Client not initialized. Use context manager.")
        return self._run(self._async_client.post(path, json))

    def put(self, path: str, json: dict | None = None) -> Any:
        """
        Make PUT request.
        """
        if not self._async_client:
            raise RuntimeError("Client not initialized. Use context manager.")
        return self._run(self._async_client.put(path, json))

    def delete(self, path: str) -> Any:
        """
        Make DELETE request.
        """
        if not self._async_client:
            raise RuntimeError("Client not initialized. Use context manager.")
        return self._run(self._async_client.delete(path))

    def patch(self, path: str, json: dict | None = None) -> Any:
        """
        Make PATCH request.
        """
        if not self._async_client:
            raise RuntimeError("Client not initialized. Use context manager.")
        return self._run(self._async_client.patch(path, json))


def get_api_client() -> ApiClient:
    """
    Get a new API client instance.
    """
    return ApiClient()
