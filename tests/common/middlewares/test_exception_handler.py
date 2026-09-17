import json
import unittest
from types import SimpleNamespace

from fastapi import Request

from app.common.middlewares.exception_handler import ExceptionHandlerMiddleware


class ErrorCounter:
    def add(self, *_args, **_kwargs):
        pass


class ExceptionHandlerMiddlewareTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_internal_error_response_does_not_include_request_headers(self):
        token = "Bearer service-account-token"
        cookie = "session=secret"
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": "/explode",
            "raw_path": b"/explode",
            "query_string": b"",
            "headers": [
                (b"authorization", token.encode()),
                (b"cookie", cookie.encode()),
                (b"x-request-id", b"request-id"),
            ],
            "client": ("testclient", 123),
            "server": ("testserver", 80),
        }

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def raise_internal_error(_request):
            raise AttributeError("failure")

        metrics = SimpleNamespace(http=SimpleNamespace(errors=ErrorCounter()))
        middleware = ExceptionHandlerMiddleware(lambda *_args: None, metrics)

        response = await middleware.dispatch(
            Request(scope, receive), raise_internal_error
        )
        payload = json.loads(response.body)
        serialized_response = response.body.decode()

        self.assertEqual(response.status_code, 500)
        self.assertNotIn("headers", payload["request"])
        self.assertNotIn(token, serialized_response)
        self.assertNotIn(cookie, serialized_response)


if __name__ == "__main__":
    unittest.main()
