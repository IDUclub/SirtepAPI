import aiohttp

from app.common.exceptions.http_exception_wrapper import http_exception


class JSONAPIHandler:

    def __init__(
        self,
        base_url: str,
    ) -> None:
        """Initialisation function

        Args:
            base_url (str): Base api url
        Returns:
            None
        """

        self.__name__ = "UrbanAPIGateway"
        self.base_url = base_url.rstrip("/")

    @staticmethod
    async def _check_response_status(
        response: aiohttp.ClientResponse,
    ) -> list | dict | None:
        """Function handles response

        Args:
            response (aiohttp.ClientResponse): Response object
        Returns:
            list|dict: requested data with additional info, e.g. {"retry": True | False, "response": {response.json}}
        Raises:
            http_exception with response status code from API
        """

        if response.status in (200, 201):
            return await response.json(content_type="application/json")
        elif response.status == 500:
            if response.content_type == "application/json":
                response_info = await response.json()
                if "reset by peer" in await response_info["error"]:
                    return None
            else:
                response_info = await response.text()
            raise http_exception(
                response.status,
                "Couldn't get data from API",
                _input=str(response.url),
                _detail=response_info,
            )
        else:
            raise http_exception(
                response.status,
                "Couldn't get data from API",
                _input=str(response.url),
                _detail=await response.json(),
            )

    @staticmethod
    async def _check_request_params(
        params: dict[str, str | int | float | bool] | None,
    ) -> dict | None:
        """
        Function checks request parameters
        Args:
            params (dict[str, str | int | float | bool]  | None): Request parameters
        Returns:
            dict | None: Returns modified parameters if they are not empty, otherwise returns None
        """

        if params:
            for key, param in params.items():
                if isinstance(param, bool):
                    params[key] = str(param).lower()
        return params

    async def get(
        self,
        endpoint: str,
        headers: dict | None = None,
        params: dict | None = None,
        session: aiohttp.ClientSession | None = None,
    ) -> dict | list:
        """Function to get data from api
        Args:
            endpoint (str): Endpoint url
            headers (dict | None): Headers
            params (dict | None): Query parameters
            session (aiohttp.ClientSession | None): Session to use
        Returns:
            dict | list: Response data as python object
        """

        if not session:
            async with aiohttp.ClientSession() as session:
                return await self.get(
                    endpoint=endpoint,
                    headers=headers,
                    params=params,
                    session=session,
                )
        url = f'{self.base_url}/{endpoint.lstrip("/")}'
        params = await self._check_request_params(params)
        async with session.get(url=url, headers=headers, params=params) as response:
            result = await self._check_response_status(response)
            if isinstance(result, (list, dict)):
                return result
            if not result:
                if isinstance(result, list):
                    return result
                elif isinstance(result, dict):
                    return result
                return await self.get(
                    endpoint=endpoint,
                    headers=headers,
                    params=params,
                    session=session,
                )
            return result

    async def post(
        self,
        endpoint: str,
        headers: dict | None = None,
        params: dict | None = None,
        data: dict | None = None,
        session: aiohttp.ClientSession | None = None,
    ) -> dict | list:
        """Function to post data from api
        Args:
            endpoint (str): Endpoint url
            headers (dict | None): Headers
            params (dict | None): Query parameters
            data (dict | None): Request data
            session (aiohttp.ClientSession | None): Session to use
        Returns:
            dict | list: Response data as python object
        """

        if not session:
            async with aiohttp.ClientSession() as session:
                return await self.post(
                    endpoint=endpoint,
                    headers=headers,
                    params=params,
                    data=data,
                    session=session,
                )
        url = f'{self.base_url}/{endpoint.lstrip("/")}'
        params = await self._check_request_params(params)
        async with session.post(
            url=url,
            headers=headers,
            params=params,
            data=data,
        ) as response:
            result = await self._check_response_status(response)
            if not result:
                return await self.post(
                    endpoint=endpoint,
                    headers=headers,
                    params=params,
                    session=session,
                )
            return result

    async def put(
        self,
        endpoint: str,
        headers: dict | None = None,
        params: dict | None = None,
        data: dict | None = None,
        session: aiohttp.ClientSession | None = None,
    ) -> dict | list:
        """Function to post data from api
        Args:
            endpoint (str): Endpoint url
            headers (dict | None): Headers
            params (dict | None): Query parameters
            data (dict | None): Request data
            session (aiohttp.ClientSession | None): Session to use
        Returns:
            dict | list: Response data as python object
        """

        if not session:
            async with aiohttp.ClientSession() as session:
                return await self.put(
                    endpoint=endpoint,
                    headers=headers,
                    params=params,
                    data=data,
                    session=session,
                )
        url = f'{self.base_url}/{endpoint.lstrip("/")}'
        params = await self._check_request_params(params)
        async with session.put(
            url=url,
            headers=headers,
            params=params,
            data=data,
        ) as response:
            result = await self._check_response_status(response)
            if not result:
                return await self.put(
                    endpoint=endpoint,
                    headers=headers,
                    params=params,
                    session=session,
                )
            return result

    async def delete(
        self,
        endpoint: str,
        headers: dict | None = None,
        params: dict | None = None,
        data: dict | None = None,
        session: aiohttp.ClientSession | None = None,
    ) -> dict | list:
        """Function to post data from api
        Args:
            endpoint (str): Endpoint url
            headers (dict | None): Headers
            params (dict | None): Query parameters
            data (dict | None): Request data
            session (aiohttp.ClientSession | None): Session to use
        Returns:
            dict | list: Response data as python object
        """

        if not session:
            async with aiohttp.ClientSession() as session:
                return await self.delete(
                    endpoint=endpoint,
                    headers=headers,
                    params=params,
                    data=data,
                    session=session,
                )
        url = f'{self.base_url}/{endpoint.lstrip("/")}'
        params = await self._check_request_params(params)
        async with session.delete(
            url=url,
            headers=headers,
            params=params,
            data=data,
        ) as response:
            result = await self._check_response_status(response)
            if not result:
                return await self.delete(
                    endpoint=endpoint,
                    headers=headers,
                    params=params,
                    session=session,
                )
            return result
