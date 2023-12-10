from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn

from authapp.parameter import Parameter
from authapp.di import DiContainer
from authapp.controllers.auth import AuthRouter
from authapp.exceptions import ClientException, ServerException


def build(app: FastAPI):
    # build service
    params = Parameter()
    params.load()
    dic = DiContainer(params)
    service = dic.get_service()

    # build controller
    user_router = AuthRouter(service)
    app.include_router(user_router)
    app.add_exception_handler(ClientException, handle_client_error)
    app.add_exception_handler(ServerException, handle_expected_server_error)
    app.add_exception_handler(Exception, handle_unexpected_server_error)


def handle_client_error(request, error: ClientException):
    return JSONResponse(
        status_code=400,
        content={"error": str(error)},
    )


def handle_expected_server_error(request, error: ServerException):
    return JSONResponse(
        status_code=500,
        content={"error": str(error)},
    )


def handle_unexpected_server_error(request, error: Exception):
    return JSONResponse(
        status_code=500,
        content={"error": str(error)},
    )


app = FastAPI()
build(app)


if __name__ == "__main__":
    uvicorn.run(app)
