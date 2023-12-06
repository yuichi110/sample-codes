# python-web-app
## Python Flask Web App 
### How to run locally

0. install poetry, python project management tool
1. cd to project root
2. issue `poetry install` for setup
3. `poetry run uvicorn --host 0.0.0.0 --port 8000 userapp.__main__:app --reload`

### How to test
#### unit test
- `poetry run pytest`

#### external api test
- `poetry python tests/external/test_api.py`

you can configure PROTOCOL(http/https), HOST, PORT via environment variable.
please see the test code for details.

### API DOC

Swagger is provided at the server on `/redoc`

Please check `controllers/user.py` and `models/user.py` for detail url and request/response body.

#### get users (debug)
- method: GET
- URI: /api/users
- REQUEST BODY:
- RESPONSE BODY: [{id:\<user-uuid\>, username:\<user-name\>, email:\<email\>},...]

#### get user
- method: GET
- URI: /api/users/\<signin-username\>
- REQUEST BODY:
- RESPONSE BODY: {id:\<user-uuid\>, username:\<user-name\>, email:\<email\>}

Requires signin first and create session.
Requesting other username returns error.

#### signin
- method: POST
- URI: /api/signin
- REQUEST BODY: {username_or_email:\<username-or-email\>, password:\<password\>}
- RESPONSE BODY: {}

#### signout
- method: DELETE
- URI: /api/signin
- REQUEST BODY:
- RESPONSE BODY: {}

without signin state, error doesn't happen.

#### signup
- method: POST
- URI: /api/users
- REQUEST BODY: {username:\<username\>, email:\<email\>, password1:\<password\>, password2:\<password\>}
- RESPONSE BODY: {}

Choosing existing username or email will fail.
password1 and password2 need to be matched.

### Architecture

`__main__.py`: 
Entry point. flask app controller. Serialize/Deserialize JSON and Data Object. Handles error.
Other modules don't depend on flask.
So, you can switch to another framework easily such as FastAPI.

`parameter.py`: load parameters from environment variable and CLI option

`di.py`: inject dependencies(repositories) to service by parameters.

`models/`: Data model.

`services/`: bussiness logic

`repositories/`:
Data layer.
Called by service with "Dependency Inversion" rule.
Currently, using mock repositories for user and session.
However able to add another repositories such as DB/Cache without chainging buiseness logic of service etc.

`tests/`: holds unittest codes.

## Tanzu Application Platform

### Sample create app command

```
$ tanzu apps workload create tz-userapp \
-a tz-userapp \
--annotation autoscaling.knative.dev/minScale=1 \
--annotation autoscaling.knative.dev/maxScale=1 \
--git-repo https://github.com/yuichi110/tz_userapp \
--git-branch main \
--type web \
--yes \
--namespace demo
```

### Caution

If you want to scale out, please use DB/Cache for repository implementation.