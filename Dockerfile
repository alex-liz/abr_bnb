# syntax=docker/dockerfile:1
FROM python:3.9-bullseye
ADD . /code
WORKDIR /code
ENV API_KEY=dfAdIzFOjxIGwVTOkRvjNgsWj1TbybvvARZZ5WLsRRU1TiSkDEtkWtie9q3GyYpd
ENV API_SECRET=UxV29zeeQw2vse2ORVoEg366SWomtCxRHKWtxq4JoqdrB4xboxmAO4Fwq1LQnMPZ
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "ico_semiauto.py"]
