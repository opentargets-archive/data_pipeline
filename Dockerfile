FROM python:2.7-alpine

WORKDIR /usr/src/app

COPY requirements.txt ./
# install fresh these requirements.
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "-m", "mrtarget.CommandLine" ]

