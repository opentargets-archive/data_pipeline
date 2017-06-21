FROM quay.io/opentargets/mrtarget_base
# which is just python2.7 plus the spacy model

WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app
# install fresh these requirements.
RUN pip install --no-cache-dir -r requirements.txt

COPY . /usr/src/app

CMD [ "python", "-m", "mrtarget.CommandLine" ]
