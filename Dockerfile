FROM quay.io/opentargets/mrtarget_base
# which is just python2.7 plus the spacy model

WORKDIR /usr/src/app
COPY . /usr/src/app

# install fresh these requirements.
RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "-m", "mrtarget.CommandLine" ]
