ARG TRIPLYDB_GITLAB_TOKEN


FROM default-route-openshift-image-registry.meemoo2-2bc857e5f10eb63ab790a3a1d19a696c-i000.eu-de.containers.appdomain.cloud/prefect/prefect-triplyetl:v4.1.13-v2
COPY requirements.txt .
RUN pip3 install -r requirements.txt --extra-index-url http://do-prd-mvn-01.do.viaa.be:8081/repository/pypi-all/simple --trusted-host do-prd-mvn-01.do.viaa.be
ADD flows /opt/prefect/flows
ADD script /opt/prefect/script
RUN npm run build

ENV NODE_ENV="production"
