# set the base image
ARG VARIANT="3.10-slim-bullseye"
FROM python:${VARIANT}

# set the working directory in the container
WORKDIR /redcap_transfer

# copy the requirements file and install dependencies
COPY requirements.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# install cron and clean up
RUN apt-get update && apt-get -y install --no-install-recommends cron \
    && rm -rf /etc/cron.*/* && rm -rf /var/lib/apt/lists/*

# Copy and load crontab
COPY crontab /etc/cron.d/crontab
RUN chmod +x /etc/cron.d/crontab \
    && /usr/bin/crontab /etc/cron.d/crontab

# Copy entrypoint.sh and set permissions
COPY entrypoint.sh ./
RUN chmod +x /redcap_transfer/entrypoint.sh

# copy the content of the local src directory to the working directory
COPY src/ src/

ENTRYPOINT ["/redcap_transfer/entrypoint.sh"]

# -f | Run cron in foreground mode
CMD ["cron","-f"]

