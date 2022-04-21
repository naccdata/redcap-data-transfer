# set the base image
FROM python:latest

# set the working directory in the container
WORKDIR /data_validator

# copy the requirements file to the working directory
COPY requirements.txt .

# install dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# copy the content of the local src directory to the working directory
ADD src/ ./src

# create directory for log files
RUN mkdir logs

#install cron
RUN apt-get update && apt-get -y install cron

# Copy and enable the CRON task
COPY ./crontab /etc/cron.d/crontab

RUN chmod 0644 /etc/cron.d/crontab
RUN /usr/bin/crontab /etc/cron.d/crontab

# run crond as main process of container
CMD ["cron", "-f"]
