FROM public.ecr.aws/lambda/python:3.9

RUN yum install -y openssh-clients

# Install MySQL client (including mysqldump)
RUN yum install -y mysql
#RUN yum install -y expect

WORKDIR /src

COPY src/ .

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python3"]

CMD ["hello_world.py"]