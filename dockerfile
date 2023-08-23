FROM public.ecr.aws/lambda/python:3.9

RUN yum install -y openssh-clients

WORKDIR /src

COPY src/ .

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python3"]

CMD ["hello_world.py"]