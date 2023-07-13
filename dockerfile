FROM public.ecr.aws/lambda/python:3.9

WORKDIR /src

COPY src/ .

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "app.py"]