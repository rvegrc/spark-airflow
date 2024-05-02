# FROM spark:3.5.1-scala2.12-java17-python3-ubuntu

FROM apache/spark:3.4.2-scala2.12-java11-python3-ubuntu 


# FROM apache/spark:3.5.1-scala2.12-java11-python3-ubuntu 
# doesn't work

# FROM apache/spark:3.3.3-scala2.12-java11-python3-ubuntu

USER root
# FROM apache/sedona:latest
COPY requirements.txt .
COPY .env .
RUN pip install --no-cache-dir -r requirements.txt
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=19888", "--no-browser", "--allow-root", "--NotebookApp.token='1234'", "--NotebookApp.password='1234'"]