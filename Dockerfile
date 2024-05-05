FROM apache/spark:3.4.2-scala2.12-java11-python3-ubuntu 

USER root
# FROM apache/sedona:latest
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# COPY .env .
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=19888", "--no-browser", "--allow-root", "--NotebookApp.token='1234'", "--NotebookApp.password='1234'"]