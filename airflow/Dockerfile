FROM apache/airflow:3.0.0
USER root
RUN apt-get update && \    
    apt-get install -y wget unzip && \    
    wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \    
    apt install -y ./google-chrome-stable_current_amd64.deb && \    
    rm -f ./google-chrome-stable_current_amd64.deb && \    
    export LATEST_VERSION=$(curl -s https://chromedriver.storage.googleapis.com/LATEST_RELEASE) && \    
    wget -O /tmp/chromedriver.zip "https://chromedriver.storage.googleapis.com/$LATEST_VERSION/chromedriver_linux64.zip" && \    
    unzip /tmp/chromedriver.zip -d /usr/local/bin/ && \    
    rm /tmp/chromedriver.zip && \    
    rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip3 install pandas numpy selenium webdriver-manager beautifulsoup4 psycopg2-binary requests
