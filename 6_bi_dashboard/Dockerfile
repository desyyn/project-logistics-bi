FROM python:3.11-slim

WORKDIR /app

# Install system dependencies untuk XGBoost
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python packages DENGAN pre-built wheels
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    streamlit==1.32.0 \
    pandas==2.2.1 \
    numpy==1.26.4 \
    joblib==1.3.2 \
    sqlalchemy==2.0.28 \
    psycopg2-binary==2.9.9 \
    scikit-learn==1.4.1.post1

RUN pip install --no-cache-dir xgboost==2.0.3 --only-binary :all:

COPY app.py .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]