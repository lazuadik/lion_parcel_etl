# Gunakan base image Python
FROM python:3.9-slim

# Install library sistem yang tadi kita instal manual (libxcb, dll)
RUN apt-get update && apt-get install -y \
    libgl1 \
    libglib2.0-0 \
    libxcb1 \
    libx11-6 \
    && rm -rf /var/lib/apt/lists/*

# Set direktori kerja di dalam kontainer
WORKDIR /app

# Copy file requirements dan install library
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy seluruh isi folder proyek ke dalam kontainer
COPY . .

# Jalankan API saat kontainer dinyalakan
CMD ["python3", "plugins/blur_api.py"]