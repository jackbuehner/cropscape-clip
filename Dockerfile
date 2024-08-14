FROM python:3.10.12-slim
RUN python --version
RUN useradd --create-home --shell /bin/bash app

# Create app directory
WORKDIR /home/app

# Install the application dependencies
COPY requirements.txt ./
RUN apt update
RUN apt install gdal-bin python3-gdal g++ libgdal-dev -y
RUN pip install --no-cache-dir -r requirements.txt

# Switch to the app user
USER app

# Copy in the source code
COPY src /home/app

# Set bash as the defailt command so that is invoked when the container starts
# (will require running the container with the -it flag)
CMD ["bash"]