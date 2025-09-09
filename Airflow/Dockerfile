# FROM apache/airflow:2.10.2

# USER root

# # Install system dependencies and libraries
# RUN apt-get update -o Acquire::Check-Valid-Until=false -o Acquire::Check-Date=false && \
#     apt-get install -y \
#     # Chrome and Selenium dependencies
#     # libglib2.0-0=2.50.3-2 \
#     # libnss3=2:3.26.2-1.1+deb9u1 \
#     # libgconf-2-4=3.2.6-4+b1 \
#     # libfontconfig1=2.11.0-6.7+b1 \
#     chromium \
#     chromium-driver \
#     # chromium-webdriver \
#     # chromium-browser \
#     xvfb \
#     xorg \
#     dbus-x11 \
#     xfonts-100dpi \
#     xfonts-75dpi \
#     # xfonts-cyrillic \
#     # Additional system libraries for Chrome
#     libgl1-mesa-glx \
#     libglib2.0-0 \
#     libnss3 \
#     libgconf-2-4 \
#     libfontconfig1 \
#     libxss1 \
#     libasound2 \
#     libxtst6 \
#     libgtk-3-0 \
#     libgbm1 \
#     # PDF and image processing dependencies
#     poppler-utils \
#     tesseract-ocr \
#     libtesseract-dev \
#     libleptonica-dev \
#     # Other utilities
#     wget \
#     unzip \
#     curl \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*


# # Create necessary directories and set permissions
# # RUN mkdir -p /opt/chrome /opt/chromedriver /opt/selenium /var/run/xvfb && \
# #     chmod -R 777 /opt/chrome /opt/chromedriver /opt/selenium /var/run/xvfb

# # Set up virtual display
# # ENV DISPLAY=:99
# # RUN mkdir -p /tmp/.X11-unix && \
# #     chmod 1777 /tmp/.X11-unix

# # Verify Chrome installation and set up symlinks
# # RUN ln -s /usr/bin/chromium /usr/bin/google-chrome && \
# #     ln -s /usr/bin/chromium-driver /usr/bin/chromedriver && \
# #     chmod 755 /usr/bin/chromium /usr/bin/chromium-driver

# USER airflow

# # Install Python packages with specific versions
# RUN pip install --no-cache-dir \
#     # Core dependencies
#     pandas \
#     boto3 \
#     requests \
#     python-dateutil \
#     beautifulsoup4 \
#     # Web scraping and automation
#     selenium==4.16.0 \
#     webdriver-manager==4.0.1 \
#     # xvfbwrapper==0.2.9 \
#     # Additional utilities
#     python-dotenv \
#     tqdm \
#     # PDF processing
#     PyPDF2 \
#     pdf2image \
#     pytesseract \
#     # First install langchain dependencies
#     pydantic<3.0.0,>=1.7.4 \
#     dataclasses-json<0.6.0 \
#     PyYAML>=5.4.1 \
#     Pillow \
#     PyMuPDF \
#     SQLAlchemy<3,>=1.4 \
#     # Then install langchain and related packages
#     langchain-core==0.1.23 \
#     langchain==0.1.0 \
#     langchain-community==0.0.20 \
#     openai \
#     pinecone-client \
#     sentence_transformers \
#      # Additional utilities
#      openpyxl \
#      python-dotenv \
#      xlrd \
#      xlwt \
#      lxml

# # Set environment variables
# ENV PYTHONPATH=/opt/airflow/dags \
#     # DISPLAY=:99 \
#     # CHROMEDRIVER_PATH=/usr/bin/chromedriver \
#     # CHROME_BINARY_PATH=/usr/bin/google-chrome \
#     AWS_DEFAULT_REGION=us-east-1 \
#     PYTHONUNBUFFERED=1 \
#     WDM_LOG_LEVEL=0
#     # CHROMEDRIVER_DIR=/opt/chromedriver \
#     # SELENIUM_DIR=/opt/selenium

# # Create and set up Xvfb startup script
# # USER root
# # RUN echo '#!/bin/bash\n\
# # # Start Xvfb\n\
# # Xvfb :99 -screen 0 1920x1080x24 -ac +extension RANDR > /dev/null 2>&1 &\n\
# # \n\
# # # Wait for Xvfb to start\n\
# # sleep 1\n\
# # \n\
# # # Execute passed command\n\
# # exec "$@"' > /start-xvfb.sh && \
# #     chmod +x /start-xvfb.sh

# # Verify installations
# # RUN chromium --version && \
# #     chromedriver --version && \
# #     which Xvfb

# USER airflow
# WORKDIR /opt/airflow

# # Final verification of Python dependencies
# # RUN python -c "from selenium import webdriver; from xvfbwrapper import Xvfb; from selenium.webdriver.chrome.service import Service; print('Selenium and Xvfb successfully installed')"

# # ENTRYPOINT ["/start-xvfb.sh"]


FROM apache/airflow:2.10.2

USER root

# Install system dependencies and libraries
RUN apt-get update -o Acquire::Check-Valid-Until=false -o Acquire::Check-Date=false && \
    apt-get install -y \
    chromium \
    chromium-driver \
    xorg \
    dbus-x11 \
    xfonts-100dpi \
    xfonts-75dpi \
    libgl1-mesa-glx \
    libglib2.0-0 \
    libnss3 \
    libgconf-2-4 \
    libfontconfig1 \
    libxss1 \
    libasound2 \
    libxtst6 \
    libgtk-3-0 \
    libgbm1 \
    poppler-utils \
    tesseract-ocr \
    libtesseract-dev \
    libleptonica-dev \
    wget \
    unzip \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages - fix the formatting issue
RUN pip install --no-cache-dir \
    pandas \
    boto3 \
    requests \
    python-dateutil \
    beautifulsoup4 \
    "selenium==4.16.0" \
    "webdriver-manager==4.0.1" \
    python-dotenv \
    tqdm \
    PyPDF2 \
    pdf2image \
    pytesseract \
    "pydantic>=1.7.4,<3.0.0" \
    "dataclasses-json<0.6.0" \
    "PyYAML>=5.4.1" \
    Pillow \
    PyMuPDF \
    "SQLAlchemy>=1.4,<3" \
    "langchain-core==0.1.23" \
    "langchain==0.1.0" \
    "langchain-community==0.0.20" \
    openai \
    pinecone-client \
    sentence_transformers \
    openpyxl \
    xlrd \
    xlwt \
    lxml

# Set environment variables
ENV PYTHONPATH=/opt/airflow/dags \
    AWS_DEFAULT_REGION=us-east-1 \
    PYTHONUNBUFFERED=1 \
    WDM_LOG_LEVEL=0

USER airflow
WORKDIR /opt/airflow