FROM rocker/r-ver

ENV DEBIAN_FRONTEND="noninteractive" TZ="Etc/UTC"

RUN apt-get update && \
    apt-get install -y \
    build-essential \
    curl \
    libcurl4-openssl-dev \
    r-base \
    r-base-dev \
    libfontconfig1-dev \
    libharfbuzz-dev \
    libfribidi-dev \
    wget \
    openssl \
    libssl-dev \
    zlib1g-dev

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

# Install R packages
# RUN R -e "install.packages('devtools', repos = 'http://cran.us.r-project.org')" && \
#     R -e "install.packages('nanoarrow', repos = 'http://cran.us.r-project.org')" && \
#     R -e "install.packages('extendr', repos = 'http://cran.us.r-project.org')"

ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustc --version
