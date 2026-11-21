FROM python:3.12-slim-bookworm AS compiler
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev make zlib1g-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Makefile .
COPY depthscope/book.c depthscope/book.c
RUN make

FROM python:3.12-slim-bookworm
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY --from=compiler /app/build/libdepthscope.so build/libdepthscope.so
COPY depthscope depthscope
ENTRYPOINT ["python", "-m", "depthscope"]
