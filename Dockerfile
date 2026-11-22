# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Full-Stack AI Gateway Dockerfile (Spring Boot 4.2 Control Plane + Direct Netty 4.2 Data Plane on Oracle GraalVM 25)
# Build command:
#   docker build -f Dockerfile -t ghcr.io/jihuayu/ai-gateway:latest .

# Stage 1: Build Executable JAR with Oracle GraalVM 25
FROM container-registry.oracle.com/graalvm/jdk:25 AS builder
WORKDIR /workspace

RUN command -v mvn >/dev/null 2>&1 || ( \
    curl -fsSL https://archive.apache.org/dist/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz \
    | tar -xz -C /opt && ln -s /opt/apache-maven-3.9.9/bin/mvn /usr/local/bin/mvn )

COPY pom.xml ./
COPY src ./src

RUN mvn -B -DskipTests package

# Stage 2: Oracle GraalVM 25 Runtime Image (Virtual Threads + JIT/AOT Ready)
FROM container-registry.oracle.com/graalvm/jdk:25
WORKDIR /app

COPY --from=builder /workspace/target/ai-gateway-0.1.0.jar /app/ai-gateway.jar

ENV HOST=0.0.0.0 \
    ADMIN_PORT=3000 \
    GATEWAY_PORT=3001 \
    DATABASE_URL=postgresql://gateway:local-development-only@postgres:5432/gateway \
    REDIS_URL=redis://redis:6379 \
    DATA_PLANE_NAMESPACE=gateway \
    HOT_DURABILITY=aof \
    HOT_JOURNAL_CAPACITY=10000 \
    ALLOW_PRIVATE_UPSTREAM=false

EXPOSE 3000 3001

ENTRYPOINT ["java", "-jar", "/app/ai-gateway.jar"]
