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
# Control Plane & Full-Stack AI Gateway Dockerfile (Spring Boot 4.2 + Spring Modulith + jOOQ + Flyway on Oracle GraalVM 25)
# Build command:
#   docker build -f Dockerfile -t ghcr.io/jihuayu/ai-gateway:latest .

FROM container-registry.oracle.com/graalvm/jdk:25 AS builder
WORKDIR /workspace

COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle ./gradle
COPY gateway-common ./gateway-common
COPY gateway-data-plane ./gateway-data-plane
COPY gateway-control-plane ./gateway-control-plane

RUN chmod +x ./gradlew && ./gradlew :gateway-control-plane:bootJar -x test --no-daemon

FROM container-registry.oracle.com/graalvm/jdk:25
WORKDIR /app

COPY --from=builder /workspace/gateway-control-plane/build/libs/gateway-control-plane-0.2.0.jar /app/ai-gateway.jar

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

ENTRYPOINT ["java", "--enable-preview", "-jar", "/app/ai-gateway.jar"]
